package main

// main.go — APS telemetry service (monolith baseline).
//
// Contract implemented here:
//   GET  /health
//   POST /telemetria
//   GET  /indice-calor/{sensor_id}
//   GET  /audit/contagem
//   GET  /audit/medidas?limit=&cursor=
//
// Contract fixes applied in this revision:
//
//   1. POST /telemetria accepts a batch of ONE measurement. Only an empty
//      batch is rejected. The load probe sends one measurement per request
//      to isolate the per-request HTTP cost, so enforcing a minimum of 5
//      would make that probe abort on its very first call.
//
//   2. GET /indice-calor returns 404 when the sensor has no measurement.
//      sql.ErrNoRows is a legitimate "not found", not an internal error.
//
//   3. Statements are prepared once at startup, outside the transaction.
//      Preparing per request burned cycles of the 0.05 vCPU quota.
//
//   4. Each row is wrapped in a SAVEPOINT. Without it, a single row error
//      aborts the transaction and every following INSERT fails with
//      "current transaction is aborted", losing the whole batch.
//
//   5. criado_em is written explicitly with now(). It records the instant
//      the server received the measurement and is NOT part of the hash.
//
// NOTE ON FIELD NAMES: the input field is raw_telemetry_packet and the
// output field is raw_packet. This asymmetry is CONTRACT, not an oversight.
// The auditor recomputes the hash over the value returned by the API, so
// renaming either side breaks the audit with 100% divergent records.
//
// Schema assumed (see init.sql):
//   medicoes(id BIGSERIAL PK, sensor_id TEXT, timestamp TIMESTAMPTZ,
//            raw_packet TEXT, hash TEXT, criado_em TIMESTAMPTZ,
//            UNIQUE(sensor_id, timestamp))
//   indices_calor(sensor_id TEXT PK, indice DOUBLE PRECISION,
//                 atualizado_em TIMESTAMPTZ)

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

// ---------- Config ----------

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

var (
	db          *sql.DB
	databaseURL = envString("DATABASE_URL", "postgres://iot:iot@postgres:5432/telemetria?sslmode=disable")
	listenAddr  = envString("LISTEN_ADDR", ":8080")

	stmtInsertMeasurement *sql.Stmt
	stmtUpsertIndex       *sql.Stmt
)

// ---------- Timestamp canônico ----------

// canonicalTimestamp renders an instant as UTC with exactly six decimal
// places. This form is part of the contract: the timestamp is concatenated
// into the measurement hash, so any other representation (three decimals,
// local offset, missing suffix) produces a different hash for the same
// measurement and breaks the audit cross-check.
func canonicalTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000") + "Z"
}

// parseCanonicalTimestamp accepts the canonical form and tolerates the
// variants a client may send (RFC3339 with fewer decimals, offsets).
func parseCanonicalTimestamp(s string) (time.Time, error) {
	layouts := []string{
		"2006-01-02T15:04:05.000000Z",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("timestamp fora do formato canonico: %q", s)
}

// computeMeasurementHash is the identity anchor of a measurement. It MUST
// stay identical to computeHash in avaliar_justo.go.
func computeMeasurementHash(sensorID, canonicalTS, rawPacket string) string {
	sum := sha256.Sum256([]byte(sensorID + canonicalTS + rawPacket))
	return hex.EncodeToString(sum[:])
}

// humidex is the microclimate index stored per sensor. The auditor never
// recomputes it, so the formula is not part of the audit contract — but it
// must be deterministic for a given pair of inputs.
func humidex(tempC, humidityPct float64) float64 {
	e := 6.112 * math.Exp((17.67*tempC)/(tempC+243.5)) * (humidityPct / 100.0)
	h := tempC + 0.5555*(e-10.0)
	return math.Round(h*100) / 100
}

// ---------- Contratos de entrada e saída ----------
type measurementInput struct {
	SensorID  string `json:"sensor_id"`
	Timestamp string `json:"timestamp"`
	Metrics   struct {
		Microclimate struct {
			TemperatureCelsius float64 `json:"temperature_celsius"`
			RelativeHumidity   float64 `json:"relative_humidity_percentage"`
		} `json:"microclimate"`
	} `json:"metrics"`
	RawPacket string `json:"raw_telemetry_packet"`
}

type batchInput struct {
	Measurements []measurementInput `json:"measurements"`
}

type ingestResponse struct {
	TotalReceived  int `json:"total_recebido"`
	TotalPersisted int `json:"total_persistido"`
	TotalRejected  int `json:"total_rejeitado"`
	TotalDuplicate int `json:"total_duplicado"`
}

type countResponse struct {
	TotalMeasurements int64 `json:"total_medicoes"`
	TotalIndices      int64 `json:"total_indices"`
}

type measurementOutput struct {
	ID        int64  `json:"id"`
	SensorID  string `json:"sensor_id"`
	Timestamp string `json:"timestamp"`
	RawPacket string `json:"raw_packet"`
	Hash      string `json:"hash"`
}

type measurementsResponse struct {
	Measurements []measurementOutput `json:"medidas"`
	NextCursor   string              `json:"proximo_cursor"`
}

// ---------- Utilitário de resposta ----------
func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("aviso: falha ao serializar resposta: %v", err)
	}
}

// ---------- Handlers ----------
func handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "banco indisponivel"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "pronto"})
}

func handleTelemetry(w http.ResponseWriter, r *http.Request) {
	var batch batchInput
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"erro": "corpo JSON invalido"})
		return
	}

	// FIX 1 — A batch of ONE measurement is valid. Only an empty batch is
	// rejected. The load probe sends one measurement per request to isolate
	// the per-request HTTP cost, so enforcing a minimum of 5 here would make
	// that probe abort on its very first call.
	if len(batch.Measurements) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"erro": "lote vazio"})
		return
	}

	ctx := r.Context()
	received := len(batch.Measurements)
	persisted := 0
	rejected := 0
	duplicates := 0

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"erro": "falha ao abrir transacao"})
		return
	}
	defer tx.Rollback()

	insertStmt := tx.StmtContext(ctx, stmtInsertMeasurement)
	defer insertStmt.Close()
	indexStmt := tx.StmtContext(ctx, stmtUpsertIndex)
	defer indexStmt.Close()

	for i, m := range batch.Measurements {
		ts, err := parseCanonicalTimestamp(m.Timestamp)
		if err != nil || m.SensorID == "" || m.RawPacket == "" {
			rejected++
			continue
		}

		canonical := canonicalTimestamp(ts)
		hash := computeMeasurementHash(m.SensorID, canonical, m.RawPacket)

		// FIX 4 — SAVEPOINT per row. A row error rolls back only that row;
		// the transaction survives and the rest of the batch is stored.
		savepoint := fmt.Sprintf("sp_%d", i)
		if _, err := tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
			rejected++
			continue
		}

		res, err := insertStmt.ExecContext(ctx, m.SensorID, ts, m.RawPacket, hash)
		if err != nil {
			tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint)
			rejected++
			continue
		}
		tx.ExecContext(ctx, "RELEASE SAVEPOINT "+savepoint)

		// A duplicate (sensor_id, timestamp) is already stored: the row
		// exists, so it counts as persisted, not as a loss.
		persisted++

		affected, _ := res.RowsAffected()
		if affected == 0 {
			duplicates++
			continue
		}

		index := humidex(m.Metrics.Microclimate.TemperatureCelsius,
			m.Metrics.Microclimate.RelativeHumidity)
		if _, err := indexStmt.ExecContext(ctx, m.SensorID, index); err != nil {
			// The measurement is stored; the index will be refreshed by the
			// next measurement of the same sensor.
			log.Printf("aviso: falha ao atualizar indice do sensor %s: %v", m.SensorID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"erro": "falha ao confirmar transacao"})
		return
	}

	writeJSON(w, http.StatusOK, ingestResponse{
		TotalReceived:  received,
		TotalPersisted: persisted,
		TotalRejected:  rejected,
		TotalDuplicate: duplicates,
	})
}

func handleHeatIndex(w http.ResponseWriter, r *http.Request) {
	sensorID := r.PathValue("sensor_id")
	if sensorID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"erro": "sensor_id ausente"})
		return
	}

	var index float64
	err := db.QueryRowContext(r.Context(),
		`SELECT indice FROM indices_calor WHERE sensor_id = $1`, sensorID).Scan(&index)

	// FIX 2 — A sensor with no measurement is a legitimate "not found", not
	// an internal error. Returning 500 here breaks the contract and makes
	// the read probe abort on its first call.
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]string{"erro": "sensor sem medicao"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"erro": "falha ao consultar indice"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sensor_id": sensorID,
		"indice":    index,
	})
}

func handleAuditCount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var totalMeasurements, totalIndices int64

	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM medicoes`).Scan(&totalMeasurements); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"erro": "falha ao contar medicoes"})
		return
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM indices_calor`).Scan(&totalIndices); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"erro": "falha ao contar indices"})
		return
	}

	writeJSON(w, http.StatusOK, countResponse{
		TotalMeasurements: totalMeasurements,
		TotalIndices:      totalIndices,
	})
}

func handleAuditMeasurements(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	limit := 1000
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 10000 {
			limit = n
		}
	}

	// The cursor is opaque to the caller: it is the id of the last row of
	// the previous page. Cursor pagination stays consistent under
	// concurrent inserts, unlike OFFSET.
	var afterID int64
	if c := r.URL.Query().Get("cursor"); c != "" {
		n, err := strconv.ParseInt(c, 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"erro": "cursor invalido"})
			return
		}
		afterID = n
	}

	rows, err := db.QueryContext(ctx, `
		SELECT id, sensor_id, timestamp, raw_packet, hash
		FROM medicoes
		WHERE id > $1
		ORDER BY id
		LIMIT $2`, afterID, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"erro": "falha ao listar medicoes"})
		return
	}
	defer rows.Close()

	out := make([]measurementOutput, 0, limit)
	for rows.Next() {
		var (
			id       int64
			sensorID string
			ts       time.Time
			raw      string
			hash     string
		)
		if err := rows.Scan(&id, &sensorID, &ts, &raw, &hash); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"erro": "falha ao ler medicao"})
			return
		}
		out = append(out, measurementOutput{
			ID:        id,
			SensorID:  sensorID,
			Timestamp: canonicalTimestamp(ts),
			RawPacket: raw,
			Hash:      hash,
		})
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"erro": "falha ao percorrer medicoes"})
		return
	}

	next := ""
	if len(out) == limit {
		next = strconv.FormatInt(out[len(out)-1].ID, 10)
	}

	writeJSON(w, http.StatusOK, measurementsResponse{
		Measurements: out,
		NextCursor:   next,
	})
}

// ---------- Bootstrap ----------
func main() {
	var err error
	db, err = sql.Open("postgres", databaseURL)
	if err != nil {
		log.Fatalf("falha ao abrir conexao: %v", err)
	}
	defer db.Close()

	// With a 0.05 vCPU quota, a large pool only adds contention. Keep it
	// small and let the queue form at the HTTP layer.
	db.SetMaxOpenConns(envInt("DB_MAX_CONNS", 8))
	db.SetMaxIdleConns(envInt("DB_MAX_IDLE", 4))
	db.SetConnMaxLifetime(5 * time.Minute)

	// FIX 3 — statements prepared once at startup, outside the transaction.
	stmtInsertMeasurement, err = db.Prepare(`
		INSERT INTO medicoes (sensor_id, timestamp, raw_packet, hash, criado_em)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (sensor_id, timestamp) DO NOTHING`)
	if err != nil {
		log.Fatalf("falha ao preparar insercao: %v", err)
	}
	defer stmtInsertMeasurement.Close()

	stmtUpsertIndex, err = db.Prepare(`
		INSERT INTO indices_calor (sensor_id, indice, atualizado_em)
		VALUES ($1, $2, now())
		ON CONFLICT (sensor_id) DO UPDATE
			SET indice = EXCLUDED.indice, atualizado_em = now()`)
	if err != nil {
		log.Fatalf("falha ao preparar indice: %v", err)
	}
	defer stmtUpsertIndex.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /telemetria", handleTelemetry)
	mux.HandleFunc("GET /indice-calor/{sensor_id}", handleHeatIndex)
	mux.HandleFunc("GET /audit/contagem", handleAuditCount)
	mux.HandleFunc("GET /audit/medidas", handleAuditMeasurements)

	server := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("servico de telemetria ouvindo em %s", listenAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("falha no servidor: %v", err)
	}
}