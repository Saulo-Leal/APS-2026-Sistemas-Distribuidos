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
// Write path: a whole batch is stored with ONE SQL statement (bulkInsertSQL).
// When REDIS_URL is set (distributed scenario) POST /telemetria only validates
// and enqueues the request body in Redis (202 Accepted); queue workers in each
// replica persist it. /audit/* waits for the queue to drain before answering.
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
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
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

	// REDIS_URL vazio = modo monolito (escrita direta no banco).
	// REDIS_URL definido = escrita assincrona via fila Redis.
	redisAddr = envString("REDIS_URL", "")
	rdb       *redis.Client
)

const (
	queueKey      = "aps:fila"
	processingKey = "aps:processando"
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

// ---------- Preparo e insercao em lote ----------

type preparedRow struct {
	sensorID string
	ts       string // timestamp canonico (o mesmo usado no hash)
	raw      string
	hash     string
	index    float64
}

// validMeasurement aplica as regras de rejeicao sem custo de hash.
func validMeasurement(m measurementInput) bool {
	if m.SensorID == "" || m.RawPacket == "" {
		return false
	}
	_, err := parseCanonicalTimestamp(m.Timestamp)
	return err == nil
}

func prepareRows(ms []measurementInput) ([]preparedRow, int) {
	rows := make([]preparedRow, 0, len(ms))
	rejected := 0
	for _, m := range ms {
		if !validMeasurement(m) {
			rejected++
			continue
		}
		ts, _ := parseCanonicalTimestamp(m.Timestamp)
		canonical := canonicalTimestamp(ts)
		rows = append(rows, preparedRow{
			sensorID: m.SensorID,
			ts:       canonical,
			raw:      m.RawPacket,
			hash:     computeMeasurementHash(m.SensorID, canonical, m.RawPacket),
			index: humidex(m.Metrics.Microclimate.TemperatureCelsius,
				m.Metrics.Microclimate.RelativeHumidity),
		})
	}
	return rows, rejected
}

// bulkInsertSQL grava o lote INTEIRO em um unico comando (uma ida ao banco):
// insere as medicoes (duplicatas ignoradas) e atualiza o indice de calor de
// cada sensor com o valor da ultima medicao realmente inserida. Substitui o
// padrao anterior de transacao + SAVEPOINT + 2 comandos por medicao.
const bulkInsertSQL = `
WITH input AS (
	SELECT * FROM unnest($1::text[], $2::timestamptz[], $3::text[], $4::text[], $5::float8[])
		WITH ORDINALITY AS t(sensor_id, ts, raw, hash, idx, ord)
),
ins AS (
	INSERT INTO medicoes (sensor_id, timestamp, raw_packet, hash, criado_em)
	SELECT sensor_id, ts, raw, hash, now() FROM input ORDER BY ord
	ON CONFLICT (sensor_id, timestamp) DO NOTHING
	RETURNING sensor_id, timestamp
),
last_idx AS (
	SELECT DISTINCT ON (i.sensor_id) i.sensor_id, i.idx
	FROM input i
	JOIN ins ON ins.sensor_id = i.sensor_id AND ins.timestamp = i.ts
	ORDER BY i.sensor_id, i.ord DESC
),
upd AS (
	INSERT INTO indices_calor (sensor_id, indice, atualizado_em)
	SELECT sensor_id, idx, now() FROM last_idx
	ON CONFLICT (sensor_id) DO UPDATE
		SET indice = EXCLUDED.indice, atualizado_em = now()
)
SELECT count(*) FROM ins`

const maxRowsPerStatement = 1000

// insertRows devolve quantas linhas foram realmente inseridas (o resto era duplicata).
func insertRows(ctx context.Context, rows []preparedRow) (int, error) {
	total := 0
	for start := 0; start < len(rows); start += maxRowsPerStatement {
		end := start + maxRowsPerStatement
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[start:end]

		sensors := make([]string, len(chunk))
		tss := make([]string, len(chunk))
		raws := make([]string, len(chunk))
		hashes := make([]string, len(chunk))
		idxs := make([]float64, len(chunk))
		for i, r := range chunk {
			sensors[i], tss[i], raws[i], hashes[i], idxs[i] = r.sensorID, r.ts, r.raw, r.hash, r.index
		}

		var inserted int
		if err := db.QueryRowContext(ctx, bulkInsertSQL,
			pq.Array(sensors), pq.Array(tss), pq.Array(raws), pq.Array(hashes), pq.Array(idxs),
		).Scan(&inserted); err != nil {
			return total, err
		}
		total += inserted
	}
	return total, nil
}

// ---------- Fila Redis (modo distribuido) ----------

func initRedis() {
	if redisAddr == "" {
		return
	}
	var opt *redis.Options
	if strings.HasPrefix(redisAddr, "redis://") {
		o, err := redis.ParseURL(redisAddr)
		if err != nil {
			log.Printf("aviso: REDIS_URL invalida, fila desativada: %v", err)
			return
		}
		opt = o
	} else {
		opt = &redis.Options{Addr: redisAddr}
	}
	client := redis.NewClient(opt)
	for i := 0; i < 30; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := client.Ping(ctx).Err()
		cancel()
		if err == nil {
			rdb = client
			log.Printf("fila Redis ativa em %s", redisAddr)
			return
		}
		time.Sleep(time.Second)
	}
	log.Printf("aviso: Redis indisponivel, usando escrita direta no banco")
}

// processBody grava no banco um corpo de requisicao que estava na fila.
func processBody(ctx context.Context, body string) error {
	var batch batchInput
	if err := json.Unmarshal([]byte(body), &batch); err != nil {
		log.Printf("aviso: item invalido descartado da fila: %v", err)
		return nil
	}
	rows, _ := prepareRows(batch.Measurements)
	_, err := insertRows(ctx, rows)
	return err
}

// queueWorker move um item da fila para a lista "processando" (atomico),
// grava no banco e so entao o remove. Em erro, devolve o item a fila.
func queueWorker() {
	ctx := context.Background()
	for {
		body, err := rdb.BLMove(ctx, queueKey, processingKey, "LEFT", "RIGHT", time.Second).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				log.Printf("aviso: falha ao ler a fila: %v", err)
				time.Sleep(500 * time.Millisecond)
			}
			continue
		}
		if err := processBody(ctx, body); err != nil {
			log.Printf("aviso: falha ao gravar item da fila, devolvendo: %v", err)
			rdb.RPush(ctx, queueKey, body)
			time.Sleep(200 * time.Millisecond)
		}
		rdb.LRem(ctx, processingKey, 1, body)
	}
}

// waitQueueDrained faz os endpoints de auditoria enxergarem tudo que ja foi
// aceito: espera (ate 15s) a fila e a lista "processando" esvaziarem.
func waitQueueDrained(ctx context.Context) {
	if rdb == nil {
		return
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		pending, err1 := rdb.LLen(ctx, queueKey).Result()
		inflight, err2 := rdb.LLen(ctx, processingKey).Result()
		if err1 != nil || err2 != nil || (pending == 0 && inflight == 0) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func handleTelemetry(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"erro": "corpo invalido"})
		return
	}
	var batch batchInput
	if err := json.Unmarshal(body, &batch); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"erro": "corpo JSON invalido"})
		return
	}

	// Um lote de UMA medicao e valido. So o lote vazio e rejeitado.
	if len(batch.Measurements) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"erro": "lote vazio"})
		return
	}

	ctx := r.Context()
	received := len(batch.Measurements)

	// Modo distribuido: valida barato, enfileira o corpo original e responde 202.
	if rdb != nil {
		valid := 0
		for _, m := range batch.Measurements {
			if validMeasurement(m) {
				valid++
			}
		}
		if valid == 0 {
			writeJSON(w, http.StatusOK, ingestResponse{TotalReceived: received, TotalRejected: received})
			return
		}
		if err := rdb.RPush(ctx, queueKey, body).Err(); err == nil {
			writeJSON(w, http.StatusAccepted, ingestResponse{
				TotalReceived:  received,
				TotalPersisted: valid, // aceitas para persistencia (gravacao assincrona)
				TotalRejected:  received - valid,
			})
			return
		}
		log.Printf("aviso: falha ao enfileirar, gravando direto no banco: %v", err)
	}

	// Modo monolito (ou fallback): grava direto, em uma unica ida ao banco.
	rows, rejected := prepareRows(batch.Measurements)
	inserted, err := insertRows(ctx, rows)
	if err != nil {
		log.Printf("aviso: falha ao gravar lote: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"erro": "falha ao gravar lote"})
		return
	}
	writeJSON(w, http.StatusOK, ingestResponse{
		TotalReceived:  received,
		TotalPersisted: len(rows), // duplicata ja esta gravada: conta como persistida
		TotalRejected:  rejected,
		TotalDuplicate: len(rows) - inserted,
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
	waitQueueDrained(ctx)
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
	waitQueueDrained(ctx)

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

// waitForDB espera o banco aceitar conexoes e o schema (init.sql) existir.
func waitForDB() {
	for i := 1; i <= 60; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var ok bool
		err := db.QueryRowContext(ctx,
			`SELECT to_regclass('public.medicoes') IS NOT NULL AND to_regclass('public.indices_calor') IS NOT NULL`).Scan(&ok)
		cancel()
		if err == nil && ok {
			return
		}
		log.Printf("aguardando banco/schema (%d/60) ok=%v err=%v", i, ok, err)
		time.Sleep(time.Second)
	}
	log.Fatalf("banco ou schema indisponivel apos 60 tentativas")
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

	waitForDB()
	initRedis()
	if rdb != nil {
		workers := envInt("QUEUE_WORKERS", 2)
		for i := 0; i < workers; i++ {
			go queueWorker()
		}
	}

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