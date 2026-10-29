package main

// avaliar_justo.go — APS integrity verifier (100% API-driven audit).
//
// This program never touches PostgreSQL. It consumes only the application
// contract:
//   GET /audit/contagem               -> {"total_medicoes":N,"total_indices":M}
//   GET /audit/medidas?cursor=&limit= -> {"medidas":[{id,sensor_id,timestamp,
//                                         raw_packet,hash}...],"proximo_cursor":"..."}
//
// MANIFEST: script.js emits the "medida_gerada" metric with the sensor and
// hash tags for EVERY dispatched measurement. `--out json` writes stream.json
// into the run directory, which lets this verifier rebuild the exact sent set.
//
// CROSS-CHECK (manifest vs API):
//   divergent  stored hash != recomputed hash over sensor_id+timestamp+raw_packet
//   ghosts     present in the API, absent from the manifest
//   lost       present in the manifest, absent from the API
//   duplicates same (sensor_id, timestamp) pair stored more than once
//
// FINAL LOG — three blocks:
//   INGESTAO (POST)  accepted / lost + rates
//   CONSULTA (GET)   accepted / lost + rates
//   REGISTROS        sent / rejected / stored + percentages
// The grade is the direct ratio between stored and sent measurements.
//
// Inputs:  $RUN_DIR/report.json, $RUN_DIR/stream.json
// Output:  $RUN_DIR/resultado_registros.txt  (one line, ';'-separated)
// Env:     API_BASE_URL   (default http://aps-app:8080)
//          RUN_DIR        (default resultados)
//          DRAIN_TIMEOUT  (default 90, in seconds)

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// ---------- Environment ----------

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envSeconds(key string, fallback int) time.Duration {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil || v <= 0 {
		v = fallback
	}
	return time.Duration(v) * time.Second
}

func apiBase() string {
	return envString("API_BASE_URL", "http://aps-app:8080")
}

// runDir is the single output directory of a run. The orchestrator injects
// RUN_DIR pointing at resultados/CenarioX/<timestamp>/, so report.json,
// stream.json and resultado_registros.txt all live side by side. Nothing is
// ever written to the project root.
func runDir() string {
	return envString("RUN_DIR", "resultados")
}

func runPath(name string) string {
	return filepath.Join(runDir(), name)
}

const resultFile = "resultado_registros.txt"

// writeResult is the only writer in the program. It creates RUN_DIR if needed
// and reports failure to the console instead of failing silently.
func writeResult(content string) {
	if err := os.MkdirAll(runDir(), 0o755); err != nil {
		fmt.Println("ERRO: nao foi possivel criar o diretorio de saida:", err)
		return
	}
	target := runPath(resultFile)
	if err := os.WriteFile(target, []byte(content+"\n"), 0o644); err != nil {
		fmt.Println("ERRO: nao foi possivel gravar", target, ":", err)
	}
}

func percentage(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

// ---------- report.json contract ----------

type metricValue struct {
	Value float64 `json:"value"`
	Count int64   `json:"count"`
	Rate  float64 `json:"rate"`
}

type reportMetrics struct {
	HTTPRequests     metricValue `json:"http_reqs"`
	Iterations       metricValue `json:"iterations"`
	Dropped          metricValue `json:"dropped_iterations"`
	WritesDone       metricValue `json:"escritas_realizadas"`
	RejectedMeasures metricValue `json:"medidas_rejeitadas"`
	MeasureGenerated metricValue `json:"medida_gerada"`
	PostAccepted     metricValue `json:"post_aceitos"`
	PostLost         metricValue `json:"post_perdidos"`
	GetAccepted      metricValue `json:"get_aceitos"`
	GetLost          metricValue `json:"get_perdidos"`
	ReadWithoutData  metricValue `json:"leitura_sem_dado"`
}

type k6Report struct {
	Metrics reportMetrics `json:"metrics"`
}

// ---------- API contract ----------

type measurementAPI struct {
	ID        int64  `json:"id"`
	SensorID  string `json:"sensor_id"`
	Timestamp string `json:"timestamp"`
	RawPacket string `json:"raw_packet"`
	Hash      string `json:"hash"`
}

type countAPI struct {
	TotalMeasurements int64 `json:"total_medicoes"`
	TotalIndices      int64 `json:"total_indices"`
}

var httpClient = &http.Client{Timeout: 60 * time.Second}

func getJSON(endpoint string, target interface{}) error {
	resp, err := httpClient.Get(endpoint)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d em %s", resp.StatusCode, endpoint)
	}
	return json.NewDecoder(resp.Body).Decode(target)
}

// computeHash recomputes the anchor from the fields returned by the API.
// It MUST stay identical to computeMeasurementHash in main.go.
func computeHash(m measurementAPI) string {
	sum := sha256.Sum256([]byte(m.SensorID + m.Timestamp + m.RawPacket))
	return hex.EncodeToString(sum[:])
}

// ---------- Manifest ----------

func readManifest() (map[string]int, error) {
	file, err := os.Open(runPath("stream.json"))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	manifest := map[string]int{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)
	for scanner.Scan() {
		var point struct {
			Metric string `json:"metric"`
			Data   struct {
				Tags map[string]string `json:"tags"`
			} `json:"data"`
		}
		if json.Unmarshal(scanner.Bytes(), &point) != nil {
			continue
		}
		if point.Metric != "medida_gerada" {
			continue
		}
		if h := point.Data.Tags["hash"]; h != "" {
			manifest[h]++
		}
	}
	return manifest, scanner.Err()
}

// ---------- API collection ----------

// drainCount polls /audit/contagem until both counters stop moving for three
// consecutive cycles. There is no early break on stored >= sent: with batches,
// stored grows far above sent and an early break would cut the drain short.
func drainCount() (countAPI, error) {
	var last countAPI
	var prevMeasurements, prevIndices int64 = -1, -1
	stable := 0
	deadline := time.Now().Add(envSeconds("DRAIN_TIMEOUT", 90))

	for {
		var current countAPI
		if err := getJSON(apiBase()+"/audit/contagem", &current); err != nil {
			if time.Now().After(deadline) {
				return last, fmt.Errorf("drenagem interrompida: %w", err)
			}
			time.Sleep(2 * time.Second)
			continue
		}
		last = current

		if current.TotalMeasurements == prevMeasurements && current.TotalIndices == prevIndices {
			stable++
			if stable >= 3 {
				return current, nil
			}
		} else {
			stable = 0
		}
		prevMeasurements = current.TotalMeasurements
		prevIndices = current.TotalIndices

		if time.Now().After(deadline) {
			return current, nil
		}
		time.Sleep(2 * time.Second)
	}
}

func fetchMeasurements() ([]measurementAPI, error) {
	var all []measurementAPI
	cursor := ""
	for {
		endpoint := fmt.Sprintf("%s/audit/medidas?limit=1000&cursor=%s",
			apiBase(), url.QueryEscape(cursor))
		var page struct {
			Measurements []measurementAPI `json:"medidas"`
			NextCursor   string           `json:"proximo_cursor"`
		}
		if err := getJSON(endpoint, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Measurements...)
		if page.NextCursor == "" {
			return all, nil
		}
		if page.NextCursor == cursor {
			return nil, fmt.Errorf("cursor nao avancou em %q: paginacao inconsistente", cursor)
		}
		cursor = page.NextCursor
	}
}

// ---------- Analysis ----------

type auditResult struct {
	divergent  int
	ghosts     int
	lost       int
	duplicates int
	collected  int
}

func analyze(manifest map[string]int, manifestOK bool, measurements []measurementAPI) auditResult {
	r := auditResult{collected: len(measurements)}

	persisted := map[string]int{}
	pairs := map[string]int{}

	for _, m := range measurements {
		if m.Hash != computeHash(m) {
			r.divergent++
		}
		persisted[m.Hash]++
		pairs[m.SensorID+"\x00"+m.Timestamp]++
	}

	for _, n := range pairs {
		if n > 1 {
			r.duplicates += n - 1
		}
	}

	if manifestOK {
		for h, n := range manifest {
			if persisted[h] == 0 {
				r.lost += n
			}
		}
		for h, n := range persisted {
			if manifest[h] == 0 {
				r.ghosts += n
			}
		}
	}

	return r
}

// ---------- Pipeline ----------

func main() {
	// 1. report.json: k6 counters.
	var report k6Report
	raw, err := os.ReadFile(runPath("report.json"))
	if err != nil {
		writeResult("ERRO_AQUISICAO_JSON")
		fmt.Println("ERRO: nao foi possivel ler", runPath("report.json"), ":", err)
		return
	}
	if err := json.Unmarshal(raw, &report); err != nil {
		writeResult("ERRO_JSON_CORROMPIDO")
		fmt.Println("ERRO: report.json invalido:", err)
		return
	}

	// 2. Manifest of everything actually dispatched.
	manifest, manifestErr := readManifest()
	manifestOK := manifestErr == nil
	if !manifestOK {
		fmt.Println("AVISO: manifesto indisponivel (", manifestErr,
			"); fantasmas e perdidas nao serao aferidos")
	}

	// 3. Drain and fetch through the API.
	count, err := drainCount()
	if err != nil {
		writeResult("ERRO_AUDITORIA_API")
		fmt.Println("ERRO:", err)
		return
	}
	measurements, err := fetchMeasurements()
	if err != nil {
		writeResult("ERRO_AUDITORIA_API")
		fmt.Println("ERRO:", err)
		return
	}

	// 4. Content-to-content cross-check.
	r := analyze(manifest, manifestOK, measurements)

	// 5. Counters.
	postAccepted := report.Metrics.PostAccepted.Count
	postLost := report.Metrics.PostLost.Count
	getAccepted := report.Metrics.GetAccepted.Count
	getLost := report.Metrics.GetLost.Count
	readWithoutData := report.Metrics.ReadWithoutData.Count

	sent := report.Metrics.MeasureGenerated.Count
	rejected := report.Metrics.RejectedMeasures.Count
	stored := count.TotalMeasurements
	indices := count.TotalIndices

	// 6. Rates.
	postRate := percentage(postAccepted, postAccepted+postLost)
	getRate := percentage(getAccepted, getAccepted+getLost)
	rejectedRate := percentage(rejected, sent)
	storedRate := percentage(stored, sent)

	// 7. Machine-readable output: one line, ';'-separated fields.
	writeResult(fmt.Sprintf("%d;%d;%d;%.2f%%;%d;%d;%d;%d",
		sent, rejected, stored, storedRate,
		r.divergent, r.ghosts, r.lost, r.duplicates))

	// 8. Human-readable output.
	fmt.Println()
	fmt.Println("==============================================")
	fmt.Println("RESULTADO DA AVALIACAO")
	fmt.Println("==============================================")
	fmt.Println()
	fmt.Println("INGESTAO (POST /telemetria)")
	fmt.Printf("  Aceitas:     %6d  (%.2f%%)\n", postAccepted, postRate)
	fmt.Printf("  Perdidas:    %6d  (%.2f%%)\n", postLost, 100.0-postRate)
	fmt.Printf("  Total:       %6d\n", postAccepted+postLost)
	fmt.Println()
	fmt.Println("CONSULTA (GET /indice-calor)")
	fmt.Printf("  Aceitas:     %6d  (%.2f%%)\n", getAccepted, getRate)
	fmt.Printf("  Perdidas:    %6d  (%.2f%%)\n", getLost, 100.0-getRate)
	fmt.Printf("  Total:       %6d\n", getAccepted+getLost)
	fmt.Printf("  Sem dado:    %6d\n", readWithoutData)
	fmt.Println()
	fmt.Println("REGISTROS")
	fmt.Printf("  Enviados:    %6d\n", sent)
	fmt.Printf("  Rejeitados:  %6d  (%.2f%%)\n", rejected, rejectedRate)
	fmt.Printf("  Gravados:    %6d  (%.2f%%)\n", stored, storedRate)
	fmt.Println()
	fmt.Println("==============================================")
	fmt.Printf("TAXA DE PERSISTENCIA: %.2f%%  (gravados / enviados)\n", storedRate)
	fmt.Printf("NOTA (proporcional):  %.2f / 10.00\n", storedRate/10.0)
	fmt.Println("==============================================")
	fmt.Println()
	fmt.Println("INTEGRIDADE (cruzamento manifesto x API)")
	if manifestOK {
		fmt.Printf("  Divergentes: %d | Fantasmas: %d | Perdidas: %d | Duplicatas: %d\n",
			r.divergent, r.ghosts, r.lost, r.duplicates)
	} else {
		fmt.Println("  Manifesto ausente: fantasmas e perdidas nao aferidos")
		fmt.Printf("  Divergentes: %d | Duplicatas: %d\n", r.divergent, r.duplicates)
	}
	fmt.Printf("  Coletadas:   %d | Medicoes: %d | Indices: %d\n",
		r.collected, stored, indices)

	// Um indice por sensor e N medicoes por sensor: as duas contagens nunca
	// coincidem por construcao. O que se verifica e que nao exista indice
	// sem medicao, o que indicaria corrupcao.
	if indices > stored {
		fmt.Println("  Consistencia entre tabelas: DIVERGENCIA (mais indices que medicoes)")
	} else {
		fmt.Println("  Consistencia entre tabelas: OK")
	}
	if storedRate > 100.0 {
		fmt.Println("  AVISO: taxa acima de 100%. Ha dado residual de uma rodada anterior.")
	}
}