import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import crypto from 'k6/crypto';
import { Counter } from 'k6/metrics';

// ---- Contadores de requisicao (POST e GET) ----
const postAceitos  = new Counter('post_aceitos');
const postPerdidos = new Counter('post_perdidos');
const getAceitos   = new Counter('get_aceitos');
const getPerdidos  = new Counter('get_perdidos');

// ---- Contadores de medida (registros) ----
const escritas          = new Counter('escritas_realizadas');
const medidasRejeitadas = new Counter('medidas_rejeitadas');
const medidaGerada      = new Counter('medida_gerada');

// Diagnostico do lado de leitura: 404 significa que o sensor existe no
// universo mas a medicao ainda nao foi gravada, ou que o sorteio saiu do
// intervalo ja criado. Nao e erro do gerador, mas precisa ser visivel.
const leituraSemDado = new Counter('leitura_sem_dado');

http.setResponseCallback(http.expectedStatuses({ min: 200, max: 499 }));

function envInt(nome, padrao) {
  const v = parseInt(__ENV[nome], 10);
  return isNaN(v) ? padrao : v;
}

function envFloat(nome, padrao) {
  const v = parseFloat(__ENV[nome]);
  return isNaN(v) ? padrao : v;
}

const WRITE_RATE  = envInt('WRITE_RATE', 1);
const READ_RATE   = envInt('READ_RATE', 8);
const PEAK_START  = envInt('PEAK_START', 60);
const PEAK_FACTOR = envInt('PEAK_FACTOR', 3);
const BATCH_MIN   = envInt('BATCH_MIN', 10);
const BATCH_MAX   = envInt('BATCH_MAX', 50);

const SPIKE_WINDOW = 14;
const TEST_TOTAL = PEAK_START + 5 * SPIKE_WINDOW + 30;
const intRate = (v) => Math.max(1, Math.round(v));

// ============================================================
// UNIVERSO DE SENSORES
//
// escrita_base roda a WRITE_RATE por TEST_TOTAL segundos e cada iteracao
// cria exatamente UM sensor_id novo, em ordem crescente. Ao final do
// cenario base existem BASE_IDS IDs distintos:
//   SNS-5G-SEQ-000001 ... SNS-5G-SEQ-<BASE_IDS>
//
// Ler fora desse universo e garantia de 404. O pool de leitura e portanto
// uma FRACAO do universo, nunca um numero absoluto arbitrario.
//
// pico_escrita usa offset 100000, entao os IDs do pico ficam fora do pool
// de proposito: a leitura mede a base, que e o conjunto estavel de dados.
// ============================================================
const BASE_IDS = WRITE_RATE * TEST_TOTAL;

const READ_POOL_RATIO = Math.min(1, Math.max(0.05, envFloat('READ_POOL_RATIO', 0.8)));
const READ_POOL_MAX = Math.max(1, Math.floor(BASE_IDS * READ_POOL_RATIO));

export const options = {
  scenarios: {
	
    escrita_base: {
      executor: 'constant-arrival-rate',
      rate: WRITE_RATE,
      timeUnit: '1s',
      duration: `${TEST_TOTAL}s`,
      preAllocatedVUs: 30,
      maxVUs: 200,
    },
	
    leitura_base: {
      executor: 'constant-arrival-rate',
      startTime: `${PEAK_START/2}s`,
	  rate: READ_RATE,
      timeUnit: '1s',
      duration: `${TEST_TOTAL}s`,
      preAllocatedVUs: 50,
      maxVUs: 800,
    },
    pico_escrita: {
      executor: 'ramping-arrival-rate',
      startTime: `${PEAK_START - SPIKE_WINDOW / 2}s`,
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 30,
      maxVUs: 600,
      stages: [
        { target: intRate(WRITE_RATE * PEAK_FACTOR / 2), duration: '2s' },
        { target: intRate(WRITE_RATE * PEAK_FACTOR), duration: '10s' },
        { target: 0, duration: '2s' },
        { target: 0, duration: '14s' },
        { target: intRate(WRITE_RATE * PEAK_FACTOR / 2), duration: '2s' },
        { target: intRate(WRITE_RATE * PEAK_FACTOR), duration: '10s' },
        { target: 0, duration: '2s' },
        { target: 0, duration: '14s' },
        { target: intRate(WRITE_RATE * PEAK_FACTOR / 2), duration: '2s' },
        { target: intRate(WRITE_RATE * PEAK_FACTOR), duration: '10s' },
        { target: 0, duration: '2s' },
      ],
    },
	
    pico_leitura: {
      executor: 'ramping-arrival-rate',
      startTime: `${PEAK_START}s`,
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 1000,
      stages: [
        { target: intRate(READ_RATE * PEAK_FACTOR / 2), duration: '2s' },
        { target: intRate(READ_RATE * PEAK_FACTOR), duration: '10s' },
        { target: 0, duration: '2s' },
        { target: 0, duration: '14s' },
        { target: intRate(READ_RATE * PEAK_FACTOR / 2), duration: '2s' },
        { target: intRate(READ_RATE * PEAK_FACTOR), duration: '10s' },
        { target: 0, duration: '2s' },
        { target: 0, duration: '14s' },
        { target: intRate(READ_RATE * PEAK_FACTOR / 2), duration: '2s' },
        { target: intRate(READ_RATE * PEAK_FACTOR), duration: '10s' },
        { target: 0, duration: '2s' },
      ],
    },
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://aps-app:8080';

// setup roda UMA vez e o retorno e compartilhado com todas as VUs.
// Cada VU tem seu proprio runtime JS em k6, entao uma variavel de modulo
// nao serviria para marcar o instante zero do teste.
export function setup() {
  return { inicio: Date.now() };
}

function sensorId(seq) {
  return 'SNS-5G-SEQ-' + String(seq).padStart(6, '0');
}

// Timestamp canonico UTC com SEIS casas decimais. Mesmo layout que a
// aplicacao devolve em /audit/medidas, o que faz o hash bater no round-trip.
function canonicalTimestamp(ms) {
  const iso = new Date(ms).toISOString();
  return iso.replace(/\.(\d{3})Z$/, '.$1000Z');
}

function hashMedida(id, tsCanonico, rawPacket) {
  return crypto.sha256(id + tsCanonico + rawPacket, 'hex');
}

function generateSyntheticBatch(id) {
  const faixa = BATCH_MAX - BATCH_MIN + 1;
  const quantidade = BATCH_MIN + Math.floor(Math.random() * faixa);
  const agora = Date.now();
  const medidas = [];
  for (let i = 0; i < quantidade; i++) {
    const ts = canonicalTimestamp(agora - (quantidade - i) * 60000);
    const raw = Math.random().toString(36).substring(2);
    const h = hashMedida(id, ts, raw);
    // Manifesto: registra ANTES do POST. Se a requisicao estourar o timeout,
    // a medida continua contabilizada como despachada.
    medidaGerada.add(1, { sensor: id, hash: h });
    medidas.push({
      sensor_id: id,
      timestamp: ts,
      metrics: {
        microclimate: {
          temperature_celsius: parseFloat((18 + Math.random() * 15).toFixed(1)),
          relative_humidity_percentage: parseFloat((50 + Math.random() * 45).toFixed(1)),
        },
      },
      raw_telemetry_packet: raw,
    });
  }
  return { measurements: medidas };
}

// Quantos sensor_ids a escrita base ja criou neste instante.
// A margem de 1 segundo absorve a defasagem entre a taxa nominal e a
// criacao efetiva: uma iteracao pode estar em voo quando a leitura sorteia.
function sensoresCriados(data) {
  const decorrido = Math.max(0, (Date.now() - data.inicio) / 1000);
  const estimado = Math.floor(Math.max(0, decorrido - 1) * WRITE_RATE);
  return Math.min(READ_POOL_MAX, Math.max(1, estimado));
}

export default function (data) {
  const cenario = exec.scenario.name;
  const PARAMS = {
    headers: { 'Content-Type': 'application/json' },
    timeout: '5s',
  };

  if (cenario === 'escrita_base' || cenario === 'pico_escrita') {
    const offset = cenario === 'pico_escrita' ? 100000 : 0;
    const seq = exec.scenario.iterationInTest + 1 + offset;
    const id = sensorId(seq);
    const lote = generateSyntheticBatch(id);
    const payload = JSON.stringify(lote);
    const res = http.post(BASE_URL + '/telemetria', payload, PARAMS);

    if (res.status >= 200 && res.status < 300) {
      postAceitos.add(1);
      escritas.add(lote.measurements.length);
    } else {
      postPerdidos.add(1);
      medidasRejeitadas.add(lote.measurements.length);
    }
    check(res, {
      'iot_write: status aceito (2xx)': (r) => r.status >= 200 && r.status < 300,
    });
  } else {
    // Pool dinamico: sorteia apenas dentro dos IDs que ja existem agora.
    const criados = sensoresCriados(data);
    const seq = 1 + Math.floor(Math.random() * criados);
    const id = sensorId(seq);
    const res = http.get(BASE_URL + '/indice-calor/' + id, PARAMS);

    if (res.status === 404) leituraSemDado.add(1);

    if ((res.status >= 200 && res.status < 300) || res.status === 404) {
      getAceitos.add(1);
    } else {
      getPerdidos.add(1);
    }
    check(res, {
      'db_read: resposta valida (2xx ou 404)': (r) =>
        (r.status >= 200 && r.status < 300) || r.status === 404,
    });
  }
}