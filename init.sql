-- ============================================================
-- init.sql — esquema da APS
--
-- Aplicado uma vez pelo serviço db-init, antes da aplicação subir.
-- O avaliador NUNCA acessa este banco: toda a auditoria passa pelas
-- rotas HTTP da aplicação.
-- ============================================================

-- ---------- medições ----------
-- A constraint UNIQUE(sensor_id, timestamp) é obrigatória: o handler de
-- ingestão usa ON CONFLICT (sensor_id, timestamp) DO NOTHING, e sem ela o
-- Postgres devolve erro 42P10 e rejeita TODAS as inserções.
--
-- criado_em registra o instante de recebimento pelo servidor e NÃO
-- substitui o timestamp do evento: são grandezas distintas, e apenas o
-- timestamp do evento compõe o hash da medição.
CREATE TABLE IF NOT EXISTS medicoes (
    id          BIGSERIAL   PRIMARY KEY,
    sensor_id   TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    raw_packet  TEXT        NOT NULL,
    hash        TEXT        NOT NULL,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT medicoes_sensor_timestamp_uniq UNIQUE (sensor_id, timestamp)
);

-- Índice sobre id: a paginação por cursor faz WHERE id > $1 ORDER BY id
-- LIMIT $2. Sem ele, cada página vira varredura completa da tabela.
CREATE INDEX IF NOT EXISTS medicoes_id_idx ON medicoes (id);

-- Índice sobre sensor_id: usado pelo upsert do índice de calor e por
-- consultas de diagnóstico.
CREATE INDEX IF NOT EXISTS medicoes_sensor_idx ON medicoes (sensor_id);

-- ---------- índices de calor ----------
-- Um registro por sensor, atualizado a cada medição recebida.
CREATE TABLE IF NOT EXISTS indices_calor (
    sensor_id     TEXT             PRIMARY KEY,
    indice        DOUBLE PRECISION NOT NULL,
    atualizado_em TIMESTAMPTZ      NOT NULL DEFAULT now()
);