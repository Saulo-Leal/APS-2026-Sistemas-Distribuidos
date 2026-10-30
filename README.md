# APS 2026 — Sistemas Distribuídos

Serviço de **telemetria de sensores IoT** (medições de microclima) implementado em dois cenários, para comparar uma arquitetura **monolítica** com uma **distribuída** sob a mesma carga e o mesmo orçamento de CPU:

| Cenário | Arquivo | Arquitetura |
|---|---|---|
| **A — Monolito** | `docker-compose.monolito.yaml` | 1 aplicação Go + 1 PostgreSQL |
| **B — Distribuído** | `docker-compose.distribuido.yaml` | Nginx + 6 réplicas Go + Redis (fila) + 1 PostgreSQL |

O benchmark sobe o cenário escolhido, dispara carga com o **k6**, audita o que foi gravado e gera um resumo com a taxa de persistência.

---

## 1. O que a aplicação faz

A aplicação (`main.go`, em Go) recebe lotes de medições de sensores, calcula um índice de calor (*humidex*) por sensor e permite consultar o índice e auditar tudo o que foi gravado.

### Endpoints

| Método e rota | Função |
|---|---|
| `GET /health` | Verifica se a aplicação e o banco estão prontos |
| `POST /telemetria` | Recebe um lote de medições |
| `GET /indice-calor/{sensor_id}` | Devolve o índice de calor do sensor (`404` se o sensor ainda não tem medição) |
| `GET /audit/contagem` | Total de medições e de índices gravados |
| `GET /audit/medidas?limit=&cursor=` | Lista as medições gravadas, paginadas por cursor |

### Exemplo de `POST /telemetria`

```json
{
  "measurements": [
    {
      "sensor_id": "SNS-5G-SEQ-000001",
      "timestamp": "2026-10-30T21:44:00.123456Z",
      "metrics": {
        "microclimate": {
          "temperature_celsius": 31.5,
          "relative_humidity_percentage": 62.0
        }
      },
      "raw_telemetry_packet": "pacote-bruto-do-sensor"
    }
  ]
}
```

Resposta no Cenário A (gravação direta): `200`, com `total_recebido`, `total_persistido`, `total_rejeitado` e `total_duplicado`.
Resposta no Cenário B (fila): `202 Accepted` — a medição foi **aceita** e será gravada de forma assíncrona.

### Regras do contrato

- Lote vazio é rejeitado (`400`); lote com **uma** medição é válido.
- Uma medição é rejeitada se tiver `sensor_id` ou `raw_telemetry_packet` vazio, ou timestamp inválido.
- Cada medição recebe um **hash**: `SHA-256(sensor_id + timestamp_canônico + raw_packet)`, onde o timestamp canônico é UTC com seis casas decimais e sufixo `Z`. O auditor recalcula esse hash para detectar divergências.
- A dupla `(sensor_id, timestamp)` é única. Uma duplicata é ignorada e conta como persistida (não como perda).
- A entrada usa `raw_telemetry_packet`; a saída usa `raw_packet`. A assimetria é intencional e faz parte do contrato.

---

## 2. Arquitetura

### Cenário A — Monolito

```
k6 ──► aps-app (Go, 0.05 vCPU) ──► PostgreSQL (0.8 vCPU)
```

### Cenário B — Distribuído (orçamento total: 0,80 vCPU)

```
                      ┌─► app-node-1 ─┐
k6 ──► Nginx (0.05) ──┼─► app-node-2 ─┼──► PostgreSQL (0.40)
                      ├─►   ...       │
                      └─► app-node-6 ─┘
                              │  ▲
                              ▼  │
                          Redis (0.05)  ← fila de escrita
```

- **Nginx** (`aps-app`): porta de entrada, balanceia entre as 6 réplicas.
- **app-node** (6 × 0,05 vCPU): a mesma aplicação Go, com healthcheck em `/health`.
- **Redis**: fila de escrita (sem persistência em disco, teto de 48 MB).
- **PostgreSQL** (`postgres-dist`): banco dedicado, com `synchronous_commit=off`.
- **db-init**: aplica o `init.sql` uma vez e encerra.

### Como a escrita funciona

1. **Em lote:** o lote inteiro é gravado por **um único comando SQL** (`unnest` + CTEs), que insere as medições, ignora duplicatas e atualiza o índice de calor de cada sensor.
2. **Fila (só quando `REDIS_URL` está definida, ou seja, no Cenário B):** o `POST` valida, coloca o corpo na fila Redis e responde `202`. Dois workers por réplica retiram os itens, gravam no banco e só então os removem da lista de processamento. Se a gravação falhar, o item volta para a fila.
3. **Auditoria:** `/audit/contagem` e `/audit/medidas` esperam (até 15 s) a fila esvaziar antes de responder, para que o auditor veja tudo o que foi aceito.
4. **Fallback:** se o Redis estiver indisponível ou cheio, a aplicação grava direto no banco.

---

## 3. Estrutura do projeto

| Arquivo | Para que serve |
|---|---|
| `main.go` | Aplicação Go (usada nos dois cenários) |
| `init.sql` | Schema do banco (`medicoes`, `indices_calor`) |
| `Dockerfile.monolito` | Build da aplicação do Cenário A |
| `Dockerfile.distribuido` | Build da aplicação do Cenário B (dependências fixadas) |
| `docker-compose.monolito.yaml` | Sistema sob teste — Cenário A |
| `docker-compose.distribuido.yaml` | Sistema sob teste — Cenário B |
| `Nginx.conf` | Configuração do balanceador do Cenário B |
| `docker-compose.teste.yaml` | Plataforma de teste: espera a aplicação, roda o k6 e o avaliador |
| `script.js` | Carga do k6 |
| `avaliar_justo.go` | Auditor: confere o que foi enviado contra o que foi gravado |
| `run_benchmark.bat` / `run_benchmark.sh` | Orquestradores originais (Windows / Linux) |
| `run_bechmark_MOD.bat` | Orquestrador (Windows) que **mantém containers e logs quando há falha** |

---

## 4. Pré-requisitos

- **Docker Desktop** (Windows/macOS) ou Docker Engine (Linux), com o `docker compose` v2 em execução. O Cenário B usa `start_interval` no healthcheck, que exige **Docker 25 ou mais recente**.
- **Acesso à internet** na primeira execução, para baixar as imagens (`golang`, `postgres`, `redis`, `nginx`, `grafana/k6`) e as dependências Go durante o build.
- Porta **8080** livre no computador.
- Cerca de 2 GB de RAM livres para o Docker.
- Não é necessário instalar Go nem k6: tudo roda em containers.

---

## 5. Passo a passo

### 5.1 Rodar o benchmark completo

1. Abra o Docker Desktop e aguarde ele ficar pronto.
2. Abra um terminal na **pasta do projeto** (a que contém `script.js` e os arquivos `docker-compose.*`).
3. Execute o cenário desejado:

   **Windows (cmd):**
   ```
   run_bechmark_MOD B
   ```
   **Windows (PowerShell):**
   ```
   .\run_bechmark_MOD B
   ```
   **Linux/macOS:**
   ```
   ./run_benchmark.sh B
   ```

   Opções do argumento: `A` (monolito), `B` (distribuído) ou nenhum argumento (roda os dois, A primeiro).
4. Aguarde. O script executa cinco etapas e leva em torno de 3 a 6 minutos por cenário:
   1. Sobe o sistema sob teste (`docker compose up --build`).
   2. Dispara a carga do k6 (cerca de 2 min 10 s) depois de esperar o `/health`.
   3. Audita os resultados com o `avaliar_justo.go`, que também espera a drenagem.
   4. Salva os arquivos da rodada.
   5. Derruba os containers e volumes.
5. No fim, o resumo comparativo é impresso no terminal.

### 5.2 Onde ficam os resultados

Cada rodada grava em `resultados/CenarioA/<data_hora>/` ou `resultados/CenarioB/<data_hora>/`:

| Arquivo | Conteúdo |
|---|---|
| `resumo.txt` | Resumo da rodada (o principal) |
| `resultado_registros.txt` | Mesmos números, em uma linha separada por `;` |
| `report.json` | Resumo do k6 (latências, falhas, requisições) |
| `stream.json` | Todas as métricas do k6 (usado como manifesto pelo auditor) |
| `console_teste.txt` | Log do k6 e do avaliador |
| `console_sistema.txt` | Logs da aplicação, do Nginx, do Redis e do banco |

### 5.3 Como ler o `resumo.txt`

| Campo | Significado |
|---|---|
| **Enviados** | Medições geradas pelo k6 |
| **Rejeitados** / **Perdidas** | Medições enviadas que não aparecem gravadas |
| **Gravados** | Medições encontradas via `/audit/medidas` |
| **Taxa persistência** | Gravados ÷ Enviados (é a nota do cenário) |
| **Divergentes** | Hash gravado diferente do recalculado (deve ser 0) |
| **Fantasmas** | Gravadas, mas ausentes do manifesto de envio (deve ser 0) |
| **Duplicatas** | Mesmo `(sensor_id, timestamp)` gravado mais de uma vez (deve ser 0) |

Referência de uma rodada do Cenário B: 29.702 enviadas, 29.702 gravadas (100%), 0 divergentes, 0 fantasmas, 0 duplicatas, latência média de 44 ms.

### 5.4 Ajustar a carga do teste

As variáveis abaixo têm valor padrão no `docker-compose.teste.yaml` e podem ser definidas **antes** de rodar o script:

| Variável | Padrão | Descrição |
|---|---|---|
| `WRITE_RATE` | 4 | Requisições de escrita por segundo (carga base) |
| `READ_RATE` | 50 | Requisições de leitura por segundo (carga base) |
| `PEAK_FACTOR` | 3 | Multiplicador de carga nos picos |
| `PEAK_START` | 30 | Segundos de carga base antes dos picos |
| `BATCH_MIN` / `BATCH_MAX` | 10 / 60 | Tamanho mínimo e máximo de cada lote enviado |
| `READ_POOL_RATIO` | 0.6 | Fração dos sensores usada nas leituras |
| `DRAIN_TIMEOUT` | 90 | Segundos que o auditor espera a drenagem |

Exemplo (Windows cmd):
```
set WRITE_RATE=2
set READ_RATE=20
run_bechmark_MOD B
```
Exemplo (Linux/macOS): `WRITE_RATE=2 READ_RATE=20 ./run_benchmark.sh B`

### 5.5 Comparar os dois cenários

1. Rode `run_bechmark_MOD A` e depois `run_bechmark_MOD B` (ou apenas `run_bechmark_MOD` para rodar os dois em sequência).
2. Compare o `resumo.txt` das duas pastas mais recentes em `resultados/`.
3. No `console_teste.txt`, veja também a latência (`http_req_duration`) e a taxa de falhas (`http_req_failed`).

---

## 6. Usar a aplicação manualmente (sem o k6)

Útil para testar os endpoints à mão.

1. Crie a rede compartilhada (uma única vez):
   ```
   docker network create aps-net
   ```
2. Suba o cenário desejado:
   ```
   docker compose -p aps-sistema -f docker-compose.distribuido.yaml up -d --build
   ```
   (para o monolito, use `docker-compose.monolito.yaml`)
3. Aguarde o `/health` responder:
   ```
   curl http://localhost:8080/health
   ```
   A resposta esperada é `{"status":"pronto"}`.
4. Envie uma medição:
   ```
   curl -X POST http://localhost:8080/telemetria -H "Content-Type: application/json" -d "{\"measurements\":[{\"sensor_id\":\"SNS-1\",\"timestamp\":\"2026-10-30T21:44:00.123456Z\",\"metrics\":{\"microclimate\":{\"temperature_celsius\":31.5,\"relative_humidity_percentage\":62}},\"raw_telemetry_packet\":\"abc\"}]}"
   ```
   No Windows PowerShell, use `curl.exe` em vez de `curl`.
5. Consulte o índice e a auditoria:
   ```
   curl http://localhost:8080/indice-calor/SNS-1
   curl http://localhost:8080/audit/contagem
   curl "http://localhost:8080/audit/medidas?limit=10"
   ```
6. Quando terminar, derrube tudo:
   ```
   docker compose -p aps-sistema -f docker-compose.distribuido.yaml down -v
   docker network rm aps-net
   ```

---

## 7. Solução de problemas

O `run_bechmark_MOD.bat` mantém os containers e salva os logs quando um cenário falha (`KEEP_ON_FAIL=1` é o padrão). Para voltar a limpar tudo sempre, defina `set KEEP_ON_FAIL=0` antes de rodar.

**Se um cenário falhar:**

1. Abra `resultados/Cenario<A|B>/<data_hora>/` e leia:
   - `falha_ps.txt` — estado de cada container (procure por `Exited` ou `unhealthy`);
   - `falha_console_sistema.txt` — logs da aplicação, do Nginx, do Redis e do banco;
   - `falha_console_teste.txt` — logs da plataforma de teste.
2. Para investigar ao vivo, com os containers ainda de pé:
   ```
   docker ps -a
   docker logs aps-sistema-app-node-1
   docker compose -p aps-sistema -f docker-compose.distribuido.yaml logs
   ```
3. Para limpar manualmente depois de investigar:
   ```
   docker compose -p aps-sistema -f docker-compose.distribuido.yaml down -v
   docker compose -p aps-teste -f docker-compose.teste.yaml down -v
   docker network rm aps-net
   ```

**Problemas comuns:**

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `Docker nao disponivel` | Docker Desktop fechado | Abra o Docker Desktop e tente de novo |
| `port is already allocated` (8080) | Outro programa ou cenário já usa a porta | Pare o outro processo ou rode `docker compose ... down -v` |
| `wait-app` falha depois de vários minutos | Aplicação não ficou saudável | Veja `falha_console_sistema.txt`; procure linhas que começam com `falha ao...` |
| `network aps-net ... already exists` ou containers antigos | Rodada anterior interrompida | Rode os comandos de limpeza manual acima |
| Erro de build com `go get` | Sem internet no build | Verifique a conexão e rode novamente |
| `start_interval` não reconhecido | Docker anterior ao 25 | Atualize o Docker Desktop |

---

## 8. Decisões de projeto e limitações

- **Teto de CPU de 0,80 vCPU** no Cenário B: Postgres 0,40 + 6 réplicas × 0,05 + Nginx 0,05 + Redis 0,05.
- **Resposta `202` no Cenário B:** o campo `total_persistido` significa "aceito para gravação"; a gravação efetiva é assíncrona.
- **Fila volátil:** o Redis roda sem persistência. Itens ainda não consumidos se perdem se o Redis reiniciar.
- **`synchronous_commit=off`:** maior vazão, ao custo de poder perder os últimos milissegundos de commits em uma queda do banco (sem corrupção). Remova a flag em `docker-compose.distribuido.yaml` se for preciso durabilidade estrita.
- **Cenário A usa o mesmo `main.go`:** sem `REDIS_URL` ele grava direto no banco, mas já com a escrita em lote. Os resultados antigos do Cenário A (anteriores a essa mudança) não são diretamente comparáveis.
- As dependências Go do Cenário B são fixadas (`lib/pq@v1.10.9` e `go-redis/v9@v9.7.0`); as do Cenário A não são.