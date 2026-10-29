#!/usr/bin/env bash
# ============================================================
# run_benchmark.sh - Orquestrador da APS (Linux)
#
# Uso:
#   ./run_benchmark.sh       Cenario A sempre; Cenario B se o compose existir
#   ./run_benchmark.sh A     somente Cenario A, monolito
#   ./run_benchmark.sh B     somente Cenario B, distribuido
#
# Cenario A = docker-compose.monolito.yml    / .yaml   (obrigatorio)
# Cenario B = docker-compose.distribuido.yml / .yaml   (opcional)
#
# DIRETORIO DE SAIDA - uma unica arvore:
#   resultados/CenarioA/<timestamp>/
#   resultados/CenarioB/<timestamp>/
# Cada rodada produz ali mesmo report.json, stream.json,
# resultado_registros.txt, resumo.txt e os dois logs de console.
# Nenhuma outra pasta e criada na raiz do projeto.
# ============================================================
set -uo pipefail

cd "$(dirname "$0")" || exit 1

CENARIOS="${1:-AB}"

PROJ_SISTEMA="aps-sistema"
PROJ_TESTE="aps-teste"
TEM_FALHA=0
COMPOSE_SISTEMA=""
SAIDA=1
RESULT_ROOT="resultados"

# ---------- resolucao dos composes: nome exato -> .yml -> .yaml ----------
resolver() {
    local base="$1"
    if [ -f "$base" ];        then printf '%s' "$base";        return 0; fi
    if [ -f "${base}.yml" ];  then printf '%s' "${base}.yml";  return 0; fi
    if [ -f "${base}.yaml" ]; then printf '%s' "${base}.yaml"; return 0; fi
    return 1
}

COMPOSE_TESTE="$(resolver docker-compose.teste     || true)"
COMPOSE_A="$(resolver docker-compose.monolito      || true)"
COMPOSE_B="$(resolver docker-compose.distribuido   || true)"

echo "=============================================="
echo "APS Benchmark"
echo "Cenario A monolito:     ${COMPOSE_A:-nao encontrado}"
if [ -n "$COMPOSE_B" ]; then
    echo "Cenario B distribuido:  $COMPOSE_B"
else
    echo "Cenario B distribuido:  nao encontrado, sera ignorado"
fi
echo "Plataforma de teste:    ${COMPOSE_TESTE:-nao encontrado}"
echo "Solicitado:             $CENARIOS"
echo "Saida:                  $RESULT_ROOT/"
echo "=============================================="
echo

# ---------- pre-condicoes ----------
if ! docker info >/dev/null 2>&1 </dev/null; then
    echo "ERRO: Docker nao disponivel. Inicie o daemon e tente novamente."
    exit 1
fi
if [ -z "$COMPOSE_TESTE" ]; then
    echo "ERRO: compose de teste nao encontrado. Procurei docker-compose.teste.yml e .yaml"
    exit 1
fi
if [ ! -f script.js ] || [ ! -f avaliar_justo.go ]; then
    echo "ERRO: script.js ou avaliar_justo.go nao encontrados."
    exit 1
fi
if [ -f go.mod ]; then
    echo "AVISO: go.mod encontrado na raiz. O avaliador usa apenas a biblioteca padrao do Go."
fi

# ---------- normalizacao: remove a estrutura legada da raiz ----------
# Versoes anteriores geravam logs/, results/ e resultado_registros.txt
# na raiz. Tudo isso e saida gerada: passa a existir apenas dentro de
# resultados/. As pastas legadas sao removidas para nao ficar duplicado.
for legado in logs results; do
    if [ -d "$legado" ]; then
        rm -rf "$legado"
        echo "Removida pasta legada: $legado/"
    fi
done
if [ -f resultado_registros.txt ]; then
    rm -f resultado_registros.txt
    echo "Removido resultado_registros.txt legado da raiz."
fi

# ---------- roteamento dos cenarios ----------
RODAR_A=0
RODAR_B=0
CEN_UP="$(printf '%s' "$CENARIOS" | tr '[:lower:]' '[:upper:]')"

case "$CEN_UP" in
    A)     RODAR_A=1 ;;
    B)     RODAR_B=1 ;;
    AB|BA) RODAR_A=1; RODAR_B=1 ;;
    *)
        echo "ERRO: cenario invalido \"$CENARIOS\". Use A, B ou AB."
        exit 1
        ;;
esac

if [ -z "$COMPOSE_A" ]; then
    echo "ERRO: compose do Cenario A nao encontrado. Procurei docker-compose.monolito.yml e .yaml"
    exit 1
fi

if [ "$RODAR_B" -eq 1 ] && [ -z "$COMPOSE_B" ]; then
    if [ "$CEN_UP" = "B" ]; then
        echo "ERRO: Cenario B solicitado explicitamente, mas docker-compose.distribuido.yml/.yaml nao existe."
        exit 1
    fi
    RODAR_B=0
    echo "AVISO: Cenario B ignorado. docker-compose.distribuido.yml/.yaml nao encontrado."
    echo
fi

# ---------- rede compartilhada ----------
if ! docker network inspect aps-net >/dev/null 2>&1 </dev/null; then
    docker network create aps-net >/dev/null 2>&1 </dev/null
    echo "Rede aps-net criada."
else
    echo "Rede aps-net ja existe."
fi

# ============================================================
# aguarda um container terminar por polling de docker inspect
# $1 servico   $2 projeto   $3 compose   $4 timeout em segundos
# ============================================================
aguardar() {
    local servico="$1" projeto="$2" compose="$3" limite="$4"
    SAIDA=1

    local cid
    cid="$(docker compose -p "$projeto" -f "$compose" ps -a -q "$servico" 2>/dev/null </dev/null | head -n1)"
    if [ -z "$cid" ]; then
        echo "  AVISO: container do servico $servico nao encontrado."
        return 1
    fi

    local espera=0 rodando
    while :; do
        rodando="$(docker inspect -f '{{.State.Running}}' "$cid" 2>/dev/null | tr -d '[:space:]')"
        if [ "$rodando" != "true" ]; then
            break
        fi
        espera=$((espera + 2))
        if [ "$espera" -ge "$limite" ]; then
            echo "  AVISO: timeout de ${limite}s aguardando o servico $servico. Encerrando o container."
            docker kill "$cid" >/dev/null 2>&1 </dev/null
            break
        fi
        sleep 2
    done

    SAIDA="$(docker inspect -f '{{.State.ExitCode}}' "$cid" 2>/dev/null | tr -d '[:space:]')"
    [ -z "$SAIDA" ] && SAIDA=1
    return 0
}

# ============================================================
# derruba containers e volumes dos dois projetos
# ============================================================
limpar() {
    docker compose -p "$PROJ_TESTE" -f "$COMPOSE_TESTE" down -v --remove-orphans \
        >/dev/null 2>&1 </dev/null
    if [ -n "${COMPOSE_SISTEMA:-}" ]; then
        docker compose -p "$PROJ_SISTEMA" -f "$COMPOSE_SISTEMA" down -v --remove-orphans \
            >/dev/null 2>&1 </dev/null
    fi
}

trap 'echo; echo "Interrompido. Limpando ..."; limpar; docker network rm aps-net >/dev/null 2>&1; exit 130' INT TERM

# ============================================================
# executa um cenario completo
# ============================================================
executar_cenario() {
    local CEN="$1"

    echo
    echo "##############################################"
    echo "# CENARIO $CEN  -  $COMPOSE_SISTEMA"
    echo "##############################################"

    limpar

    # O destino nasce AQUI, antes da fase 1. O k6 e o avaliador
    # recebem esse caminho por RUN_DIR e escrevem direto nele.
    local STAMP DESTINO
    STAMP="$(date +%Y%m%d_%H%M%S)"
    DESTINO="${RESULT_ROOT}/Cenario${CEN}/${STAMP}"
    mkdir -p "$DESTINO"
    export RUN_DIR="$DESTINO"
    echo "  Saida da rodada: $DESTINO"

    echo
    echo "[1/5] Subindo o sistema sob teste do Cenario $CEN ..."
    if ! docker compose -p "$PROJ_SISTEMA" -f "$COMPOSE_SISTEMA" up -d --build </dev/null; then
        echo "  ERRO ao subir o sistema sob teste."
        return 1
    fi

    echo
    echo "[2/5] Fase 1: disparando a carga em modo destacado ..."
    if ! docker compose -p "$PROJ_TESTE" -f "$COMPOSE_TESTE" up -d stress-test </dev/null; then
        echo "  ERRO ao disparar o gerador de carga."
        return 1
    fi

    if ! aguardar wait-app "$PROJ_TESTE" "$COMPOSE_TESTE" 180; then
        return 1
    fi
    if [ "$SAIDA" != "0" ]; then
        echo "  ERRO: wait-app terminou com codigo $SAIDA."
        return 1
    fi

    if ! aguardar stress-test "$PROJ_TESTE" "$COMPOSE_TESTE" 900; then
        return 1
    fi
    local K6_EXIT="$SAIDA"
    if [ "$K6_EXIT" != "0" ]; then
        echo "  ERRO: o gerador de carga falhou com codigo $K6_EXIT."
        return 1
    fi

    echo
    echo "---- INICIO DO LOG DO GERADOR DE CARGA ----"
    docker compose -p "$PROJ_TESTE" -f "$COMPOSE_TESTE" logs --no-color stress-test </dev/null 2>&1
    echo "---- FIM DO LOG DO GERADOR DE CARGA ----"

    echo
    echo "Verificando artefatos da fase 1 ..."
    if [ ! -f "$DESTINO/report.json" ]; then
        echo "  ERRO: report.json nao foi gerado. A fase 2 nao sera iniciada."
        return 1
    fi
    echo "  report.json OK"
    if [ ! -f "$DESTINO/stream.json" ]; then
        echo "  AVISO: stream.json ausente."
    else
        echo "  stream.json OK"
    fi

    echo
    echo "[3/5] Fase 2: auditando resultados ..."
    if ! docker compose -p "$PROJ_TESTE" -f "$COMPOSE_TESTE" up -d --no-deps avaliador-notas </dev/null; then
        echo "  ERRO ao disparar o avaliador."
        return 1
    fi

    if ! aguardar avaliador-notas "$PROJ_TESTE" "$COMPOSE_TESTE" 900; then
        return 1
    fi
    local BENCH_EXIT="$SAIDA"

    echo
    echo "---- INICIO DO LOG DO AVALIADOR ----"
    docker compose -p "$PROJ_TESTE" -f "$COMPOSE_TESTE" logs --no-color avaliador-notas </dev/null 2>&1
    echo "---- FIM DO LOG DO AVALIADOR ----"

    if [ "$BENCH_EXIT" != "0" ]; then
        TEM_FALHA=1
    fi

    echo
    echo "[4/5] Complementando o arquivo da rodada ..."

    docker compose -p "$PROJ_TESTE"   -f "$COMPOSE_TESTE"   logs --no-color \
        > "$DESTINO/console_teste.txt" 2>&1 </dev/null
    docker compose -p "$PROJ_SISTEMA" -f "$COMPOSE_SISTEMA" logs --no-color \
        > "$DESTINO/console_sistema.txt" 2>&1 </dev/null

    local ARR="$DESTINO/resultado_registros.txt"
    {
        echo "Cenario $CEN"
        echo "Compose: $COMPOSE_SISTEMA"
        echo "Execucao: $STAMP"
        echo
        if [ -f "$ARR" ]; then
            local ENVIADOS="-" REJEITADOS="-" GRAVADOS="-" TAXA="-"
            local DIVERGENTES="-" FANTASMAS="-" PERDIDAS="-" DUPLICATAS="-"
            IFS=';' read -r ENVIADOS REJEITADOS GRAVADOS TAXA \
                          DIVERGENTES FANTASMAS PERDIDAS DUPLICATAS < "$ARR"
            echo "Enviados:    $ENVIADOS"
            echo "Rejeitados:  $REJEITADOS"
            echo "Gravados:    $GRAVADOS"
            echo "Taxa persistencia: $TAXA"
            echo "Divergentes: $DIVERGENTES"
            echo "Fantasmas:   $FANTASMAS"
            echo "Perdidas:    $PERDIDAS"
            echo "Duplicatas:  $DUPLICATAS"
        else
            echo "Auditoria nao gerou resultado_registros.txt"
        fi
    } > "$DESTINO/resumo.txt"

    echo
    echo "[5/5] Derrubando os compose do Cenario $CEN ..."
    limpar
    return 0
}

# ============================================================
# imprime o ultimo resultado de um cenario
# ============================================================
mostra_ultimo() {
    local cen="$1"
    local dir="${RESULT_ROOT}/Cenario${cen}"

    echo
    if [ ! -d "$dir" ]; then
        echo "Cenario ${cen}  sem resultado arquivado"
        return 0
    fi

    local ult
    ult="$(ls -1 "$dir" 2>/dev/null | sort -r | head -n1)"
    if [ -z "$ult" ]; then
        echo "Cenario ${cen}  sem resultado arquivado"
        return 0
    fi

    echo "Cenario ${cen}  em  ${dir}/${ult}"

    local arr="${dir}/${ult}/resultado_registros.txt"
    if [ ! -f "$arr" ]; then
        echo "  sem resultado_registros.txt"
        return 0
    fi

    local ENVIADOS="-" REJEITADOS="-" GRAVADOS="-" TAXA="-"
    local DIVERGENTES="-" FANTASMAS="-" PERDIDAS="-" DUPLICATAS="-"
    IFS=';' read -r ENVIADOS REJEITADOS GRAVADOS TAXA \
                  DIVERGENTES FANTASMAS PERDIDAS DUPLICATAS < "$arr"
    echo "  Enviados: $ENVIADOS   Gravados: $GRAVADOS   Taxa: $TAXA"
}

# ============================================================
# execucao dos cenarios
# ============================================================
if [ "$RODAR_A" -eq 1 ]; then
    COMPOSE_SISTEMA="$COMPOSE_A"
    if ! executar_cenario A; then
        echo
        echo "Encerrando e limpando ..."
        limpar
        docker network rm aps-net >/dev/null 2>&1 </dev/null
        exit 1
    fi
fi

if [ "$RODAR_B" -eq 1 ]; then
    COMPOSE_SISTEMA="$COMPOSE_B"
    if ! executar_cenario B; then
        echo
        echo "Encerrando e limpando ..."
        limpar
        docker network rm aps-net >/dev/null 2>&1 </dev/null
        exit 1
    fi
fi

echo
echo "=============================================="
echo "RESUMO COMPARATIVO"
echo "=============================================="
mostra_ultimo A
if [ "$RODAR_B" -eq 1 ]; then
    mostra_ultimo B
else
    echo
    echo "Cenario B  nao executado, arquivo nao encontrado"
fi

docker network rm aps-net >/dev/null 2>&1 </dev/null

echo
echo "=============================================="
if [ "$TEM_FALHA" -eq 1 ]; then
    echo "Benchmark finalizado com falhas em algum cenario."
else
    echo "Benchmark finalizado com sucesso."
fi
echo "=============================================="
echo "Tudo em: $RESULT_ROOT/"
exit 0