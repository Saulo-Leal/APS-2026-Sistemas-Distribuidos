@echo off
setlocal enabledelayedexpansion
pushd "%~dp0"

REM ============================================================
REM run_benchmark.bat - Orquestrador da APS
REM
REM Uso:
REM   run_benchmark.bat       Cenario A sempre; Cenario B se o compose existir
REM   run_benchmark.bat A     somente Cenario A, monolito
REM   run_benchmark.bat B     somente Cenario B, distribuido
REM
REM Cenario A = docker-compose.monolito.yml    / .yaml   (obrigatorio)
REM Cenario B = docker-compose.distribuido.yml / .yaml   (opcional)
REM
REM DIRETORIO DE SAIDA - uma unica arvore:
REM   resultados\CenarioA\<timestamp>\
REM   resultados\CenarioB\<timestamp>\
REM Nenhuma outra pasta e criada na raiz do projeto.
REM ============================================================

set "CENARIOS=%~1"
if not defined CENARIOS set "CENARIOS=AB"
REM Modo MOD: mantem containers e logs quando houver falha (use KEEP_ON_FAIL=0 para limpar sempre)
if not defined KEEP_ON_FAIL set "KEEP_ON_FAIL=1"

set "PROJ_SISTEMA=aps-sistema"
set "PROJ_TESTE=aps-teste"
set "TEM_FALHA=0"
set "RESULT_ROOT=resultados"

REM ---------- compose da plataforma de teste ----------
set "COMPOSE_TESTE="
if exist "docker-compose.teste.yml" set "COMPOSE_TESTE=docker-compose.teste.yml"
if not defined COMPOSE_TESTE if exist "docker-compose.teste.yaml" set "COMPOSE_TESTE=docker-compose.teste.yaml"

REM ---------- Cenario A: monolito ----------
set "COMPOSE_A="
if exist "docker-compose.monolito.yml" set "COMPOSE_A=docker-compose.monolito.yml"
if not defined COMPOSE_A if exist "docker-compose.monolito.yaml" set "COMPOSE_A=docker-compose.monolito.yaml"

REM ---------- Cenario B: distribuido ----------
set "COMPOSE_B="
if exist "docker-compose.distribuido.yml" set "COMPOSE_B=docker-compose.distribuido.yml"
if not defined COMPOSE_B if exist "docker-compose.distribuido.yaml" set "COMPOSE_B=docker-compose.distribuido.yaml"

echo ==============================================
echo APS Benchmark
echo Cenario A monolito:     %COMPOSE_A%
if defined COMPOSE_B echo Cenario B distribuido:  %COMPOSE_B%
if not defined COMPOSE_B echo Cenario B distribuido:  nao encontrado, sera ignorado
echo Plataforma de teste:    %COMPOSE_TESTE%
echo Solicitado:              %CENARIOS%
echo Saida:                  %RESULT_ROOT%\
echo ==============================================
echo.

REM ---------- pre-condicoes ----------
docker info >nul 2>&1 <nul
if errorlevel 1 goto :sem_docker
if not defined COMPOSE_TESTE goto :sem_compose_teste
if not exist "script.js" goto :sem_arquivo
if not exist "avaliar_justo.go" goto :sem_arquivo

if exist "go.mod" echo AVISO: go.mod encontrado na raiz. O avaliador usa apenas a biblioteca padrao do Go.

REM ---------- normalizacao: remove a estrutura legada da raiz ----------
if exist "logs" rmdir /s /q "logs"
if exist "results" rmdir /s /q "results"
if exist "resultado_registros.txt" del /q "resultado_registros.txt"

REM ---------- roteamento ----------
if /i "%CENARIOS%"=="A" goto :marca_a
if /i "%CENARIOS%"=="B" goto :marca_b
if /i "%CENARIOS%"=="AB" goto :marca_ab
if /i "%CENARIOS%"=="BA" goto :marca_ab
goto :cenario_invalido

:marca_a
set "RODAR_A=1"
goto :verifica

:marca_b
set "RODAR_B=1"
goto :verifica

:marca_ab
set "RODAR_A=1"
set "RODAR_B=1"

:verifica
if not defined COMPOSE_A goto :sem_compose_a
if not "%RODAR_B%"=="1" goto :rede_setup
if defined COMPOSE_B goto :rede_setup
if /i "%CENARIOS%"=="B" goto :sem_compose_b
set "RODAR_B="
echo AVISO: Cenario B ignorado. docker-compose.distribuido.yml/.yaml nao encontrado.
echo.

:rede_setup
docker network inspect aps-net >nul 2>&1 <nul
if errorlevel 1 goto :cria_rede
echo Rede aps-net ja existe.
goto :executa_a

:cria_rede
docker network create aps-net >nul 2>&1 <nul
echo Rede aps-net criada.

:executa_a
if not "%RODAR_A%"=="1" goto :executa_b
set "CEN=A"
set "COMPOSE_SISTEMA=%COMPOSE_A%"
call :executar
if errorlevel 1 goto :erro

:executa_b
if not "%RODAR_B%"=="1" goto :consolidar
set "CEN=B"
set "COMPOSE_SISTEMA=%COMPOSE_B%"
call :executar
if errorlevel 1 goto :erro

:consolidar
echo.
echo ==============================================
echo RESUMO COMPARATIVO
echo ==============================================
call :mostra_ultimo A
if "%RODAR_B%"=="1" call :mostra_ultimo B
if not "%RODAR_B%"=="1" echo.
if not "%RODAR_B%"=="1" echo Cenario B  nao executado, arquivo nao encontrado

docker network rm aps-net >nul 2>&1 <nul

echo.
echo ==============================================
if "!TEM_FALHA!"=="1" echo Benchmark finalizado com falhas em algum cenario.
if "!TEM_FALHA!"=="0" echo Benchmark finalizado com sucesso.
echo ==============================================
echo Tudo em: %RESULT_ROOT%\
popd
exit /b 0


REM ============================================================
REM executa um cenario completo. Le CEN e COMPOSE_SISTEMA.
REM ============================================================
:executar
echo.
echo ##############################################
echo # CENARIO %CEN%  -  %COMPOSE_SISTEMA%
echo ##############################################

call :limpar

REM O destino nasce AQUI, antes da fase 1.
REM DESTINO usa barra invertida para operacoes do Windows.
REM RUN_DIR usa barra normal, porque quem consome e o Linux
REM dentro do container.
for /f "usebackq delims=" %%I in (`powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"`) do set "STAMP=%%I"
set "DESTINO=%RESULT_ROOT%\Cenario!CEN!\!STAMP!"
mkdir "!DESTINO!" 2>nul
set "RUN_DIR=%RESULT_ROOT%/Cenario!CEN!/!STAMP!"
echo   Saida da rodada: !DESTINO!

echo.
echo [1/5] Subindo o sistema sob teste do Cenario %CEN% ...
docker compose -p %PROJ_SISTEMA% -f "%COMPOSE_SISTEMA%" up -d --build <nul
if errorlevel 1 goto :falha_cenario

echo.
echo [2/5] Fase 1: disparando a carga em modo destacado ...
docker compose -p %PROJ_TESTE% -f "%COMPOSE_TESTE%" up -d stress-test <nul
if errorlevel 1 goto :falha_cenario

call :aguardar wait-app %PROJ_TESTE% "%COMPOSE_TESTE%" 180
if not "!SAIDA!"=="0" goto :falha_cenario

call :aguardar stress-test %PROJ_TESTE% "%COMPOSE_TESTE%" 900
set "K6_EXIT=!SAIDA!"
if not "!K6_EXIT!"=="0" goto :falha_cenario

echo.
echo ---- INICIO DO LOG DO GERADOR DE CARGA ----
docker compose -p %PROJ_TESTE% -f "%COMPOSE_TESTE%" logs --no-color stress-test <nul
echo ---- FIM DO LOG DO GERADOR DE CARGA ----

echo.
echo Verificando artefatos da fase 1 ...
if not exist "!DESTINO!\report.json" goto :falha_cenario
echo   report.json OK
if not exist "!DESTINO!\stream.json" echo   AVISO: stream.json ausente.
if exist "!DESTINO!\stream.json" echo   stream.json OK

echo.
echo [3/5] Fase 2: auditando resultados ...
docker compose -p %PROJ_TESTE% -f "%COMPOSE_TESTE%" up -d --no-deps avaliador-notas <nul
if errorlevel 1 goto :falha_cenario

call :aguardar avaliador-notas %PROJ_TESTE% "%COMPOSE_TESTE%" 900
set "BENCH_EXIT=!SAIDA!"

echo.
echo ---- INICIO DO LOG DO AVALIADOR ----
docker compose -p %PROJ_TESTE% -f "%COMPOSE_TESTE%" logs --no-color avaliador-notas <nul
echo ---- FIM DO LOG DO AVALIADOR ----

if not "!BENCH_EXIT!"=="0" set "TEM_FALHA=1"

echo.
echo [4/5] Complementando o arquivo da rodada ...

docker compose -p %PROJ_TESTE% -f "%COMPOSE_TESTE%" logs --no-color > "!DESTINO!\console_teste.txt" 2>&1 <nul
docker compose -p %PROJ_SISTEMA% -f "%COMPOSE_SISTEMA%" logs --no-color > "!DESTINO!\console_sistema.txt" 2>&1 <nul

> "!DESTINO!\resumo.txt" echo Cenario %CEN%
>> "!DESTINO!\resumo.txt" echo Compose: %COMPOSE_SISTEMA%
>> "!DESTINO!\resumo.txt" echo Execucao: !STAMP!
>> "!DESTINO!\resumo.txt" echo.

if not exist "!DESTINO!\resultado_registros.txt" goto :resumo_sem_dados
for /f "usebackq tokens=1-8 delims=;" %%A in ("!DESTINO!\resultado_registros.txt") do (
    >> "!DESTINO!\resumo.txt" echo Enviados:    %%A
    >> "!DESTINO!\resumo.txt" echo Rejeitados:  %%B
    >> "!DESTINO!\resumo.txt" echo Gravados:    %%C
    >> "!DESTINO!\resumo.txt" echo Taxa persistencia: %%D
    >> "!DESTINO!\resumo.txt" echo Divergentes: %%E
    >> "!DESTINO!\resumo.txt" echo Fantasmas:   %%F
    >> "!DESTINO!\resumo.txt" echo Perdidas:    %%G
    >> "!DESTINO!\resumo.txt" echo Duplicatas:  %%H
)
goto :resumo_pronto

:resumo_sem_dados
>> "!DESTINO!\resumo.txt" echo Auditoria nao gerou resultado_registros.txt

:resumo_pronto
echo.
echo [5/5] Derrubando os compose do Cenario %CEN% ...
call :limpar
exit /b 0

:falha_cenario
echo   ERRO no Cenario %CEN%. Encerrando este cenario.
set "TEM_FALHA=1"
REM Salva os logs de TODOS os containers (inclusive os que morreram) antes de qualquer limpeza
if defined DESTINO (
  docker compose -p %PROJ_SISTEMA% -f "%COMPOSE_SISTEMA%" ps -a > "!DESTINO!\falha_ps.txt" 2>&1 <nul
  docker compose -p %PROJ_SISTEMA% -f "%COMPOSE_SISTEMA%" logs --no-color > "!DESTINO!\falha_console_sistema.txt" 2>&1 <nul
  docker compose -p %PROJ_TESTE% -f "%COMPOSE_TESTE%" logs --no-color > "!DESTINO!\falha_console_teste.txt" 2>&1 <nul
  echo   Logs da falha salvos em !DESTINO!
)
if /i "%KEEP_ON_FAIL%"=="1" (
  echo   KEEP_ON_FAIL=1: containers MANTIDOS para inspecao. Rode "docker compose -p %PROJ_SISTEMA% -f %COMPOSE_SISTEMA% logs" para ver.
  echo   Para limpar depois: docker compose -p %PROJ_SISTEMA% -f %COMPOSE_SISTEMA% down -v ^& docker compose -p %PROJ_TESTE% -f %COMPOSE_TESTE% down -v
  exit /b 1
)
call :limpar
exit /b 1


REM ============================================================
REM aguarda um container terminar por polling de docker inspect
REM %1 servico  %2 projeto  %3 compose  %4 timeout em segundos
REM ============================================================
:aguardar
set "SAIDA=1"
set "CID="
for /f "usebackq delims=" %%C in (`docker compose -p %~2 -f "%~3" ps -a -q %~1 2^>nul`) do set "CID=%%C"
if not defined CID goto :aguardar_sem_container

set /a "ESPERA=0"
:aguardar_loop
set "RODANDO=desconhecido"
for /f "usebackq delims=" %%S in (`docker inspect -f "{{.State.Running}}" !CID! 2^>nul`) do set "RODANDO=%%S"
if /i not "!RODANDO!"=="true" goto :aguardar_fim
set /a "ESPERA+=2"
if !ESPERA! GEQ %~4 goto :aguardar_timeout
ping -n 3 127.0.0.1 >nul 2>&1
goto :aguardar_loop

:aguardar_timeout
echo   AVISO: timeout de %~4s aguardando o servico %~1. Encerrando o container.
docker kill !CID! >nul 2>&1 <nul

:aguardar_fim
for /f "usebackq delims=" %%E in (`docker inspect -f "{{.State.ExitCode}}" !CID! 2^>nul`) do set "SAIDA=%%E"
exit /b 0

:aguardar_sem_container
echo   AVISO: container do servico %~1 nao encontrado.
exit /b 1


REM ============================================================
REM derruba containers e volumes dos dois projetos
REM ============================================================
:limpar
docker compose -p %PROJ_TESTE% -f "%COMPOSE_TESTE%" down -v --remove-orphans >nul 2>&1 <nul
if defined COMPOSE_SISTEMA docker compose -p %PROJ_SISTEMA% -f "%COMPOSE_SISTEMA%" down -v --remove-orphans >nul 2>&1 <nul
exit /b 0


REM ============================================================
REM imprime o ultimo resultado de um cenario
REM ============================================================
:mostra_ultimo
set "ULT="
for /f "delims=" %%D in ('dir /b /ad /o-n "%RESULT_ROOT%\Cenario%~1" 2^>nul') do if not defined ULT set "ULT=%%D"
if not defined ULT goto :sem_historico
set "ARR=%RESULT_ROOT%\Cenario%~1\!ULT!\resultado_registros.txt"
echo.
echo Cenario %~1  em  %RESULT_ROOT%\Cenario%~1\!ULT!
if not exist "!ARR!" goto :sem_historico
for /f "usebackq tokens=1-8 delims=;" %%A in ("!ARR!") do echo   Enviados: %%A   Gravados: %%C   Taxa: %%D
goto :eof

:sem_historico
echo.
echo Cenario %~1  sem resultado arquivado
goto :eof


REM ============================================================
REM mensagens de erro
REM ============================================================
:sem_docker
echo ERRO: Docker nao disponivel. Abra o Docker Desktop e tente novamente.
goto :erro

:cenario_invalido
echo ERRO: cenario invalido "%CENARIOS%". Use A, B ou AB.
goto :erro

:sem_compose_teste
echo ERRO: compose de teste nao encontrado. Procurei docker-compose.teste.yml e .yaml
goto :erro

:sem_compose_a
echo ERRO: compose do Cenario A nao encontrado. Procurei docker-compose.monolito.yml e .yaml
goto :erro

:sem_compose_b
echo ERRO: Cenario B solicitado explicitamente, mas docker-compose.distribuido.yml/.yaml nao existe.
goto :erro

:sem_arquivo
echo ERRO: script.js ou avaliar_justo.go nao encontrados.
goto :erro

:erro
echo.
if /i "%KEEP_ON_FAIL%"=="1" if "!TEM_FALHA!"=="1" (
  echo Falha: containers mantidos para inspecao ^(KEEP_ON_FAIL=1^).
  popd
  exit /b 1
)
echo Encerrando e limpando ...
call :limpar
docker network rm aps-net >nul 2>&1 <nul
popd
exit /b 1