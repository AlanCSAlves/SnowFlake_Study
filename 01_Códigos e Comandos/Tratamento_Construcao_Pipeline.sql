-- Criando um PIPELINE para automatizar o processo de ETL a partir dos dados da Agnie

-- 1) Criar uma tabela chamada PL_GAME_LOGS (no schema RAW).
-- Ela deve ter a mesma estrutura da tabela GAME_LOGS:
-- mesmas colunas e mesmos tipos de dados.
create or replace TABLE AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS (
	RAW_LOG VARIANT
);

-- 2) Escrever um comando COPY INTO que carregue
-- NÃO apenas um arquivo específico,
-- mas QUALQUER arquivo que chegue nessa pasta (stage).

-- 3) Testar o COPY INTO e, ao ver o resultado,
-- anotar quantos arquivos foram carregados.
-- (Neste caso: 45 arquivos)
copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
file_format = (format_name=ff_json_logs);

-- 4) Consultar a tabela PL_GAME_LOGS.
-- Quantas linhas ela possui?
-- Cada arquivo possui 10 registros.
-- A quantidade total de registros está correta?
select * from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

-- 5) Executar o COPY INTO novamente.
-- A cada execução:
-- • se um novo arquivo foi adicionado, ele será carregado
-- • se nenhum arquivo novo existir, nada será carregado

------------------------------------------------------------------
-- TAREFA (TASK)

-- Criar uma tarefa que seja executada a cada 10 minutos.
-- Nome da tarefa: GET_NEW_FILES
-- A tarefa deve ficar no schema RAW.

-- Copiar o comando COPY INTO
-- e colar dentro do corpo da task GET_NEW_FILES.
create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
	schedule='10 minutes'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
	as copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
    from 
        @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
        file_format = (format_name=ff_json_logs);

-- Executar o comando EXECUTE TASK algumas vezes.
-- Novos arquivos são adicionados ao stage a cada 5 minutos,
-- então leve isso em consideração durante os testes.
execute task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;

select * from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

------------------------------------------------------------------
-- VIEW DO PIPELINE

-- 1) Usando a view LOGS como modelo,
-- criar uma nova view chamada PL_LOGS.
-- Essa view deve ler os dados da nova tabela PL_GAME_LOGS.
create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS(
	USER_EVENT,
	IP_ADDRESS,
	DATETIME_ISO8601,
	USER_LOGIN,
	RAW_LOG
) as
select
  raw_log:user_event::text as USER_EVENT
, raw_log:ip_address::text as ip_address
, raw_log:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601
, raw_log:user_login::text as user_login
, *
from PL_GAME_LOGS
where RAW_LOG:ip_address::text is not null;

-- 2) Verificar a nova view para garantir
-- que todas as linhas estão aparecendo corretamente.
SELECT * FROM AGS_GAME_AUDIENCE.RAW.PL_LOGS;

------------------------------------------------------------------
-- ENHANCED PIPELINE

-- 3) Modificar a task LOAD_LOGS_ENHANCED
-- para buscar os registros a partir da versão PIPELINE (PL_LOGS)
create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
	after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
	as MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
    SELECT 
        logs.ip_address
        , logs.user_login as GAMER_NAME
        , logs.user_event as GAME_EVENT_NAME
        , logs.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
    from ags_game_audience.raw.PL_LOGS logs
    JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_NAME = e.game_event_name 
AND r.GAME_EVENT_UTC = e.game_event_utc
WHEN NOT MATCHED THEN
INSERT (
    IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, 
    GAME_EVENT_UTC, CITY, REGION, COUNTRY, 
    GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME
)
VALUES (
    IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, 
    GAME_EVENT_UTC, CITY, REGION, COUNTRY, 
    GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME
);

execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

------------------------------------------------------------------
-- 4) Verificar se o pipeline está funcionando
-- (Execução manual)
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

------------------------------------------------------------------
-- GERENCIAMENTO DE TASKS

-- Ativar uma task é feito com o comando RESUME
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

-- Desativar uma task é feito com o comando SUSPEND
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

-- Quando há tasks dependentes,
-- é obrigatório ativar primeiro as tasks dependentes
-- e depois a task raiz.
-- Neste caso:
-- 1) LOAD_LOGS_ENHANCED
-- 2) GET_NEW_FILES

-- A primeira task da cadeia é chamada de Root Task.
-- Aqui, GET_NEW_FILES é a Root Task.

-- PIPELINE pronto para uso!

------------------------------------------------------------------
-- CHECKLIST DE VALIDAÇÃO

-- Passo 1 - Quantos arquivos existem no bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

-- Passo 2 - Quantidade de linhas na tabela RAW
-- (número de arquivos x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

-- Passo 3 - Quantidade de linhas na view RAW
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

-- Passo 4 - Quantidade de linhas na tabela ENHANCED
-- Pode ser menor, pois nem todos os IPs existem no IPInfo
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

------------------------------------------------------------------
-- Concedendo permissão de gerenciamento de Tasks Serverless ao SYSADMIN

-- Compute Serverless é mais eficiente para tasks pequenas
use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

-- Voltar para a role SYSADMIN
use role sysadmin;

------------------------------------------------------------------
-- EVENT DRIVEN PIPELINE (PIPE + STREAM + CDC)

-- Novas funcionalidades para transformar o pipeline em Event Driven

-- Criando tabela no formato CTAS (Create Table As Select)
create table ags_game_audience.raw.ED_PIPELINE_LOGS as
SELECT 
    METADATA$FILENAME as log_file_name -- nome do arquivo
  , METADATA$FILE_ROW_NUMBER as log_file_row_id -- número da linha no arquivo
  , current_timestamp(0) as load_ltz -- horário local da carga
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(file_format => 'ff_json_logs');

-- Ajustando os tipos de dados após a criação da tabela
create or replace TABLE AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS (
	LOG_FILE_NAME VARCHAR(100),
	LOG_FILE_ROW_ID NUMBER(18,0),
	LOAD_LTZ TIMESTAMP_LTZ(0),
	DATETIME_ISO8601 TIMESTAMP_NTZ(9),
	USER_EVENT VARCHAR(25),
	USER_LOGIN VARCHAR(100),
	IP_ADDRESS VARCHAR(100)
);

-- Recarregar a tabela usando COPY INTO
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
        METADATA$FILENAME as log_file_name,
        METADATA$FILE_ROW_NUMBER as log_file_row_id,
        current_timestamp(0) as load_ltz,
        get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601,
        get($1,'user_event')::text as USER_EVENT,
        get($1,'user_login')::text as USER_LOGIN,
        get($1,'ip_address')::text as IP_ADDRESS
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

------------------------------------------------------------------
-- PIPE (Snowpipe)

-- Ao invés de usar uma task para checar arquivos a cada 10 minutos,
-- criamos um PIPE que recebe notificações automáticas da AWS (SNS)
-- sempre que um novo arquivo é adicionado ao bucket.

CREATE OR REPLACE PIPE PIPE_GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
        METADATA$FILENAME as log_file_name,
        METADATA$FILE_ROW_NUMBER as log_file_row_id,
        current_timestamp(0) as load_ltz,
        get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601,
        get($1,'user_event')::text as USER_EVENT,
        get($1,'user_login')::text as USER_LOGIN,
        get($1,'ip_address')::text as IP_ADDRESS
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

------------------------------------------------------------------
-- STREAM (CDC)

-- Criando um STREAM para rastrear alterações na tabela RAW
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

-- Verificar streams existentes
show streams;

-- Verificar se há dados pendentes no stream
select system$stream_has_data('ed_cdc_stream');

-- Consultar os dados do stream
select * from ags_game_audience.raw.ed_cdc_stream;

-- Verificar status do PIPE
select SYSTEM$PIPE_STATUS('PIPE_GET_NEW_FILES');

------------------------------------------------------------------
-- TASK CDC (Carga Incremental)

-- Task que só executa se houver dados novos no stream
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
    WHEN system$stream_has_data('ed_cdc_stream')
AS
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
    SELECT 
        cdc.ip_address,
        cdc.user_login as GAMER_NAME,
        cdc.user_event as GAME_EVENT_NAME,
        cdc.datetime_iso8601 as GAME_EVENT_UTC,
        city,
        region,
        country,
        timezone as GAMER_LTZ_TIME,
        CONVERT_TIMEZONE('UTC', timezone, cdc.datetime_iso8601) as game_event_ltz,
        DAYNAME(game_event_ltz) as DOW_NAME,
        TOD_NAME
    FROM ags_game_audience.raw.ed_cdc_stream cdc
    JOIN ipinfo_geoloc.demo.location loc
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
    JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME
WHEN NOT MATCHED THEN
INSERT (
    IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME,
    GAME_EVENT_UTC, CITY, REGION, COUNTRY,
    GAMER_LTZ_TIME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME
)
VALUES (
    IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME,
    GAME_EVENT_UTC, CITY, REGION, COUNTRY,
    GAMER_LTZ_TIME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME
);

-- Ativar a task CDC
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;

------------------------------------------------------------------
-- PIPELINE COMPLETO COM PIPE + STREAM + TASK (CDC)

-- Explorando dados

-- ListAgg agrupa login e logout em uma única linha
select GAMER_NAME,
       listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
group by gamer_name;

-- Cálculo de tempo de sessão usando funções analíticas
select GAMER_NAME,
       game_event_ltz as login,
       lead(game_event_ltz) 
            over (partition by GAMER_NAME order by GAME_EVENT_LTZ) as logout,
       coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;
