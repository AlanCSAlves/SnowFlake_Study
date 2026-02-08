-- Lista os arquivos presentes no stage uni_kishore/kickoff
list @uni_kishore/kickoff;

-- Visualiza o conteúdo bruto do arquivo JSON usando o file format definido
select $1
from @uni_kishore/kickoff
(file_format => ff_json_logs);

-- Copia os dados do JSON para a tabela GAME_LOGS
copy into AGS_GAME_AUDIENCE.RAW.GAME_LOGS
from @uni_kishore/kickoff
file_format = (format_name=ff_json_logs);

-- Você percebeu que não especificamos o nome do arquivo no FROM?
-- Isso acontece porque existe apenas um arquivo dentro da pasta kickoff.

-- ==========================================================
-- Abrindo e acessando os elementos do JSON e salvando em uma VIEW
-- ==========================================================
create or replace view LOGS as
select
    raw_log:agent::text as AGENT,                              -- Agente do evento
    raw_log:user_event::text as USER_EVENT,                   -- Evento do usuário
    raw_log:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601, -- Data/hora do evento
    raw_log:user_login::text as user_login,                   -- Login do usuário
    *
from game_logs;

-- Consulta completa da view
select * from LOGS;

-- Qual fuso horário a conta (ou sessão) está utilizando atualmente?
select current_timestamp();

-- Worksheets também são chamadas de sessões
-- Aqui estamos alterando o fuso horário da sessão
alter session set timezone = 'UTC';
select current_timestamp();

-- Como o horário muda ao alterar o fuso?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

-- Exibe o parâmetro de timezone da conta
show parameters like 'timezone';

-- Visualiza os dados do novo feed atualizado
select $1
from @uni_kishore/updated_feed
(file_format => ff_json_logs);

-- Carrega os novos dados para a tabela GAME_LOGS
copy into AGS_GAME_AUDIENCE.RAW.GAME_LOGS
from @uni_kishore/updated_feed
file_format = (format_name=ff_json_logs);

-- Atualiza a VIEW para incluir o endereço IP
create or replace view LOGS as
select
    raw_log:agent::text as AGENT,
    raw_log:user_event::text as USER_EVENT,
    raw_log:ip_address::text as ip_address,
    raw_log:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601,
    raw_log:user_login::text as user_login,
    *
from game_logs;

-- Procurando registros onde a coluna AGENT está vazia
select * from LOGS where agent is null;

-- Procurando registros onde o IP_ADDRESS não é nulo
select 
    RAW_LOG:ip_address::text as IP_ADDRESS,
    *
from ags_game_audience.raw.LOGS
where RAW_LOG:ip_address::text is not null;

-- Recriando a VIEW apenas com registros que possuem IP
create or replace view LOGS as
select
    raw_log:user_event::text as USER_EVENT,
    raw_log:ip_address::text as ip_address,
    raw_log:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601,
    raw_log:user_login::text as user_login,
    *
from game_logs
where RAW_LOG:ip_address::text is not null;

-- Consulta eventos de um usuário específico
select * from LOGS where user_login = 'princess_prajina';

-- Parse do IP retornando apenas IPv4
select parse_ip('100.41.16.160','inet'):ipv4;

-- Parse completo do IP
select parse_ip('100.41.16.160','inet');

-- Criação do schema ENHANCED
create schema ENHANCED;

-- ==========================================================
-- Buscando o fuso horário a partir do IP usando o Data Share IPINFO
-- ==========================================================
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4
between start_ip_int and end_ip_int;

-- Join entre logs e localização para adicionar dados geográficos
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
between start_ip_int and end_ip_int;

-- ==========================================================
-- Criação da tabela ENHANCED com CTAS
-- ==========================================================
create table ags_game_audience.enhanced.logs_enhanced as (
    select 
        logs.ip_address,
        logs.user_login as GAMER_NAME,
        logs.user_event as GAME_EVENT_NAME,
        logs.datetime_iso8601 as GAME_EVENT_UTC,
        city,
        region,
        country,
        timezone as GAMER_LTZ_NAME,
        convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz,
        dayname(game_event_ltz) as dow_name,
        tod_name
    from AGS_GAME_AUDIENCE.RAW.LOGS logs
    join IPINFO_GEOLOC.demo.location loc
        on IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        and IPINFO_GEOLOC.public.TO_INT(logs.ip_address)
        between start_ip_int and end_ip_int
    join ags_game_audience.raw.time_of_day_lu tod
        on hour(game_event_ltz) = tod.hour
);

-- ==========================================================
-- Observações de contexto
-- ==========================================================
-- Seu role deve ser SYSADMIN
-- O database deve ser AGS_GAME_AUDIENCE
-- O schema deve ser RAW

-- Tabela de lookup para converter hora em período do dia
create table ags_game_audience.raw.time_of_day_lu
(
    hour number,
    tod_name varchar(25)
);

-- Inserção dos 24 períodos do dia
insert into time_of_day_lu values
(6,'Early morning'), (7,'Early morning'), (8,'Early morning'),
(9,'Mid-morning'), (10,'Mid-morning'), (11,'Late morning'),
(12,'Late morning'), (13,'Early afternoon'), (14,'Early afternoon'),
(15,'Mid-afternoon'), (16,'Mid-afternoon'), (17,'Late afternoon'),
(18,'Late afternoon'), (19,'Early evening'), (20,'Early evening'),
(21,'Late evening'), (22,'Late evening'), (23,'Late evening'),
(0,'Late at night'), (1,'Late at night'), (2,'Late at night'),
(3,'Toward morning'), (4,'Toward morning'), (5,'Toward morning');

-- Verifica se a tabela foi carregada corretamente
select * from time_of_day_lu;

-- Limpa a tabela enhanced
truncate table ags_game_audience.enhanced.LOGS_ENHANCED;

-- Recarrega os dados
insert into ags_game_audience.enhanced.LOGS_ENHANCED
select
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    timezone as GAMER_LTZ_NAME,
    convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz,
    dayname(game_event_ltz) as DOW_NAME,
    TOD_NAME
from ags_game_audience.raw.LOGS logs
join ipinfo_geoloc.demo.location loc
    on ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    and ipinfo_geoloc.public.TO_INT(logs.ip_address)
    between start_ip_int and end_ip_int
join ags_game_audience.raw.TIME_OF_DAY_LU tod
    on hour(game_event_ltz) = tod.hour;

-- Clona a tabela como backup
create table ags_game_audience.enhanced.LOGS_ENHANCED_BU
clone ags_game_audience.enhanced.LOGS_ENHANCED;

/* ============================================================
   MERGE NA CAMADA ENHANCED
   Objetivo: inserir apenas eventos novos na tabela analítica
   ============================================================ */

MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (

    /* --------------------------------------------------------
       SELECT de enriquecimento
       Aqui combinamos:
       - Dados brutos do LOGS
       - Geolocalização por IP (IPInfo)
       - Dimensão de horário (TIME_OF_DAY)
       -------------------------------------------------------- */

    SELECT 
        logs.ip_address                         -- IP do jogador
      , logs.user_login as GAMER_NAME           -- Nome do jogador
      , logs.user_event as GAME_EVENT_NAME      -- Evento (login, logout, etc.)
      , logs.datetime_iso8601 as GAME_EVENT_UTC -- Evento em UTC (fonte da verdade)
      , city                                   -- Cidade derivada do IP
      , region                                 -- Região derivada do IP
      , country                                -- País derivado do IP
      , timezone as GAMER_LTZ_NAME              -- Timezone local do jogador

      /* Conversão do horário UTC para o horário local do jogador */
      , CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) 
            as game_event_ltz

      /* Nome do dia da semana no horário local */
      , DAYNAME(game_event_ltz) as DOW_NAME

      /* Classificação do horário (manhã, tarde, noite, etc.) */
      , TOD_NAME

    from ags_game_audience.raw.LOGS logs

    /* --------------------------------------------------------
       JOIN com IPINFO
       Objetivo: transformar IP em localização geográfica
       -------------------------------------------------------- */
    JOIN ipinfo_geoloc.demo.location loc 
      ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
     AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
         BETWEEN start_ip_int AND end_ip_int

    /* --------------------------------------------------------
       JOIN com dimensão de tempo
       Objetivo: classificar o horário do evento
       -------------------------------------------------------- */
    JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
      ON HOUR(game_event_ltz) = tod.hour

) r  -- Resultado final enriquecido que será comparado com a tabela destino

/* ------------------------------------------------------------
   CONDIÇÃO DO MERGE
   Define quando um registro já existe
   ------------------------------------------------------------ */
ON r.GAMER_NAME       = e.GAMER_NAME
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
AND r.GAME_EVENT_UTC  = e.GAME_EVENT_UTC

/* ------------------------------------------------------------
   INSERÇÃO APENAS DE REGISTROS NOVOS
   Evita duplicação de eventos
   ------------------------------------------------------------ */
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

/* ============================================================
   CONTROLE DE PERMISSÕES PARA TASKS
   ============================================================ */

use role accountadmin;

-- Mesmo que o SYSADMIN seja dono da task,
-- ele NÃO consegue executá-la sem esse grant explícito
grant execute task on account to role SYSADMIN;

/* Informação complementar:
   Em Snowflake, permissões de TASK são de nível ACCOUNT,
   não apenas de ownership do objeto.
*/

use role sysadmin;

/* ============================================================
   EXECUÇÃO E MONITORAMENTO DA TASK
   ============================================================ */

-- Agora o SYSADMIN consegue executar a task manualmente
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

/* Informação complementar:
   Executar manualmente é essencial para validar:
   - Lógica do MERGE
   - Enriquecimento de dados
   - Performance da query
*/

-- Lista todas as tasks da conta
show tasks in account;

-- Mostra detalhes da task:
-- SQL, agendamento, status e dependências
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- Executa novamente para observar o histórico de execuções
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- Executar múltiplas vezes ajuda a validar:
-- - Se o MERGE é idempotente
-- - Se não há duplicações
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

/* ============================================================
   RESET CONTROLADO DA TABELA ENHANCED
   ============================================================ */

-- Limpa a tabela para reiniciar os testes de carga
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

/* Informação complementar:
   TRUNCATE é útil em ambientes de estudo,
   mas NÃO deve ser usado em produção sem critério.
*/

/* ============================================================
   VIEW RAW.LOGS
   Camada de abertura do JSON
   ============================================================ */

create or replace view AGS_GAME_AUDIENCE.RAW.LOGS(
	USER_EVENT,
	IP_ADDRESS,
	DATETIME_ISO8601,
	USER_LOGIN,
	RAW_LOG
) as

select
    raw_log:user_event::text            as USER_EVENT
  , raw_log:ip_address::text            as IP_ADDRESS
  , raw_log:datetime_iso8601::TIMESTAMP_NTZ as DATETIME_ISO8601
  , raw_log:user_login::text            as USER_LOGIN
  , *                                   -- Mantém o JSON completo
from game_logs

-- Filtro técnico: remove eventos sem IP válido
where RAW_LOG:ip_address::text is not null;

/* Informação complementar:
   Essa view:
   - Não altera dados
   - Apenas estrutura o JSON
   - Serve como base para múltiplas camadas (ENHANCED, PIPELINE, CDC)
*/
