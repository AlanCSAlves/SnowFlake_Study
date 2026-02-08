-- Cria (ou recria) a tabela tweet_ingest
-- A coluna raw_status do tipo VARIANT armazenará o JSON bruto de cada tweet
create or replace table social_media_floodgates.public.tweet_ingest(
    raw_status variant
);

-- Criação de um FILE FORMAT para leitura de arquivos JSON
-- Esse formato define como o Snowflake deve interpretar o JSON durante o load
create or replace file format social_media_floodgates.public.json_file_format
    type = 'JSON'                 -- Define que o arquivo é do tipo JSON
    compression = 'AUTO'          -- Detecta automaticamente se o arquivo está compactado
    enable_octal = FALSE          -- Não permite valores octais
    allow_duplicate = FALSE       -- Não permite chaves duplicadas no JSON
    strip_outer_array = TRUE      -- Remove o array externo (útil quando o JSON é uma lista)
    strip_null_values = FALSE     -- Mantém valores nulos
    ignore_utf8_errors = FALSE;   -- Não ignora erros de codificação UTF-8

-- Visualiza o conteúdo do arquivo JSON no stage
-- $1 representa a primeira coluna lida do arquivo (o JSON inteiro)
select $1
from @util_db.public.my_internal_stage/nutrition_tweets.json
(file_format => social_media_floodgates.public.json_file_format);

-- Copia os dados do arquivo JSON para a tabela tweet_ingest
-- Cada registro do JSON será carregado na coluna raw_status
copy into social_media_floodgates.public.tweet_ingest
from @util_db.public.my_internal_stage
files = ('nutrition_tweets.json')
file_format = (format_name = social_media_floodgates.public.json_file_format);

-- Consulta simples para verificar se os dados foram carregados
-- Espera-se visualizar aproximadamente 9 registros (tweets)
select raw_status
from tweet_ingest;

-- Acessa apenas o objeto "entities" dentro do JSON de cada tweet
select raw_status:entities
from tweet_ingest;

-- Acessa especificamente o array de hashtags dentro de entities
select raw_status:entities:hashtags
from tweet_ingest;

-- Retorna apenas o PRIMEIRO hashtag de cada tweet
-- O índice [0] acessa o primeiro elemento do array
select raw_status:entities:hashtags[0].text
from tweet_ingest;

-- Retorna apenas tweets que possuem pelo menos um hashtag
-- O WHERE remove registros sem hashtags
select raw_status:entities:hashtags[0].text
from tweet_ingest
where raw_status:entities:hashtags[0].text is not null;

-- Converte o campo created_at para DATE
-- Em seguida ordena os tweets pela data de criação
select raw_status:created_at::date
from tweet_ingest
order by raw_status:created_at::date;

-- Utiliza FLATTEN para "explodir" o array de URLs
-- Cada URL vira uma linha
select value
from tweet_ingest,
lateral flatten(
    input => raw_status:entities:urls
);

-- Outra forma equivalente de usar o FLATTEN via TABLE()
select value
from tweet_ingest,
table(flatten(raw_status:entities:urls));

-- FLATTEN dos hashtags
-- Retorna apenas o texto do hashtag, convertendo para VARCHAR
select value:text::varchar as hashtag_used
from tweet_ingest,
lateral flatten(
    input => raw_status:entities:hashtags
);

-- Retorna informações completas para relacionar hashtags ao tweet original
-- Inclui: nome do usuário, ID do tweet e o hashtag utilizado
select 
    raw_status:user:name::text as user_name,
    raw_status:id as tweet_id,
    value:text::varchar as hashtag_used
from tweet_ingest,
lateral flatten(
    input => raw_status:entities:hashtags
);

-- Cria uma VIEW normalizada de URLs
-- Cada URL fica em uma linha, facilitando análises e joins
create or replace view social_media_floodgates.public.urls_normalized as
select 
    raw_status:user:name::text as user_name,
    raw_status:id as tweet_id,
    value:display_url::text as url_used
from tweet_ingest,
lateral flatten(
    input => raw_status:entities:urls
);

-- Cria uma VIEW normalizada de hashtags
-- Cada hashtag fica em uma linha associada ao tweet e ao usuário
create or replace view social_media_floodgates.public.hashtags_normalized as
select 
    raw_status:user:name::text as user_name,
    raw_status:id as tweet_id,
    value:text::varchar as hashtag_used
from tweet_ingest,
lateral flatten(
    input => raw_status:entities:hashtags
);

-- Verifica se a VIEW hashtags_normalized existe no schema
select count(*) 
from social_media_floodgates.information_schema.views 
where table_name = 'HASHTAGS_NORMALIZED';

-- Consulta final na VIEW normalizada de hashtags
select *
from social_media_floodgates.public.hashtags_normalized;
