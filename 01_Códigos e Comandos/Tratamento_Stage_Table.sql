-- Configurando os atributos iniciais do Snowflake
use role SYSADMIN;
create database INTL_DB;
use schema INTL_DB.PUBLIC;

use role SYSADMIN;

-- Criando o Warehouse
create warehouse INTL_WH 
with 
warehouse_size = 'XSMALL' 
warehouse_type = 'STANDARD' 
auto_suspend = 600 -- 600 segundos / 10 minutos
auto_resume = TRUE;

use warehouse INTL_WH;


-- Criando a tabela
create or replace table intl_db.public.INT_STDS_ORG_3166 
(
  iso_country_name varchar(100), 
  country_name_official varchar(200), 
  sovreignty varchar(40), 
  alpha_code_2digit varchar(2), 
  alpha_code_3digit varchar(3), 
  numeric_country_code integer,
  iso_subdivision varchar(15), 
  internet_domain_code varchar(10)
);

-- Criando o File Format e preparando para copiar dados do bucket S3 para a tabela
create or replace file format util_db.public.PIPE_DBLQUOTE_HEADER_CR 
  type = 'CSV' -- Utilizar CSV para qualquer arquivo texto plano
  compression = 'AUTO' 
  field_delimiter = '|' -- Pipe ou barra vertical
  record_delimiter = '\r' -- Retorno de carro (Carriage Return)
  skip_header = 1  -- Ignorar 1 linha de cabeçalho
  field_optionally_enclosed_by = '\042'  -- Aspas duplas
  trim_space = FALSE;

-- Criando o Stage no banco UtilDB
create stage util_db.public.aws_s3_bucket 
url = 's3://uni-cmcw';

-- Listando os arquivos disponíveis no Stage
list @util_db.public.aws_s3_bucket;

-- Testando a leitura do arquivo usando o File Format
select $1, $2, $3
from @util_db.public.aws_s3_bucket/ISO_Countries_UTF8_pipe.csv
(file_format => util_db.public.PIPE_DBLQUOTE_HEADER_CR);

-- Copiando o arquivo do Stage para a tabela
copy into intl_db.public.INT_STDS_ORG_3166
from @util_db.public.aws_s3_bucket
files = ( 'ISO_Countries_UTF8_pipe.csv')
file_format = ( format_name=util_db.public.PIPE_DBLQUOTE_HEADER_CR );

-- Validando a quantidade de registros carregados
select count(*) as found, '249' as expected 
from INTL_DB.PUBLIC.INT_STDS_ORG_3166;

-- Verificando se a tabela existe no schema e contando objetos encontrados
select count(*) as OBJECTS_FOUND
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';

-- Consultando o número de linhas da tabela
select row_count
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';

-- Visualizando uma amostra dos dados carregados
select * 
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 
limit 10;

-- Realizando LEFT JOIN entre a tabela importada e tabelas compartilhadas do Snowflake
select  
     iso_country_name,
     country_name_official,
     alpha_code_2digit,
     r_name as region,
     n_regionkey
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
    on upper(i.iso_country_name) = n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
    on n_regionkey = r_regionkey;

-- Criando uma View com dados enriquecidos
create or replace view intl_db.public.NATIONS_SAMPLE_PLUS_ISO 
(
  iso_country_name,
  country_name_official,
  alpha_code_2digit,
  region
) 
as
select  
     iso_country_name,
     country_name_official,
     alpha_code_2digit,
     r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
    on upper(i.iso_country_name) = n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
    on n_regionkey = r_regionkey;

-- Consultando os dados da View criada
select *
from intl_db.public.NATIONS_SAMPLE_PLUS_ISO;

-- Criando tabelas e copiando arquivos do Stage S3 para preenchimento

-- Criando tabela de moedas
create table intl_db.public.CURRENCIES 
(
  currency_ID integer, 
  currency_char_code varchar(3), 
  currency_symbol varchar(4), 
  currency_digital_code varchar(3), 
  currency_digital_name varchar(30)
)
comment = 'Informações sobre moedas, incluindo códigos alfanuméricos, símbolos e códigos digitais';

-- Criando tabela de relacionamento entre países e moedas
create table intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE 
(
  country_char_code varchar(3), 
  country_numeric_code integer, 
  country_name varchar(100), 
  currency_name varchar(100), 
  currency_char_code varchar(3), 
  currency_numeric_code integer
) 
comment = 'Tabela de relacionamento entre países e suas respectivas moedas';

-- Criando File Format para arquivos CSV separados por vírgula
create or replace file format intl_db.public.CSV_COMMA_LF_HEADER
  type = 'CSV' 
  field_delimiter = ',' 
  record_delimiter = '\n' -- O \n representa o caractere Line Feed
  skip_header = 1;

-- Listando arquivos disponíveis no Stage
list @util_db.public.aws_s3_bucket;

-- Testando leitura do arquivo de moedas
select $1, $2, $3, $4, $5
from @util_db.public.aws_s3_bucket/currencies.csv
(file_format => intl_db.public.CSV_COMMA_LF_HEADER);

-- Testando leitura do arquivo de relacionamento país x moeda
select $1, $2, $3, $4, $5, $6
from @util_db.public.aws_s3_bucket/country_code_to_currency_code.csv
(file_format => intl_db.public.CSV_COMMA_LF_HEADER);

-- Copiando o arquivo de moedas para a tabela
copy into intl_db.public.CURRENCIES
from @util_db.public.aws_s3_bucket
files = ( 'currencies.csv')
file_format = ( format_name=intl_db.public.CSV_COMMA_LF_HEADER);

-- Copiando o arquivo de relacionamento país x moeda para a tabela
copy into intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE
from @util_db.public.aws_s3_bucket
files = ( 'country_code_to_currency_code.csv')
file_format = ( format_name=intl_db.public.CSV_COMMA_LF_HEADER);

-- Criando uma View simplificada com código do país e código da moeda
create or replace view intl_db.public.SIMPLE_CURRENCY
(
  CTY_CODE,
  CUR_CODE
) 
as
select 
    country_char_code,
    currency_char_code
from intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE;

-- Consultando a View simplificada
select * 
from intl_db.public.SIMPLE_CURRENCY;
