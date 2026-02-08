-- ============================
-- DATA TYPES NO SNOWFLAKE
-- ============================
-- Cria uma tabela demonstrando vários tipos de dados suportados pelo Snowflake
create or replace table util_db.public.my_data_types
(
  my_number number,              -- Tipo numérico genérico
  my_text varchar(10),            -- Texto com limite de 10 caracteres
  my_bool boolean,                -- Valor booleano (TRUE/FALSE)
  my_float float,                 -- Número de ponto flutuante
  my_date date,                   -- Data (YYYY-MM-DD)
  my_timestamp timestamp_tz,      -- Timestamp com fuso horário
  my_variant variant,             -- Tipo semi-estruturado (JSON, XML, etc.)
  my_array array,                 -- Array (listas)
  my_object object,               -- Estrutura chave/valor
  my_geography geography,         -- Dados geográficos (mapas, coordenadas)
  my_geometry geometry,           -- Dados geométricos
  my_vector vector(int,16)        -- Vetor com 16 posições (ex: ML / AI)
);

-- ============================
-- CRIAÇÃO DO DATABASE
-- ============================
-- Cria o banco de dados principal do projeto Athleisure
create database ZENAS_ATHLEISURE_DB;

-- ============================
-- TRABALHANDO COM STAGES
-- ============================
-- Lista os arquivos disponíveis no stage product_metadata
list @product_metadata;

-- Visualiza o conteúdo bruto do arquivo de sugestões de coordenação
select $1
from @product_metadata/product_coordination_suggestions.txt; 

-- ============================
-- FILE FORMATS PERSONALIZADOS
-- ============================

-- File format onde apenas o delimitador de registros é ";"
create or replace file format zmd_file_format_1
    RECORD_DELIMITER = ';'
    TRIM_SPACE = TRUE;

-- File format com delimitador de campos "|" e registros ";"
create or replace file format zmd_file_format_2
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = ';'
    TRIM_SPACE = TRUE;

-- File format com delimitador de registros "^" e campos "="
create or replace file format zmd_file_format_3
    RECORD_DELIMITER = '^'
    FIELD_DELIMITER = '='
    TRIM_SPACE = TRUE;

-- ============================
-- VIEWS BASEADAS EM METADADOS
-- ============================

-- View com os tamanhos disponíveis dos sweatsuits
-- Remove caracteres de quebra de linha e ignora registros vazios
create or replace view zenas_athleisure_db.products.sweatsuit_sizes as 
select 
    REPLACE($1, chr(13)||chr(10)) as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1)
where sizes_available <> '';

---------------------------------------------------------------------------
-- View com a linha de produtos de sweatband (headband + wristband)
create or replace view zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE as
select 
    REPLACE($1, chr(13)||chr(10)) as product_code, 
    REPLACE($2, chr(13)||chr(10)) as headband_description, 
    REPLACE($3, chr(13)||chr(10)) as wristband_description
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2)
where product_code <> '';

---------------------------------------------------------------------------
-- View com informações de coordenação entre sweatsuit e sweatband
create or replace view zenas_athleisure_db.products.SWEATBAND_COORDINATION as
select 
    REPLACE($1, chr(13)||chr(10)) as product_code,
    REPLACE($2, chr(13)||chr(10)) as has_matching_sweatsuit
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

---------------------------------------------------------------------------

-- ============================
-- STAGE DE ARQUIVOS NÃO ESTRUTURADOS
-- ============================

-- Lista os arquivos do stage sweatsuits
list @sweatsuits;

-- Acessa diretamente um arquivo PNG
select $1
from @sweatsuits/purple_sweatsuit.png; 

-- Consulta metadados do arquivo (nome e linha)
select 
    metadata$filename, 
    metadata$file_row_number
from @sweatsuits/purple_sweatsuit.png;

-- Conta quantas linhas (registros) existem por arquivo no stage
select 
    metadata$filename, 
    count(*)
from @sweatsuits
group by all;

-- ============================
-- DIRECTORY TABLE
-- ============================
-- Permite consultar arquivos não estruturados diretamente
select * 
from directory(@sweatsuits);

-- ============================
-- TRANSFORMAÇÃO DE NOMES DE ARQUIVO
-- ============================

-- Etapas separadas para transformar o nome do arquivo em nome de produto
select 
    REPLACE(relative_path, '_', ' ') as no_underscores_filename, 
    REPLACE(no_underscores_filename, '.png') as just_words_filename, 
    INITCAP(just_words_filename) as product_name
from directory(@sweatsuits);

-- Mesma transformação aplicada em uma única expressão
select 
   INITCAP(REPLACE(REPLACE(relative_path, '_', ' '),'.png')) as product_name
from directory(@sweatsuits);

-- ============================
-- TABELA INTERNA DE SWEATSUITS
-- ============================

-- Cria uma tabela com informações dos produtos
create or replace table zenas_athleisure_db.products.sweatsuits (
    color_or_style varchar(25),   -- Cor ou estilo do produto
    file_name varchar(50),        -- Nome do arquivo da imagem
    price number(5,2)              -- Preço
);

-- Insere os dados dos produtos
insert into zenas_athleisure_db.products.sweatsuits 
(color_or_style, file_name, price)
values
 ('Burgundy', 'burgundy_sweatsuit.png',65),
 ('Charcoal Grey', 'charcoal_grey_sweatsuit.png',65),
 ('Forest Green', 'forest_green_sweatsuit.png',64),
 ('Navy Blue', 'navy_blue_sweatsuit.png',65),
 ('Orange', 'orange_sweatsuit.png',65),
 ('Pink', 'pink_sweatsuit.png',63),
 ('Purple', 'purple_sweatsuit.png',64),
 ('Red', 'red_sweatsuit.png',68),
 ('Royal Blue','royal_blue_sweatsuit.png',65),
 ('Yellow', 'yellow_sweatsuit.png',67);

-- Consulta dos dados inseridos
select * 
from zenas_athleisure_db.products.sweatsuits;

-- ============================
-- JOIN COM DIRECTORY TABLE
-- ============================

-- Cria uma view unindo imagens do stage com dados da tabela interna
create or replace view zenas_athleisure_db.products.PRODUCT_LIST as   
select 
   INITCAP(REPLACE(REPLACE(relative_path, '_', ' '),'.png')) as product_name,
   s.file_name,
   s.color_or_style,
   s.price,
   d.file_url
from directory(@sweatsuits) d
join sweatsuits s
    on d.relative_path = s.file_name;

select * 
from zenas_athleisure_db.products.PRODUCT_LIST;

-- ============================
-- CATÁLOGO COM CROSS JOIN
-- ============================

-- Gera todas as combinações de produtos com tamanhos disponíveis
create or replace view zenas_athleisure_db.products.catalog as
select * 
from product_list p
cross join sweatsuit_sizes;

select * 
from zenas_athleisure_db.products.catalog;

-- ============================
-- UPSELL (PRODUTOS COMPLEMENTARES)
-- ============================

-- Tabela de mapeamento entre sweatsuits e sweatbands
create table zenas_athleisure_db.products.upsell_mapping
(
    sweatsuit_color_or_style varchar(25),
    upsell_product_code varchar(10)
);

-- Popula o mapeamento de upsell
insert into zenas_athleisure_db.products.upsell_mapping
values
('Charcoal Grey','SWT_GRY'),
('Forest Green','SWT_FGN'),
('Orange','SWT_ORG'),
('Pink','SWT_PNK'),
('Red','SWT_RED'),
('Yellow','SWT_YLW');

-- ============================
-- VIEW FINAL PARA O WEBSITE
-- ============================

-- Cria uma view pronta para consumo no site
create view catalog_for_website as 
select 
    color_or_style,
    price,
    file_name,
    get_presigned_url(@sweatsuits, file_name, 3600) as file_url, -- URL temporária da imagem
    size_list,
    coalesce(
        'Consider: ' || headband_description || ' & ' || wristband_description,
        'Consider: White, Black or Grey Sweat Accessories'
    ) as upsell_product_desc
from
(
    -- Agrega os tamanhos por produto
    select 
        color_or_style,
        price,
        file_name,
        listagg(sizes_available, ' | ') 
            within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, file_name
) c
left join upsell_mapping u
    on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
    on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
    on spl.product_code = sc.product_code;

-- Consulta final do catálogo completo
select * 
from catalog_for_website;
