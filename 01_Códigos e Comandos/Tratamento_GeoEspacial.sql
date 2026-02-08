-- Cria (ou recria) o banco de dados principal do desafio
create or replace database MELS_SMOOTHIE_CHALLENGE_DB;

-- Visualiza os dados brutos carregados a partir de arquivos GeoJSON
select *
from @trails_geojson
(file_format => FF_JSON);

-- Visualiza os dados brutos carregados a partir de arquivos Parquet
select *
from @trails_parquet
(file_format => FF_PARQUET);

/*
 Exemplo da estrutura de um registro do arquivo Parquet:
 Contém informações geográficas como latitude, longitude,
 sequência do ponto e nome da trilha
*/

-- Cria uma View estruturando os pontos da trilha Cherry Creek
-- Extraindo campos do Parquet e ordenando os pontos corretamente
create or replace view cherry_creek_trail as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng,
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

-- Consulta os pontos da trilha Cherry Creek
select * from cherry_creek_trail;

-- Monta uma LINESTRING com os primeiros 10 pontos da trilha
-- Apenas para teste e validação
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 10
group by trail_name;

-- Monta uma LINESTRING maior, usando até o ponto 2450
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 2450
group by trail_name;

-- Cria uma View a partir de dados GeoJSON das trilhas da região de Denver
-- Já calculando o comprimento das trilhas com funções geoespaciais
create or replace view DENVER_AREA_TRAILS as
select
 $1:features[0]:properties:Name::string as feature_name,
 $1:features[0]:geometry:coordinates::string as feature_coordinates,
 $1:features[0]:geometry::string as geometry,
 st_length(to_geography(geometry)) as trail_lenght,
 $1:features[0]:properties::string as feature_properties,
 $1:crs:properties:name::string as specs,
 $1 as whole_object
from @trails_geojson (file_format => ff_json);

-- Tentativa de calcular o comprimento da trilha
-- OBS: Essa abordagem não funciona porque o alias não pode ser reutilizado no mesmo SELECT
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring,
st_length(TO_GEOGRAPHY(my_linestring)) as length_of_trail
from cherry_creek_trail
group by trail_name;

-- Cria uma View definitiva das trilhas da região de Denver
-- Definindo explicitamente o schema e os nomes das colunas
create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS(
	FEATURE_NAME,
	FEATURE_COORDINATES,
	GEOMETRY,
    TRAIL_LENGTH,
	FEATURE_PROPERTIES,
	SPECS,
	WHOLE_OBJECT
) as
select
 $1:features[0]:properties:Name::string as feature_name,
 $1:features[0]:geometry:coordinates::string as feature_coordinates,
 $1:features[0]:geometry::string as geometry,
 st_length(to_geography(geometry)) as trail_length,
 $1:features[0]:properties::string as feature_properties,
 $1:crs:properties:name::string as specs,
 $1 as whole_object
from @trails_geojson (file_format => ff_json);

-- Consulta a View final de trilhas GeoJSON
SELECT * 
FROM MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS;

-- Cria uma segunda View de trilhas, agora a partir dos pontos Parquet
-- Reconstruindo o objeto GeoJSON manualmente
create or replace view DENVER_AREA_TRAILS_2 as
select 
 trail_name as feature_name,
 '{"coordinates":['||
 listagg('['||lng||','||lat||']',',') 
 within group (order by point_id)
 ||'],"type":"LineString"}' as geometry,
 st_length(to_geography(geometry))  as trail_length
from cherry_creek_trail
group by trail_name;

-- Une as trilhas vindas do GeoJSON e do Parquet
-- Mantendo estrutura de colunas compatível
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;

-- Cria uma View com informações geoespaciais adicionais
-- Calcula os limites geográficos (bounding box) das trilhas
create or replace view trails_and_boundaries as
select feature_name,
 to_geography(geometry) as my_linestring,
 st_xmin(my_linestring) as min_eastwest,
 st_xmax(my_linestring) as max_eastwest,
 st_ymin(my_linestring) as min_northsouth,
 st_ymax(my_linestring) as max_northsouth,
 trail_length
from DENVER_AREA_TRAILS
union all
select feature_name,
 to_geography(geometry) as my_linestring,
 st_xmin(my_linestring) as min_eastwest,
 st_xmax(my_linestring) as max_eastwest,
 st_ymin(my_linestring) as min_northsouth,
 st_ymax(my_linestring) as max_northsouth,
 trail_length
from DENVER_AREA_TRAILS_2;

-- Cria um polígono que representa o limite máximo das trilhas
select 'POLYGON(('|| 
    min(min_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||min(min_northsouth)||','|| 
    min(min_eastwest)||' '||min(min_northsouth)||'))' AS my_polygon
from trails_and_boundaries;

-- Define as coordenadas do Melanie's Cafe como variáveis
set mc_lng='-104.97300245114094';
set mc_lat='39.76471253574085';

-- Define as coordenadas do Confluence Park
set loc_lng='-105.00840763333615'; 
set loc_lat='39.754141917497826';

-- Testa as variáveis criando pontos geográficos
select st_makepoint($mc_lng,$mc_lat) as melanies_cafe_point;
select st_makepoint($loc_lng,$loc_lat) as confluent_park_point;

-- Calcula a distância entre o Melanie's Cafe e o Confluence Park
select st_distance(
        st_makepoint($mc_lng,$mc_lat),
        st_makepoint($loc_lng,$loc_lat)
        ) as mc_to_cp;

-- Define as coordenadas do Tivoli Center
set tc_lng='-105.00532059763648'; 
set tc_lat='39.74548137398218';

-- Usa uma UDF para calcular a distância até o Melanie's Cafe
select mels_smoothie_challenge_db.locations.distance_to_mc($tc_lng,$tc_lat);

-- Cria uma View com estabelecimentos concorrentes
-- Baseada em amenidades e tipos de culinária
create or replace view COMPETITION as
select * 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
     and 
     (name ilike '%jamba%' 
      or name ilike '%juice%'
      or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

-- Calcula a distância dos concorrentes até o Melanie's Cafe
select
 name,
 cuisine,
 ST_DISTANCE(
    st_makepoint('-104.97300245114094','39.76471253574085'),
    coordinates
 ) AS distance_to_melanies,
 *
from competition
order by distance_to_melanies;

-- Mesma análise usando uma UDF
select
 name,
 cuisine,
 distance_to_mc(coordinates) AS distance_to_melanies,
 *
from competition
order by distance_to_melanies;

-- Define as coordenadas da livraria Tattered Cover
set tcb_lng='-104.9956203'; 
set tcb_lat='39.754874';

-- Executa a primeira versão da UDF de distância
select distance_to_mc($tcb_lng,$tcb_lat);

-- Cria uma View com lojas de bicicleta em Denver
create or replace view DENVER_BIKE_SHOPS as
select
 name,
 ST_DISTANCE(
    st_makepoint('-104.97300245114094','39.76471253574085'),
    coordinates
 ) AS distance_to_melanies,
 coordinates
from openstreetmap_denver.denver.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES 
where shop = 'bicycle';

-- Consulta as lojas de bicicleta ordenadas pela distância
select * 
from DENVER_BIKE_SHOPS 
order by distance_to_melanies desc;

-- Cria uma tabela externa baseada em arquivos Parquet no S3
create or replace external table T_CHERRY_CREEK_TRAIL
(
	my_filename varchar(100) as (metadata$filename::varchar(100))
) 
location = @EXTERNAL_AWS_DLKW
auto_refresh = true
file_format = (type = parquet);

-- Consulta a tabela externa
select * from T_CHERRY_CREEK_TRAIL;

-- Cria uma Secure Materialized View
-- Calcula a distância de cada ponto da trilha até o Melanie's Cafe
create secure materialized view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL
(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR,
    DISTANCE_TO_MELANIES
) as
select 
 value:sequence_1 as point_id,
 value:trail_name::varchar as trail_name,
 value:latitude::number(11,8) as lng,
 value:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair,
 locations.distance_to_mc(st_makepoint(lng, lat)) as distance_to_melanies
from t_cherry_creek_trail;

-- Consulta a Secure Materialized View final
select * 
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL;
