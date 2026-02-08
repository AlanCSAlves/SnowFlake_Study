-- Consulta inicial para verificar os dados existentes na tabela ROOT_DEPTH
SELECT * 
FROM ROOT_DEPTH;

-- Inserindo valores de referência para profundidade de raiz
-- Cada linha representa um tipo de profundidade e seu intervalo em centímetros
insert into root_depth 
values
(1,'S','Shallow','cm',30,45),
(2,'M','Medium','cm',45,60),
(3,'D','Deep','cm',60,90);

-- Criação da tabela de detalhes de vegetais
-- Armazena o nome da planta e o código da profundidade da raiz
create table garden_plants.veggies.vegetable_details
(
  plant_name varchar(25),
  root_depth_code varchar(1)    
);

-- Verificando se existem registros duplicados por planta
select 
    plant_name,
    count(*) as registros
from garden_plants.veggies.vegetable_details 
group by plant_name
having count(*) > 1;

-- Removendo um registro específico duplicado (Spinach com raiz profunda)
delete 
from vegetable_details 
where plant_name = 'Spinach' 
  and root_depth_code = 'D';

-- Consultando os dados atualizados da tabela vegetable_details
select *
from garden_plants.veggies.vegetable_details;

-- Criando tabela para armazenar detalhes de flores
create or replace TABLE FLOWER_DETAILS
(
	PLANT_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);

-- Criando tabela para armazenar detalhes de frutas
create or replace TABLE Fruit_DETAILS
(
	PLANT_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);

-- Criação da tabela que relaciona vegetais com tipo de solo
create or replace table vegetable_details_soil_type
(
  plant_name varchar(25),
  soil_type number(1,0)
);

-- Criando File Format para arquivos CSV separados por pipe (|)
-- Contém uma linha de cabeçalho
create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
    type = 'CSV' -- CSV é usado para qualquer arquivo texto plano
    field_delimiter = '|' -- Pipe como separador de colunas
    skip_header = 1; -- Ignora a primeira linha (cabeçalho)

-- Copiando os dados do arquivo para a tabela vegetable_details_soil_type
copy into vegetable_details_soil_type
from @util_db.public.my_internal_stage
files = ( 'VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
file_format = ( format_name = GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW );

-- Tratando a segunda tabela de lookup de tipos de solo

-- Criando File Format para arquivos separados por vírgula
-- Com valores opcionalmente envolvidos por aspas duplas
create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV' -- CSV para arquivos separados por vírgula
    FIELD_DELIMITER = ',' -- Vírgula como separador
    SKIP_HEADER = 1 -- Ignora a linha de cabeçalho
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'; 
    -- Algumas colunas usam aspas por conterem vírgulas no texto

-- Visualizando o arquivo sem especificar File Format
select $1
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv;

-- Visualizando o mesmo arquivo utilizando o File Format de vírgula
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

-- Visualizando o mesmo arquivo utilizando o File Format de pipe
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW);

-- Criando um novo File Format específico para o desafio
-- Utiliza TAB como separador de colunas
create file format garden_plants.veggies.L9_CHALLENGE_FF 
    TYPE = 'CSV'
    FIELD_DELIMITER = '\t'  -- TAB como separador
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE;

-- Testando a leitura do arquivo com o novo File Format
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.L9_CHALLENGE_FF);

-- Criando tabela de lookup para tipos de solo
create or replace table LU_SOIL_TYPE
(
    SOIL_TYPE_ID number,	
    SOIL_TYPE varchar(15),
    SOIL_DESCRIPTION varchar(75)
);

-- Copiando os dados do arquivo LU_SOIL_TYPE.tsv para a tabela
copy into lu_soil_type
from @util_db.public.my_internal_stage
files = ('LU_SOIL_TYPE.tsv')
file_format = ( format_name = GARDEN_PLANTS.VEGGIES.L9_CHALLENGE_FF );

-- Criando tabela com informações de altura das plantas
create or replace table VEGETABLE_DETAILS_PLANT_HEIGHT
(
    PLANT_NAME VARCHAR(75),
    UOM VARCHAR(1), -- Unidade de medida
    LOW_END_OF_RANGE NUMBER,
    HIGH_END_OF_RANGE NUMBER
);

-- Testando a leitura do arquivo de altura das plantas
select $1, $2, $3, $4
from @util_db.public.my_internal_stage/veg_plant_height.csv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

-- Copiando os dados do arquivo para a tabela de altura das plantas
copy into VEGETABLE_DETAILS_PLANT_HEIGHT
from @util_db.public.my_internal_stage
files = ('veg_plant_height.csv')
file_format = ( format_name = GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW );

-- Consulta final para validar os dados carregados
SELECT * 
FROM VEGETABLE_DETAILS_PLANT_HEIGHT;
