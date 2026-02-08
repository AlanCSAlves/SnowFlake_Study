-- Criando um novo banco de dados e definindo o contexto para utilizá-lo
create database library_card_catalog 
comment = 'DWW Lesson 10 ';

-- Definindo o contexto da worksheet para usar o novo banco de dados
use database library_card_catalog;
use role sysadmin;

-- Criando a tabela de livros e utilizando AUTOINCREMENT para gerar um UID único
-- para cada novo registro
create or replace table book
(
  book_uid number autoincrement,
  title varchar(50),
  year_published number(4,0)
);

-- Inserindo registros na tabela de livros
-- Não é necessário informar o campo BOOK_UID
-- pois a propriedade AUTOINCREMENT cuidará disso automaticamente
insert into book(title, year_published)
values
 ('Food',2001),
 ('Food',2006),
 ('Food',2008),
 ('Food',2016),
 ('Food',2015);

-- Verificando a tabela
-- Cada linha possui um identificador único?
select * from book;

-- Criando a tabela de autores
create or replace table author
(
   author_uid number,
   first_name varchar(50),
   middle_name varchar(50),
   last_name varchar(50)
);

-- Inserindo os dois primeiros autores na tabela AUTHOR
insert into author(author_uid, first_name, middle_name, last_name)  
values
 (1, 'Fiona', '', 'Macdonald'),
 (2, 'Gian', 'Paulo', 'Faleschini');

-- Visualizando a tabela com os novos registros
select * 
from author;

-- Scripts DDL para JSON
use database library_card_catalog;
use role sysadmin;

use database library_card_catalog;
use role sysadmin;

-- Criando uma sequência para gerar AUTHOR_UID automaticamente
create or replace sequence library_card_catalog.public.seq_author_uid
    start = 3 
    increment = 1 
    order
    comment = 'Usar esta sequência para preencher o AUTHOR_UID sempre que uma nova linha for adicionada';

use role sysadmin;

-- Verificando como a função NEXTVAL funciona
select seq_author_uid.nextval;

-- Inserindo os autores restantes utilizando a função NEXTVAL
-- em vez de informar manualmente os números
insert into author(author_uid, first_name, middle_name, last_name) 
values
 (seq_author_uid.nextval, 'Laura', 'K', 'Egendorf'),
 (seq_author_uid.nextval, 'Jan', '', 'Grover'),
 (seq_author_uid.nextval, 'Jennifer', '', 'Clapp'),
 (seq_author_uid.nextval, 'Kathleen', '', 'Petelinsek');

select * from author;

-- Criando a tabela de relacionamento
-- Este tipo de tabela é conhecido como "Muitos-para-Muitos"
create table book_to_author
(
  book_uid number,
  author_uid number
);

-- Inserindo os relacionamentos conhecidos entre livros e autores
insert into book_to_author(book_uid, author_uid)
values
 (1,1),  -- Relaciona o livro de 2001 com Fiona Macdonald
 (1,2),  -- Relaciona o livro de 2001 com Gian Paulo Faleschini
 (2,3),  -- Relaciona o livro de 2006 com Laura K Egendorf
 (3,4),  -- Relaciona o livro de 2008 com Jan Grover
 (4,5),  -- Relaciona o livro de 2016 com Jennifer Clapp
 (5,6);  -- Relaciona o livro de 2015 com Kathleen Petelinsek

-- Conferindo o resultado ao realizar JOIN entre as três tabelas
-- O resultado deve conter uma linha para cada autor
select * 
from book_to_author ba 
join author a 
  on ba.author_uid = a.author_uid 
join book b 
  on b.book_uid = ba.book_uid; 

-- Criando uma tabela de ingestão para dados JSON
create table library_card_catalog.public.author_ingest_json
(
  raw_author variant
);

-- Criando o File Format para dados JSON
create or replace file format library_card_catalog.public.json_file_format
type = 'JSON' 
compression = 'AUTO' 
enable_octal = FALSE
allow_duplicate = FALSE 
strip_outer_array = TRUE
strip_null_values = FALSE 
ignore_utf8_errors = FALSE;

-- Abrindo o arquivo JSON utilizando o novo File Format configurado
select $1
from @util_db.public.my_internal_stage/author_with_header.json
(file_format => library_card_catalog.public.json_file_format);

-- Copiando os arquivos JSON para a tabela recém-criada
copy into library_card_catalog.public.author_ingest_json
from @util_db.public.my_internal_stage
files = ('author_with_header.json')
file_format = ( format_name = library_card_catalog.public.json_file_format);

select * 
from library_card_catalog.public.author_ingest_json;

-- Retornando o valor AUTHOR_UID do atributo de nível superior do objeto JSON
select raw_author:AUTHOR_UID
from author_ingest_json;

-- Retornando os dados em um formato semelhante a uma tabela normalizada
select 
 raw_author:AUTHOR_UID as AUTHOR_UID,
 raw_author:FIRST_NAME::string as FIRST_NAME,
 raw_author:MIDDLE_NAME::string as MIDDLE_NAME,
 raw_author:LAST_NAME::string as LAST_NAME
from author_ingest_json;

-- Criando tabela para ingestão de JSON aninhado
create or replace table library_card_catalog.public.nested_ingest_json 
(
  raw_nested_book variant
);

-- Abrindo o arquivo JSON aninhado utilizando o File Format configurado
select $1
from @util_db.public.my_internal_stage/NESTED_INGEST_JSON.json
(file_format => library_card_catalog.public.json_file_format);

-- Copiando o arquivo JSON aninhado para a tabela
copy into library_card_catalog.public.nested_ingest_json
from @util_db.public.my_internal_stage
files = ('NESTED_INGEST_JSON.json')
file_format = ( format_name = library_card_catalog.public.json_file_format);

-- Algumas consultas simples
select raw_nested_book
from nested_ingest_json;

select raw_nested_book:year_published
from nested_ingest_json;

select raw_nested_book:authors
from nested_ingest_json;

-- Utilizando comandos FLATTEN para explorar os dados aninhados de livros e autores
select value:first_name
from nested_ingest_json,
lateral flatten(input => raw_nested_book:authors);

select value:first_name
from nested_ingest_json,
table(flatten(raw_nested_book:authors));

-- Adicionando CAST aos campos retornados
select 
  value:first_name::varchar,
  value:last_name::varchar
from nested_ingest_json,
lateral flatten(input => raw_nested_book:authors);

-- Atribuindo novos nomes às colunas utilizando "AS"
select 
  value:first_name::varchar as first_nm,
  value:last_name::varchar as last_nm
from nested_ingest_json,
lateral flatten(input => raw_nested_book:authors);
