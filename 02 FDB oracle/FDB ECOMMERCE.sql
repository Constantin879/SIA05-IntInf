
/*setari PGADMIN*/
 
 DROP DATABASE LINK PG;
CREATE DATABASE LINK PG
   CONNECT TO "postgres" IDENTIFIED BY postgres17
   USING '(DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1522))
    (CONNECT_DATA =
      (SID = PG_DSN)
    )
    (HS = OK)
    )';

SELECT * FROM "interactions"@PG;

SELECT * FROM "products"@PG;



--------------------------------------
/*setari CSV extern*/


SELECT directory_name, directory_path
FROM all_directories
WHERE directory_name = 'CSV_DIR';



DROP TABLE reviews_ext;

CREATE TABLE reviews_ext
(
 review_id VARCHAR2(50),
 user_id VARCHAR2(50),
 product_id VARCHAR2(50),
 rating VARCHAR2(50),
 review_text VARCHAR2(4000),
 review_date VARCHAR2(50)
)
ORGANIZATION EXTERNAL
(
 TYPE ORACLE_LOADER
 DEFAULT DIRECTORY csv_dir
 ACCESS PARAMETERS
 (
  RECORDS DELIMITED BY 0x'0A'
  SKIP 1
  FIELDS TERMINATED BY ','
  OPTIONALLY ENCLOSED BY '"'
 )
 LOCATION ('reviews.csv')
)
REJECT LIMIT UNLIMITED;


SELECT * FROM reviews_ext;



-----------------------
/*setari mongodb prin RESTheart */

select get_restheart_data_media('http://localhost:8081/ECOMMERCE-IntInf/userss', 'admin:secret') from dual;
select get_restheart_data_media('http://localhost:8081/ECOMMERCE-IntInf/Users', 'admin:secret') from dual;

CREATE OR REPLACE VIEW userss_view_mongodb AS
SELECT 
  *
FROM  JSON_TABLE( 
           (select get_restheart_data_media('http://localhost:8081/ECOMMERCE-IntInf/userss', 'admin:secret') from dual) , 
           '$[*].profile'  
            COLUMNS ( 
                user_id   PATH '$.user_id'  
                , user_name PATH '$.user_name'  
                , email PATH '$.email'  
            )  
);
select * from userss_view_mongodb;





/*CREARE VIEW NOU PT. TOATE ELEMENTELE DIN MONGODB*/

CREATE OR REPLACE VIEW users_full_mongodb AS
SELECT 
    jt.user_id,
    jt.user_name,
    jt.email,
    jt.age,
    jt.gender,
    jt.city,
    jt.signup_date,
    jt.segment
FROM JSON_TABLE(
    (
        SELECT get_restheart_data_media(
            'http://localhost:8081/ECOMMERCE-IntInf/userss', 
            'admin:secret'
        ) 
        FROM dual
    ),
    '$[*]'
    COLUMNS (
        user_id      NUMBER        PATH '$.profile.user_id',
        user_name    VARCHAR2(100) PATH '$.profile.user_name',
        email        VARCHAR2(100) PATH '$.profile.email',
        age          NUMBER        PATH '$.profile.age',
        gender       VARCHAR2(20)  PATH '$.profile.gender',
        city         VARCHAR2(50)  PATH '$.profile.city',
        signup_date  VARCHAR2(50)  PATH '$.profile.signup_date',

        NESTED PATH '$.segments[*]'
        COLUMNS (
            segment VARCHAR2(50) PATH '$'
        )
    )
) jt;

select * from users_full_mongodb;


