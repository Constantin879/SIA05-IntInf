
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




------------------------------------------------
------------------------------------------------

-----------/*TEMA 3*/------------------

------------------------------------------------




/*MEMBRU 1*/

/*1. view  CONSOLIDARE1  */

CREATE OR REPLACE VIEW vw_consolidare AS
SELECT 
    u.user_id,
    u.user_name,
    u.city,
    u.segment,
    
    p."product_id",
    p."product_name",
    p."category",
    p."sub_category",
    p."brand",
    
    i."event_type",
    i."event_value",
    
    TO_NUMBER(r.rating) AS rating
FROM users_full_mongodb u
JOIN "interactions"@PG i 
    ON u.user_id = i."user_id"
JOIN "products"@PG p 
    ON p."product_id" = i."product_id"
LEFT JOIN reviews_ext r 
    ON r.product_id = p."product_id";
    
    
SELECT * FROM vw_consolidare FETCH FIRST 5 ROWS ONLY;
/*View-ul evidentiaza comportamentul utilizatorilor in functie de segment si produse,
prin tipul si intensitatea interactiunilor, precum si prin evaluarile (rating-urile) acestora*/




    
/*2. FACT view - tabela de fapte1*/

CREATE OR REPLACE VIEW fact_activitate AS
SELECT 
    "product_id",
    user_id,
    "category",
    "sub_category",
    "brand",
    segment,
    
    COUNT(*) AS nr_interactii,
    SUM("event_value") AS scor_activitate,
    AVG(rating) AS rating_mediu
FROM vw_consolidare
GROUP BY 
    "product_id",
    user_id,
    "category",
    "sub_category",
    "brand",
    segment;
    
SELECT * FROM fact_activitate FETCH FIRST 5 ROWS ONLY;

/*View-ul agrega activitatea utilizatorilor asupra produselor,
evidentiind numarul de interactiuni, intensitatea acestora si rating-ul mediu,
in functie de produs, categorie si segment*/



/*3. DIMENSIUNI */


/*3.1.dim_produs + rating + interactiuni*/
CREATE OR REPLACE VIEW dim_user_product AS
SELECT DISTINCT
    user_id,
    user_name,
    city,
    segment,
    
    "product_id",
    "product_name",
    "category",
    "brand",
    
    "event_type",
    rating
FROM vw_consolidare;

SELECT * FROM dim_user_product FETCH FIRST 10 ROWS ONLY;

/*View-ul evidentiaza relatia dintre utilizatori si produse,
incluzand caracteristicile utilizatorilor, ale produselor si tipurile de interactiuni,
impreuna cu evaluarile (rating-urile) asociate*/



/*3.2. dim_produs + rating + interactiuni*/
CREATE OR REPLACE VIEW dim_product_fullP AS
SELECT DISTINCT
    "product_id",
    "product_name",
    "category",
    "sub_category",
    "brand",
    
    "event_type",
    "event_value",
    rating
FROM vw_consolidare;

SELECT * FROM dim_product_fullP FETCH FIRST 10 ROWS ONLY;

/*View-ul evidentiaza caracteristicile produselor impreuna cu tipurile si intensitatea interactiunilor,
precum si evaluarile (rating-urile) asociate acestora*/



/*3.3. dim_utilizator + activitate + rating*/
 CREATE OR REPLACE VIEW dim_user_activity AS
SELECT DISTINCT
    user_id,
    user_name,
    city,
    segment,
    
    "event_type",
    "event_value",
    rating
FROM vw_consolidare; 

SELECT * FROM dim_user_activity FETCH FIRST 10 ROWS ONLY;

/*View-ul evidentiaza activitatea utilizatorilor,
incluzand tipurile si intensitatea interactiunilor,
precum si evaluarile (rating-urile) asociate acestora*/



/*4. view analytic OLAP_cube*/

CREATE OR REPLACE VIEW olap_cube AS
SELECT 
    NVL("category", 'TOTAL') AS category,
    NVL("brand", 'TOTAL') AS brand,
    NVL(segment, 'TOTAL') AS segment,
    COUNT(*) AS total_interactiuni,
    SUM("event_value") AS scor_total
FROM vw_consolidare
GROUP BY CUBE (
    "category",
    "brand",
    segment
);

SELECT * FROM olap_cube FETCH FIRST 10 ROWS ONLY;

/*View-ul olap_cube realizeaza o analiza multidimensionala a interactiunilor, 
agregand datele pe categorii, branduri si segmente de utilizatori, 
folosind operatorul CUBE pentru a genera toate combinatiile posibile, inclusiv subtotaluri si total general*/
/*Cine (segment) interactioneaza cu ce (categorie + brand) si cat de mult*/


/*5. View OLAP rollup*/
CREATE OR REPLACE VIEW olap_rollup AS
SELECT 
    NVL("category",'TOTAL') AS category,
    NVL("brand",'TOTAL') AS brand,
    COUNT(*) AS total_interactiuni
FROM vw_consolidare
GROUP BY ROLLUP("category","brand");

SELECT * FROM olap_rollup ORDER BY category, brand;

/*View-ul calculeaza numarul total de interactiuni si scorul total agregat pe categorii si branduri,
incluzand subtotaluri pe categorie si total general.*/



/*6. view olap_segment*/

CREATE OR REPLACE VIEW olap_segment AS
SELECT 
    NVL(segment, 'TOTAL') AS segment,
    NVL("category", 'TOTAL') AS category,
    NVL("brand", 'TOTAL') AS brand,
    
    COUNT(*) AS total_interactiuni,
    SUM("event_value") AS scor_total
FROM vw_consolidare
GROUP BY CUBE (
    segment,
    "category",
    "brand"
);

SELECT * FROM olap_segment ORDER BY segment, category, brand;
    
/*View-ul OLAP olap_segment realizeaza o analiza multidimensionala a interactiunilor in functie de segmentul utilizatorilor, categorie si brand,
utilizand operatorul CUBE pentru a genera toate combinatiile posibile, inclusiv totaluri si subtotaluri.
Datele provin dintr-un view de consolidare care integreaza MongoDB, PostgreSQL si CSV.*/




--------------------------------------------------------------------------------

/*MEMBRU 2*/

/*1. view consolidare2*/
CREATE OR REPLACE VIEW vw_consolidare_product_reviews AS
SELECT 
    u.user_id,
    u.segment,
    
    p."product_id",
    p."product_name",
    p."category",
    p."brand",
    
    i."event_type",
    SUM(i."event_value") AS scor_interactiuni,
    
    COUNT(r.review_id) AS nr_reviews,
    AVG(TO_NUMBER(r.rating)) AS rating_mediu
FROM users_full_mongodb u
JOIN "interactions"@PG i
    ON u.user_id = i."user_id"
JOIN "products"@PG p
    ON p."product_id" = i."product_id"
LEFT JOIN reviews_ext r
    ON p."product_id" = r.product_id
GROUP BY 
    u.user_id,
    u.segment,
    p."product_id",
    p."product_name",
    p."category",
    p."brand",
    i."event_type";
    
SELECT * FROM vw_consolidare_product_reviews FETCH FIRST 5 ROWS ONLY;

/*View-ul integreaza si agrega date despre utilizatori, produse si review-uri,
evidentiind intensitatea interactiunilor, numarul de evaluari si rating-ul mediu,
in functie de utilizator, produs si tipul interactiunii*/



/*2. View tabela de fapte2*/

CREATE OR REPLACE VIEW fact_product_reviews AS
SELECT 
    "product_id",
    "category",
    "brand",
    segment,
    
    SUM(scor_interactiuni) AS scor_total_interactiuni,
    SUM(nr_reviews) AS total_reviews,
    AVG(rating_mediu) AS rating_mediu
FROM vw_consolidare_product_reviews
GROUP BY 
    "product_id",
    "category",
    "brand",
    segment;

SELECT * FROM fact_product_reviews FETCH FIRST 10 ROWS ONLY;

/*View-ul agrega activitatea produselor,
evidentiind scorul total al interactiunilor, numarul total de review-uri si rating-ul mediu,
in functie de produs, categorie, brand si segment*/




/*3. VIEW dimensiune 3 surse*/

CREATE OR REPLACE VIEW dim_product_full AS
SELECT DISTINCT
    p."product_id",
    p."product_name",
    p."category",
    p."sub_category",
    p."brand",
    
    u.segment,
    
    COUNT(r.review_id) OVER (PARTITION BY p."product_id") AS nr_reviews,
    AVG(TO_NUMBER(r.rating)) OVER (PARTITION BY p."product_id") AS rating_mediu
FROM "products"@PG p
JOIN "interactions"@PG i
    ON p."product_id" = i."product_id"
JOIN users_full_mongodb u
    ON u.user_id = i."user_id"
LEFT JOIN reviews_ext r
    ON p."product_id" = r.product_id;


SELECT *FROM dim_product_full FETCH FIRST 10 ROWS ONLY;
/*View-ul evidentiaza caracteristicile produselor in functie de segmentul utilizatorilor,
incluzand numarul de review-uri si rating-ul mediu asociat fiecarui produs*/

/*Interogarea evidentiaza distributia produselor pe categorii, branduri si segmente de utilizatori, impreuna cu evaluarile si numarul de review-uri*/





/*4. view olap_cube */
CREATE OR REPLACE VIEW olap_product_reviews AS
SELECT 
    NVL("category", 'TOTAL') AS category,
    NVL("brand", 'TOTAL') AS brand,
    NVL(segment, 'TOTAL') AS segment,
    
    SUM(scor_interactiuni) AS scor_total,
    SUM(nr_reviews) AS total_reviews,
    AVG(rating_mediu) AS rating_mediu
FROM vw_consolidare_product_reviews
GROUP BY CUBE (
    "category",
    "brand",
    segment
);

SELECT *FROM olap_product_reviews FETCH FIRST 10 ROWS ONLY;
/*View-ul realizeaza o analiza multidimensionala a interactiunilor si review-urilor,
agregand datele pe categorii, branduri si segmente de utilizatori,
utilizand operatorul CUBE pentru a genera toate combinatiile posibile, inclusiv subtotaluri si total general*/



/*5. view olap_rollup  */
CREATE OR REPLACE VIEW olap_product_reviews_rollup AS
SELECT 
    NVL("category",'TOTAL') AS category,
    NVL("brand",'TOTAL') AS brand,
    
    SUM(scor_interactiuni) AS scor_total,
    SUM(nr_reviews) AS total_reviews
FROM vw_consolidare_product_reviews
GROUP BY ROLLUP("category","brand");

SELECT *FROM olap_product_reviews_rollup FETCH FIRST 10 ROWS ONLY;

/*View-ul realizeaza o agregare ierarhica a interactiunilor si review-urilor,
pe categorii si branduri de produse,
utilizand operatorul ROLLUP pentru a genera subtotaluri si totalul general*/





/*6. view olap_segment */
CREATE OR REPLACE VIEW olap_segment_category_reviews AS
SELECT 
    NVL(segment,'TOTAL') AS segment,
    NVL("category",'TOTAL') AS category,
    
    SUM(scor_interactiuni) AS scor_total,
    SUM(nr_reviews) AS total_reviews
FROM vw_consolidare_product_reviews
GROUP BY CUBE(segment, "category");


SELECT * FROM olap_segment_category_reviews ORDER BY segment, category;
/*View-ul realizeaza o analiza multidimensionala a interactiunilor si review-urilor,
agregand datele pe segmente de utilizatori si categorii de produse,
utilizand operatorul CUBE pentru a genera toate combinatiile posibile, inclusiv subtotaluri si totaluri*/

/*View-ul evidentiaza interactiunile si review-urile in functie de segment si categorie,
permitand analiza comparativa intre diferite segmente de utilizatori si tipuri de produse*/





--------------------------------------------------------------------------------

/*MEMBRU 3*/
    
/*1. View consolidare 3*/

CREATE OR REPLACE VIEW vw_consolidare_full_analysis AS
SELECT 
    u.user_id,
    u.user_name,
    u.segment,
    u.city,
    
    p."product_id",
    p."product_name",
    p."category",
    p."brand",
    
    i."event_type",
    SUM(i."event_value") AS scor_interactiuni,
    
    COUNT(r.review_id) AS nr_reviews,
    AVG(TO_NUMBER(r.rating)) AS rating_mediu
FROM users_full_mongodb u
JOIN "interactions"@PG i
    ON u.user_id = i."user_id"
JOIN "products"@PG p
    ON p."product_id" = i."product_id"
LEFT JOIN reviews_ext r
    ON p."product_id" = r.product_id
GROUP BY 
    u.user_id,
    u.user_name,
    u.segment,
    u.city,
    p."product_id",
    p."product_name",
    p."category",
    p."brand",
    i."event_type";
    
SELECT * FROM vw_consolidare_full_analysis FETCH FIRST 10 ROWS ONLY;
/*View-ul integreaza si agrega date despre utilizatori, produse si interactiuni,
evidentiind comportamentul acestora prin intensitatea interactiunilor,
numarul de review-uri si rating-ul mediu, in functie de utilizator, segment si produs*/



/*2. view tabela de fapte 3*/
CREATE OR REPLACE VIEW fact_full_analysis AS
SELECT 
    "product_id",
    user_id,
    "category",
    "brand",
    segment,
    
    SUM(scor_interactiuni) AS scor_total,
    SUM(nr_reviews) AS total_reviews,
    AVG(rating_mediu) AS rating_mediu
FROM vw_consolidare_full_analysis
GROUP BY 
    "product_id",
    user_id,
    "category",
    "brand",
    segment;
    
SELECT * FROM fact_full_analysis FETCH FIRST 10 ROWS ONLY;

/*View-ul agrega activitatea utilizatorilor asupra produselor,
evidentiind scorul total al interactiunilor, numarul de review-uri si rating-ul mediu,
in functie de produs, utilizator, categorie, brand si segment*/


/*DIMENSIUNI*/
/*3.1 view dim_user_full 3 surse*/

CREATE OR REPLACE VIEW dim_user_full AS
SELECT DISTINCT
    user_id,
    user_name,
    segment,
    city,
    
    "category",
    "brand",
    
    "event_type",
    rating_mediu
FROM vw_consolidare_full_analysis;

SELECT * FROM dim_user_full FETCH FIRST 10 ROWS ONLY;

/* View-ul evidentiaza caracteristicile utilizatorilor impreuna cu interactiunile acestora,
incluzand segmentul, locatia, produsele asociate si rating-urile aferente*/





/*3.2 view dimensiune produs  extinsa 3 surse*/

CREATE OR REPLACE VIEW dim_product_fulll AS
SELECT DISTINCT
    "product_id",
    "product_name",
    "category",
    "brand",
    
    segment,
    "event_type",
    rating_mediu
FROM vw_consolidare_full_analysis;

SELECT * FROM dim_product_fulll FETCH FIRST 10 ROWS ONLY;

/*View-ul evidentiaza caracteristicile produselor impreuna cu segmentul utilizatorilor,
tipurile de interactiuni si evaluarile (rating-urile) asociate acestora*/



/*3.3 view dimensiune comportament extinsa 3 surse*/
CREATE OR REPLACE VIEW dim_behavior_full AS
SELECT DISTINCT
    "event_type",
    
    segment,
    city,
    
    "category",
    "brand",
    
    rating_mediu
FROM vw_consolidare_full_analysis;

SELECT * FROM dim_behavior_full FETCH FIRST 10 ROWS ONLY;
/*View-ul evidentiaza tipurile de interactiuni ale utilizatorilor,
in functie de segment, locatie si produse, impreuna cu evaluarile (rating-urile) asociate*/





/*4. view olap_cube*/

CREATE OR REPLACE VIEW olap_full_segment_category_event AS
SELECT 
    NVL(segment,'TOTAL') AS segment,
    NVL("category",'TOTAL') AS category,
    NVL("event_type",'TOTAL') AS event_type,
    
    SUM(scor_interactiuni) AS scor_total,
    SUM(nr_reviews) AS total_reviews,
    AVG(rating_mediu) AS rating_mediu
FROM vw_consolidare_full_analysis
GROUP BY CUBE(
    segment,
    "category",
    "event_type"
);


SELECT * FROM olap_full_segment_category_event ORDER BY scor_total DESC;
/*View-ul realizeaza o analiza multidimensionala a interactiunilor si review-urilor,
agregand datele pe segmente, categorii si tipuri de interactiuni,
utilizand operatorul CUBE pentru a genera toate combinatiile posibile, inclusiv subtotaluri si totaluri*/



/*5. view olap_rollup*/

CREATE OR REPLACE VIEW olap_full_category_brand_rollup AS
SELECT 
    NVL("category",'TOTAL') AS category,
    NVL("brand",'TOTAL') AS brand,
    
    SUM(scor_interactiuni) AS scor_total,
    SUM(nr_reviews) AS total_reviews
FROM vw_consolidare_full_analysis
GROUP BY ROLLUP(
    "category",
    "brand"
);

SELECT * FROM olap_full_category_brand_rollup ORDER BY category, brand;

/*View-ul realizeaza o agregare ierarhica a interactiunilor si review-urilor,
pe categorii si branduri de produse,
utilizand operatorul ROLLUP pentru a genera subtotaluri si totalul general*/




/*6. view olap_segment*/
CREATE OR REPLACE VIEW olap_full_segment_brand AS
SELECT 
    NVL(segment,'TOTAL') AS segment,
    NVL("brand",'TOTAL') AS brand,
    
    SUM(scor_interactiuni) AS scor_total,
    AVG(rating_mediu) AS rating_mediu
FROM vw_consolidare_full_analysis
GROUP BY CUBE(
    segment,
    "brand"
);


SELECT * FROM olap_full_segment_brand ORDER BY scor_total DESC;

/*View-ul realizeaza o analiza multidimensionala a interactiunilor si rating-urilor,
agregand datele pe segmente de utilizatori si branduri,
utilizand operatorul CUBE pentru a genera toate combinatiile posibile, inclusiv subtotaluri si totaluri*/

