

/*creare user ECOMMERCE pentru schema ECOMMERCE */

CREATE USER ECOMMERCE IDENTIFIED BY eco123
DEFAULT TABLESPACE users
TEMPORARY TABLESPACE temp
QUOTA UNLIMITED ON users;

GRANT connect, resource TO ECOMMERCE;







/*creare director pentru CSV extern*/

CREATE OR REPLACE DIRECTORY csv_dir AS 'C:\DataSources';

GRANT READ, WRITE ON DIRECTORY csv_dir TO ECOMMERCE;


SELECT directory_name, directory_path
FROM dba_directories
WHERE directory_name = 'CSV_DIR';

-----------------------------------------------------------------------------------
/*setari pentru mongodb*/

grant connect, resource to ECOMMERCE;
grant CREATE VIEW to ECOMMERCE;
grant create database link to ECOMMERCE;

----
grant CREATE ANY DIRECTORY to ECOMMERCE;
grant execute on utl_http to ECOMMERCE;
grant execute on dbms_lob to ECOMMERCE;

--- Permissions for ExcelTable
grant execute on sys.dbms_crypto to ECOMMERCE;
select value from v$parameter where name = 'cursor_sharing';

alter session set cursor_sharing = exact;

-- APEX suplimentary permissions
grant CREATE DIMENSION, CREATE JOB, CREATE MATERIALIZED VIEW, CREATE SYNONYM to ECOMMERCE;

--------------------------------------------------------------------------------
--- Permissions to invoke REST URLs --------------------------------------------
---
begin
  dbms_network_acl_admin.append_host_ace (
      host       => '*',
      lower_port => NULL,
      upper_port => NULL,
      ace        => xs$ace_type(privilege_list => xs$name_list('http'),
                                principal_name => 'ECOMMERCE',
                                principal_type => xs_acl.ptype_db));
  end;
/
COMMIT;

--------------------------------------------------------------------------------
SELECT host,
       lower_port,
       upper_port,
       ace_order,
       TO_CHAR(start_date, 'DD-MON-YYYY') AS start_date,
       TO_CHAR(end_date, 'DD-MON-YYYY') AS end_date,
       grant_type,
       inverted_principal,
       principal,
       principal_type,
       privilege
FROM   dba_host_aces
ORDER BY host, ace_order;
