WITH tbl AS (
 SELECT t.Table_Schema, t.Table_Name,
   (
   	select count(*) from information_schema.columns c
   	where c.table_name = t.Table_Name
	   and c.Table_Schema = Table_Schema
   ) as column_count
 FROM   information_schema.Tables t
 WHERE  Table_Name NOT LIKE 'pg_%'
)
SELECT  Table_Schema AS Schema_Name,
       Table_Name,
       (xpath('/row/c/text()', query_to_xml(format(
          'SELECT count(*) AS c FROM %I.%I', Table_Schema, Table_Name
        ), FALSE, TRUE, '')))[1]::text::int AS row_count,
		column_count
FROM    tbl
WHERE Table_Name=:Table_Name
ORDER   BY Table_Name;