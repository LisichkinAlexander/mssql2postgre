SET NOCOUNT ON;

DECLARE
      @object_name SYSNAME
	, @ms_object_name SYSNAME
    , @object_id INT
    , @SQL NVARCHAR(MAX)

SELECT
      @object_name = '"' + OBJECT_NAME([object_id]) + '"'
    , @ms_object_name = '[' + OBJECT_SCHEMA_NAME(o.[object_id]) + '].[' + OBJECT_NAME([object_id]) + ']'
    , @object_id = [object_id]
FROM (SELECT [object_id] = OBJECT_ID('%s', 'U')) o

IF OBJECT_ID('tempdb..##TEXT_FIELD_WITH_ZERO') IS NOT NULL
    DROP TABLE ##TEXT_FIELD_WITH_ZERO

-- PostgreSQL doesn't support storing NULL (0x00) characters in text
select @SQL = 'select * into ##TEXT_FIELD_WITH_ZERO from (' + 
	STUFF((
	select
		' union all select count(*) as CNT, ''' + c.name + ''' column_name from ' + @ms_object_name + ' where [' + c.name +'] like ''%'' + CHAR(0) + ''%'''
	from
		sys.columns c WITH(NOLOCK)
	join
		(
			select tp2.name, tp.user_type_id
			from sys.types tp WITH(NOLOCK)
			left join sys.types tp2 WITH(NOLOCK) on tp.system_type_id = tp2.system_type_id and tp2.user_type_id = tp2.system_type_id
		) t ON c.user_type_id = t.user_type_id
	where
		t.name in ('TEXT', 'NTEXT', 'VARCHAR', 'NVARCHAR')
		and c.object_id = @object_id
		FOR XML PATH(''), TYPE).value('.', 'VARCHAR(MAX)'), 1, 10, '') +') t'

if @SQL is null 
	set @SQL = 'select * into ##TEXT_FIELD_WITH_ZERO from (select cast(null as int) as cnt, cast(null as nvarchar) as column_name) t' 

EXECUTE sp_executesql @SQL

SELECT
	c.name FIELD_NAME,
	UPPER(tp.name) SOURCE_TYPE,
    CASE 
        WHEN tp.name = 'BINARY' THEN 'BYTEA'
        WHEN tp.name = 'BIT' THEN 'BOOLEAN'
        WHEN tp.name = 'VARCHAR' and c.max_length = -1 and IsNull(tf.cnt, 0) = 0 THEN 'TEXT'
        WHEN tp.name = 'VARCHAR' and c.max_length = -1 and IsNull(tf.cnt, 0) > 0 THEN 'BYTEA'
        WHEN tp.name = 'VARBINARY' THEN 'BYTEA'
        WHEN tp.name = 'NVARCHAR' and c.max_length = -1 and IsNull(tf.cnt, 0) = 0 THEN 'TEXT'
        WHEN tp.name = 'NVARCHAR' and c.max_length = -1 and IsNull(tf.cnt, 0) > 0 THEN 'BYTEA'
        WHEN tp.name = 'NVARCHAR' THEN 'VARCHAR'
        WHEN tp.name = 'NTEXT' and IsNull(tf.cnt, 0) = 0 THEN 'TEXT'
        WHEN tp.name = 'NTEXT' and IsNull(tf.cnt, 0) > 0 THEN 'BYTEA'
        WHEN tp.name = 'DATETIME' THEN 'TIMESTAMP'
        WHEN tp.name = 'IMAGE' THEN 'BYTEA'
        WHEN tp.name = 'DATETIMEOFFSET' THEN 'TIMESTAMP'
        WHEN tp.name = 'UNIQUEIDENTIFIER' THEN 'UUID'
        WHEN tp.name = 'DATETIME2' THEN 'TIMESTAMP'
        WHEN tp.name = 'TINYINT' THEN 'SMALLINT'
        WHEN tp.name = 'SMALLMONEY' THEN 'MONEY'
        WHEN tp.name = 'SMALLDATETIME' THEN 'TIMESTAMP(0)'
        WHEN tp.name = 'ROWVERSION' THEN 'BYTEA'
		WHEN tp.name = 'TEXT' and tf.cnt > 0 THEN 'BYTEA'
		WHEN tp.name = 'VARCHAR' and tf.cnt > 0 THEN 'BYTEA'
        ELSE UPPER(tp.name)
    END  DESTINATION_TYPE
FROM sys.columns c WITH(NOLOCK)
JOIN (
	select tp2.name, tp.user_type_id
	from sys.types tp WITH(NOLOCK)
	left join sys.types tp2 WITH(NOLOCK) on tp.system_type_id = tp2.system_type_id and tp2.user_type_id = tp2.system_type_id
) tp ON c.user_type_id = tp.user_type_id
LEFT JOIN ##TEXT_FIELD_WITH_ZERO tf on c.name = tf.column_name
WHERE c.[object_id] = @object_id
