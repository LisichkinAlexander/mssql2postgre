--- SCRIPT TO GENERATE THE CREATION SCRIPT OF ALL FOREIGN KEY CONSTRAINTS
declare @ForeignKeyID int
declare @ForeignKeyName varchar(4000)
declare @ParentTableName varchar(4000)
declare @ParentColumn varchar(4000)
declare @ReferencedTable varchar(4000)
declare @ReferencedColumn varchar(4000)
declare @StrParentColumn varchar(max)
declare @StrReferencedColumn varchar(max)
declare @ParentTableSchema varchar(4000)
declare @ReferencedTableSchema varchar(4000)
declare @TSQLCreationFK varchar(max)

IF OBJECT_ID('tempdb..#SQL_TEXT') IS NOT NULL 
begin
    DROP TABLE #SQL_TEXT
end
create table #SQL_TEXT ([SQL] varchar(max), [name] sysname, [index_name] sysname)

--Written by Percy Reyes www.percyreyes.com
declare CursorFK cursor for select object_id--, name, object_name( parent_object_id) 
from sys.foreign_keys
order by object_name( parent_object_id)

open CursorFK
fetch next from CursorFK into @ForeignKeyID
while (@@FETCH_STATUS=0)
begin
 set @StrParentColumn=''
 set @StrReferencedColumn=''
 declare CursorFKDetails cursor for
  select  fk.name ForeignKeyName, schema_name(t1.schema_id) ParentTableSchema,
  object_name(fkc.parent_object_id) ParentTable, c1.name ParentColumn,schema_name(t2.schema_id) ReferencedTableSchema,
   object_name(fkc.referenced_object_id) ReferencedTable,c2.name ReferencedColumn
  from --sys.tables t inner join 
  sys.foreign_keys fk 
  inner join sys.foreign_key_columns fkc on fk.object_id=fkc.constraint_object_id
  inner join sys.columns c1 on c1.object_id=fkc.parent_object_id and c1.column_id=fkc.parent_column_id 
  inner join sys.columns c2 on c2.object_id=fkc.referenced_object_id and c2.column_id=fkc.referenced_column_id 
  inner join sys.tables t1 on t1.object_id=fkc.parent_object_id 
  inner join sys.tables t2 on t2.object_id=fkc.referenced_object_id 
  where fk.object_id=@ForeignKeyID
 open CursorFKDetails
 fetch next from CursorFKDetails into  @ForeignKeyName, @ParentTableSchema, @ParentTableName, @ParentColumn, @ReferencedTableSchema, @ReferencedTable, @ReferencedColumn
 while (@@FETCH_STATUS=0)
 begin    
  set @StrParentColumn=@StrParentColumn + ', "' + @ParentColumn+'"'
  set @StrReferencedColumn=@StrReferencedColumn + ', "' + @ReferencedColumn+'"'
  
     fetch next from CursorFKDetails into  @ForeignKeyName, @ParentTableSchema, @ParentTableName, @ParentColumn, @ReferencedTableSchema, @ReferencedTable, @ReferencedColumn
 end
 close CursorFKDetails
 deallocate CursorFKDetails
 set @StrParentColumn=substring(@StrParentColumn,2,len(@StrParentColumn)-1)
 set @StrReferencedColumn=substring(@StrReferencedColumn,2,len(@StrReferencedColumn)-1)
 set @TSQLCreationFK='ALTER TABLE "'+@ParentTableName+'" ADD CONSTRAINT "'+@ForeignKeyName +'"'
 + ' FOREIGN KEY('+ltrim(@StrParentColumn)+') '+ 'REFERENCES "'+@ReferencedTable+'" ('+ltrim(@StrReferencedColumn)+')' + ';'
 
insert into #SQL_TEXT ([sql], [name], [index_name]) values (@TSQLCreationFK, @ParentTableName, @ParentColumn)
--print @TSQLCreationFK
fetch next from CursorFK into @ForeignKeyID 
end
close CursorFK
deallocate CursorFK

select * from #SQL_TEXT
order by [name], [index_name]