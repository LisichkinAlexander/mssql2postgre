select o.name,
	( 
		select sum(p.rows) 
		from sys.partitions AS p WITH(NOLOCK)
		where
			p.object_id = o.id
			and p.index_id IN (0,1)
	) row_count,
	(
		select count(*) 
		from sys.columns c WITH(NOLOCK) 
		join (
			select tp2.name, tp.user_type_id
			from sys.types tp WITH(NOLOCK)
			left join sys.types tp2 WITH(NOLOCK) on tp.system_type_id = tp2.system_type_id and tp2.user_type_id = tp2.system_type_id
		) tp ON c.user_type_id = tp.user_type_id
		where 
			c.object_id = o.id
	) column_count
from sysobjects o WITH(NOLOCK)
where xtype= 'U' and name not like '%$%' and name not like '[_]%'
  -- SQL Server Native Client 11.0 do not support datetimeoffset
  and id not in (
	select o.id
	from sysobjects o WITH(NOLOCK)
	join sys.columns c WITH(NOLOCK) on o.id = c.object_id
	join sys.types t WITH(NOLOCK) on c.system_type_id = t.system_type_id
	where t.name = 'datetimeoffset'
  )
  and name >= 'ZTESTGLOSS'
order by o.name
