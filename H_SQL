

select
Package_id, object_name,cdata

from "_SYS_REPO"."ACTIVE_OBJECT"
where
Package_id||object_name in (
select
Package_id||object_name
from(
select
distinct
"PackageName" as Package_id,
"CalculationView/TableFunction" as object_name
from
"DependencyObjectList_TF"('!viewPath!')
union all

select
distinct
"BaseObjectSchemaName" as Package_id,
"BaseObjectName" as object_name

from
"DependencyObjectList_TF"('!viewPath!')
where "BaseObjectType" = 'Calculation View'

)

)
