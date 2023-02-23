--registros por mes
select COUNT(*),EXTRACT(YEAR from recorded_at),EXTRACT(MONTH from recorded_at)
from locations where activity_type='on_bicycle' 
GROUP by EXTRACT(YEAR from recorded_at),EXTRACT(MONTH from recorded_at)
order by EXTRACT(YEAR from recorded_at),EXTRACT(MONTH from recorded_at)

--medios de transporte
select distinct activity_type from locations

--velocidad media por medio de transporte
select avg(speed) avg_speed, max(speed) max_speed, activity_type 
from locations 
group by activity_type 
order by avg_speed