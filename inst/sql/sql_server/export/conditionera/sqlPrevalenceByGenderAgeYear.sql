select c1.concept_id as concept_id,  
	cast(cast(num.stratum_4 as int)*10 as varchar) + '-' + cast((cast(num.stratum_4 as int)+1)*10-1 as varchar) as trellis_name,
	c2.concept_name as series_name, 
	num.stratum_2 as x_calendar_year,
	ROUND(1000*(1.0*num.count_value/denom.count_value),5) as y_prevalence_1000pp
from 
	(select * from ACHILLES_results where analysis_id = 1004) num
	inner join
	(select * from ACHILLES_results where analysis_id = 116) denom
	on num.stratum_2 = denom.stratum_1  
	and num.stratum_3 = denom.stratum_2 
	and num.stratum_4 = denom.stratum_3 
	inner join @cdmSchema.dbo.concept c1 on CAST(num.stratum_1 AS INT) = c1.concept_id
	inner join @cdmSchema.dbo.concept c2 on CAST(num.stratum_3 AS INT) = c2.concept_id
  where c2.concept_id in (8507, 8532)
ORDER BY c1.concept_id, CAST(num.stratum_2 as INT)