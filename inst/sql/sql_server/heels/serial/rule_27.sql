select *
into #rule27_1
from
(

  select * from @scratchDatabaseSchema@schemaDelim@tempHeelPrefix_achilles_results_derived_0
  
  union all
  
  select
    null as analysis_id,
    CAST('Condition' AS VARCHAR(255)) as stratum_1, 
    null as stratum_2,
    CAST(100.0*st.val/statistic_value AS FLOAT) as statistic_value,
    CAST('UnmappedData:byDomain:Percentage' AS VARCHAR(255)) as measure_id
  from @scratchDatabaseSchema@schemaDelim@tempHeelPrefix_achilles_results_derived_0
  cross join (select statistic_value as val from @scratchDatabaseSchema@schemaDelim@tempHeelPrefix_achilles_results_derived_0 
      where measure_id like 'UnmappedData:ach_401:GlobalRowCnt') as st
  where measure_id = 'ach_401:GlobalRowCnt'
) Q
;


select *
into #rule27_2
from
(

  select * from #rule27_1
  
  union all
  
  select
    null as analysis_id,
    CAST('Procedure' AS VARCHAR(255)) as stratum_1,
    null as stratum_2,
    CAST(100.0*st.val/statistic_value AS FLOAT) as statistic_value,
     CAST(  'UnmappedData:byDomain:Percentage' AS VARCHAR(255)) as measure_id
  from #rule27_1 A
  join (select statistic_value as val from #rule27_1 
        where measure_id = 'UnmappedData:ach_601:GlobalRowCnt') as st
    on A.statistic_value = st.val
  where measure_id ='ach_601:GlobalRowCnt'
    
) Q
;


select *
into #rule27_3
from
(

  select * from #rule27_2
  
  union all
  
  select
    null as analysis_id,
    CAST('DrugExposure' AS VARCHAR(255)) as stratum_1,
    null as stratum_2,
    CAST(100.0*st.val/statistic_value AS FLOAT) as statistic_value,
    CAST(  'UnmappedData:byDomain:Percentage' AS VARCHAR(255)) as measure_id
  from #rule27_2 A
  join (select statistic_value as val from #rule27_2 
        where measure_id = 'UnmappedData:ach_701:GlobalRowCnt') as st
      on A.statistic_value = st.val
  where measure_id ='ach_701:GlobalRowCnt'
  
) Q
;


select *
into #rule27_4
from
(

  select * from #rule27_3
  
  union all
  
  select
    null as analysis_id,
    CAST('Observation' AS VARCHAR(255)) as stratum_1, 
    null as stratum_2,
    CAST(100.0*st.val/statistic_value AS FLOAT) as statistic_value,
    CAST(  'UnmappedData:byDomain:Percentage' AS VARCHAR(255)) as measure_id
  from #rule27_3 A
  join (select statistic_value as val from #rule27_3
        where measure_id = 'UnmappedData:ach_801:GlobalRowCnt') as st
    on A.statistic_value = st.val
  where measure_id ='ach_801:GlobalRowCnt'
  
) Q
;


select *
into #rule27_5
from
(

  select * from #rule27_4
  
  union all
  
  select
    null as analysis_id,
    CAST('Measurement' AS VARCHAR(255)) as stratum_1, 
    null as stratum_2,
    CAST(100.0*st.val/statistic_value AS FLOAT) as statistic_value,
    CAST(  'UnmappedData:byDomain:Percentage' AS VARCHAR(255)) as measure_id
  from #rule27_4 A
  join (select statistic_value as val from #rule27_4
        where measure_id = 'UnmappedData:ach_1801:GlobalRowCnt') as st
    on A.statistic_value = st.val
  where measure_id ='ach_1801:GlobalRowCnt'
  
) Q
;


select * 
into @scratchDatabaseSchema@schemaDelim@tempHeelPrefix_serial_rd_@rdNewId
from
(
  select * from #rule27_5
) Q;

truncate table #rule27_1;
drop table #rule27_1;

truncate table #rule27_2;
drop table #rule27_2;

truncate table #rule27_3;
drop table #rule27_3;

truncate table #rule27_4;
drop table #rule27_4;

truncate table #rule27_5;
drop table #rule27_5;


--actual rule27
  
select *
into @scratchDatabaseSchema@schemaDelim@tempHeelPrefix_serial_hr_@hrNewId
from
(
  select * from @scratchDatabaseSchema@schemaDelim@tempHeelPrefix_achilles_heel_results_0
  
  union all
  
  SELECT 
    null as analysis_id,
    CAST(CONCAT('NOTIFICATION:Unmapped data over percentage threshold in:', 
    cast(d.stratum_1 as varchar)) AS VARCHAR(255)) as ACHILLES_HEEL_warning,
    27 as rule_id,
    null as record_count
  FROM @scratchDatabaseSchema@schemaDelim@tempHeelPrefix_serial_rd_@rdNewId
  where d.measure_id = 'UnmappedData:byDomain:Percentage'
  and d.statistic_value > 0.1  --thresholds will be decided in the ongoing DQ-Study2
) Q
;


--end of rule27