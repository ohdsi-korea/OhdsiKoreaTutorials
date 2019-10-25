CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4193704,376065,4008576,443732,443731,443733,443729,443412,4024659,443735,435216,4058243,443730,377821,200687,443734,439770,201826,201254,40480000,373999,318712,43531010,4063042,321822,4096042,4099652,4096671,4096670,4224419,4096041,4030061,4062687,4224879,4034964,36715571,36717156,36713275,4143857,4140466,43531588,45769891,45769888,4060085,45763585,45763582,37018912,45773688,43531578,45773576,43531559,45769901,43531566,45771075,43531653,45769902,43531577,45769903,43531562,45769837,4221933,37016163,45769894,37016350,201820,4322638,4143529,4245270,4240589,4178452,4178790,4144583,43531011,43531642,4192852,45757077,4237068,443012,192691,45757129,194700,4079850,45766050,4062685,4062686,4235410,4136889,45757674,45757474,4096666,44793113,43531645,43531019,43531020,37016767,37016768,4225656,4221495,43531616,45769832,43531564,43531565,43531563,43530689,4227210,4226121,43531608,45757074,43531597,4202383,37016353,45757280,42689695,45769904,45769906,4177050,44810261,4221344,4223463,45766963,45757507,43530690,45769892,45769890,4226354,4223303,4222876,37017430,37017429,37018728,37018765,45757124,4326434,4263902,37396524,46274096,45772019,45771068,45769889,44809658,37016348,37016349,40480031,45757432,45770880,443592,4226238,37016355,4215719,45757393,45757392,45771067,45771064,45757447,45757446,45757445,45757444,36714116,45769876,45757363,45772060,4226798,4228112,45757362,4130165,4047906,4102018,45769875,4130162,4130166,36717215,45771072,4224254,4228443,4145827,44787902,4129379,45770830,4327944,4166381,44792134,45757535,45769905,37016356,37016179,45757435,43530660,43531651,37016358,37016180,45770881,4225055,4222415,4224709,4034961,193323,37017221,4027121,45769829,45769828,45769830,45757450,45770883,45757255,37016354,45768456,4223734,45763583,43530656,37016357,45769834,45769836,36716258,36713094,37018566,45757278,43531641,4222687,4221487,4222553,4223739,4030066,37017431,37017432,45757789,40482883,44793114,45757079,43531007,43531008,4063043,4219466,4146514,4242853,4144221,4182243,4220981,4305491,43531009,4129519,43530685,45763584,45770831,4212631,45757604,45757499,36716853,45757266,45770928,45757073,45757075,45770986,45771533,45769872,195771,4034960,45773567,45769833,45769835,4143689,4142579,4082346,45770832,4099653,45769873,45773064,4129378,4099215,4152858,4096668,201531,4151281,4295011,4099214,4230254,4304377,4321756,4196141,4099217,201530,4099216,4198296,4200875,4099651,45766051,45766052,36712686,45757277,45770902,36712687,36712670,45757449)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (45757449)
  and c.invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4034964,36715571,36717156,4143857,4140466,43531588,45769891,45769888,4060085,36713275,45763585,45763582,37018912,45773688,43531578,45773576,43531559,45769901,43531566,45771075,43531653,45769902,43531577,45769903,43531562,45769837,443735,4221933,37016163,45769894,37016350,201820,4322638,4143529,4245270,4240589,4178452,4178790,4144583,43531011,43531642,4192852,45757077,4237068,443012,192691,4058243,45757129,194700,4079850,45766050,4062685,4062686,4235410,4136889,45757674,45757474,4096666,44793113,4008576,43531645,43531019,43531020,37016767,37016768,4225656,4221495,43531616,45769832,43531564,43531565,43531563,373999,443733,43530689,4227210,4226121,36712686,36712687,43531608,45757074,43531597,435216,443732,4202383,37016353,45757280,42689695,45769904,45769906,4177050,44810261,4221344,4223463,45766963,45757507,43530690,45769892,45769890,4226354,4223303,4222876,37017430,37017429,37018728,4024659,37018765,45757124,4326434,4263902,37396524,46274096,45772019,45771068,45769889,44809658,37016348,37016349,40480031,45757432,45770880,443592,4226238,37016355,4215719,45757393,45757392,45771067,45771064,45757447,45757446,45757445,45757444,45769876,45757363,45772060,36714116,4226798,4228112,45757362,4130165,4047906,4102018,45769875,4130162,4130166,36717215,45771072,439770,443734,4224254,4228443,4145827,44787902,4129379,45770830,4327944,4030061,4096041,4099652,4096671,4096670,4096042,4166381,44792134,45757535,45769905,37016356,37016179,45757435,43530660,43531651,37016358,37016180,45770881,4225055,4222415,40480000,4224709,4034961,193323,37017221,4027121,45769829,45769828,443730,376065,4224879,377821,45769830,45757450,45770883,45757255,37016354,45768456,4223734,45763583,43530656,37016357,4224419,45769834,45769836,36716258,36713094,321822,318712,443729,37018566,45757278,43531641,4222687,4221487,4222553,4223739,4030066,37017431,37017432,45757789,40482883,44793114,45757079,43531007,4062687,4063042,43531008,4063043,43531010,4219466,4146514,4242853,4144221,4182243,4220981,4305491,43531009,4129519,43530685,45763584,45770831,4212631,45757604,45757499,200687,443731,36716853,45757266,45770928,45757073,45757075,45770986,45771533,45769872,195771,4034960,45773567,45769833,45769835,36712670,4143689,4142579,4082346,45770832,4099653,45769873,45773064,4129378,201254,4099215,4152858,4096668,201531,4151281,4295011,4099214,443412,201826,4230254,4304377,4321756,4196141,4099217,201530,4099216,4198296,4200875,4099651,4193704,45766051,45766052,45757277,45770902,45757449)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4034964,36715571,36717156,4143857,4140466,43531588,45769891,45769888,4060085,36713275,45763585,45763582,37018912,45773688,43531578,45773576,43531559,45769901,43531566,45771075,43531653,45769902,43531577,45769903,43531562,45769837,443735,4221933,37016163,45769894,37016350,201820,4322638,4143529,4245270,4240589,4178452,4178790,4144583,43531011,43531642,4192852,45757077,4237068,443012,192691,4058243,45757129,194700,4079850,45766050,4062685,4062686,4235410,4136889,45757674,45757474,4096666,44793113,4008576,43531645,43531019,43531020,37016767,37016768,4225656,4221495,43531616,45769832,43531564,43531565,43531563,373999,443733,43530689,4227210,4226121,36712686,36712687,43531608,45757074,43531597,435216,443732,4202383,37016353,45757280,42689695,45769904,45769906,4177050,44810261,4221344,4223463,45766963,45757507,43530690,45769892,45769890,4226354,4223303,4222876,37017430,37017429,37018728,4024659,37018765,45757124,4326434,4263902,37396524,46274096,45772019,45771068,45769889,44809658,37016348,37016349,40480031,45757432,45770880,443592,4226238,37016355,4215719,45757393,45757392,45771067,45771064,45757447,45757446,45757445,45757444,45769876,45757363,45772060,36714116,4226798,4228112,45757362,4130165,4047906,4102018,45769875,4130162,4130166,36717215,45771072,439770,443734,4224254,4228443,4145827,44787902,4129379,45770830,4327944,4030061,4096041,4099652,4096671,4096670,4096042,4166381,44792134,45757535,45769905,37016356,37016179,45757435,43530660,43531651,37016358,37016180,45770881,4225055,4222415,40480000,4224709,4034961,193323,37017221,4027121,45769829,45769828,443730,376065,4224879,377821,45769830,45757450,45770883,45757255,37016354,45768456,4223734,45763583,43530656,37016357,4224419,45769834,45769836,36716258,36713094,321822,318712,443729,37018566,45757278,43531641,4222687,4221487,4222553,4223739,4030066,37017431,37017432,45757789,40482883,44793114,45757079,43531007,4062687,4063042,43531008,4063043,43531010,4219466,4146514,4242853,4144221,4182243,4220981,4305491,43531009,4129519,43530685,45763584,45770831,4212631,45757604,45757499,200687,443731,36716853,45757266,45770928,45757073,45757075,45770986,45771533,45769872,195771,4034960,45773567,45769833,45769835,36712670,4143689,4142579,4082346,45770832,4099653,45769873,45773064,4129378,201254,4099215,4152858,4096668,201531,4151281,4295011,4099214,443412,201826,4230254,4304377,4321756,4196141,4099217,201530,4099216,4198296,4200875,4099651,4193704,45766051,45766052,45757277,45770902,45757449)
  and c.invalid_reason is null

) I
) C;


with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date, row_number() OVER (PARTITION BY E.person_id ORDER BY E.start_date ASC) ordinal, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date, C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 1))
) C


-- End Condition Occurrence Criteria

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P

-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE

;

--- Inclusion Rule Inserts

create table #inclusion_events (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups
{0 != 0}?{
  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),0)-1)
}
)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results

;

-- date offset strategy

select event_id, person_id, 
  case when DATEADD(day,0,start_date) > start_date then DATEADD(day,0,start_date) else start_date end as end_date
INTO #strategy_ends
from #included_events;


-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- End Date Strategy
SELECT event_id, person_id, end_date from #strategy_ends

),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(	
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows
		
			UNION ALL
		

			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS era_end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date 
FROM #final_cohort CO
;

{0 != 0}?{
-- Find the event that is the 'best match' per person.  
-- the 'best match' is defiend as the event that satisfies the most inclulsion rules.
-- ties are solved by choosign the event that matches the earliest inclusion rule, and then earliest.

select q.person_id, q.event_id
into #best_events
from #qualified_events Q
join (
	SELECT R.person_id, R.event_id, ROW_NUMBER() OVER (PARTITION BY R.person_id ORDER BY R.rule_count DESC,R.min_rule_id ASC, R.start_date ASC) AS rank_value
	FROM (
		SELECT Q.person_id, Q.event_id, COALESCE(COUNT(DISTINCT I.inclusion_rule_id), 0) AS rule_count, COALESCE(MIN(I.inclusion_rule_id), 0) AS min_rule_id, Q.start_date
		FROM #qualified_events Q
		LEFT JOIN #inclusion_events I ON q.person_id = i.person_id AND q.event_id = i.event_id
		GROUP BY Q.person_id, Q.event_id, Q.start_date
	) R
) ranked on Q.person_id = ranked.person_id and Q.event_id = ranked.event_id
WHERE ranked.rank_value = 1
;

-- modes of generation: (the same tables store the results for the different modes, identified by the mode_id column)
-- 0: all events
-- 1: best event


-- BEGIN: Inclusion Impact Analysis - event
-- calculte matching group counts
delete from @results_database_schema.cohort_inclusion_result where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
select @target_cohort_id as cohort_definition_id, inclusion_rule_mask, count_big(*) as person_count, 0 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from #qualified_events Q
  LEFT JOIN #inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from @results_database_schema.cohort_inclusion_stats where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select ir.cohort_definition_id, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 0 as mode_id
from @results_database_schema.cohort_inclusion ir
left join
(
  select i.inclusion_rule_id, count_big(i.event_id) as person_count
  from #qualified_events Q
  JOIN #inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from @results_database_schema.cohort_inclusion where cohort_definition_id = @target_cohort_id) RuleTotal
CROSS JOIN (select count_big(event_id) as total from #qualified_events) EventTotal
LEFT JOIN @results_database_schema.cohort_inclusion_result SR on SR.mode_id = 0 AND SR.cohort_definition_id = @target_cohort_id AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule' 
WHERE ir.cohort_definition_id = @target_cohort_id
;

-- calculate totals
delete from @results_database_schema.cohort_summary_stats where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select @target_cohort_id as cohort_definition_id, PC.total as person_count, coalesce(FC.total, 0) as final_count, 0 as mode_id
FROM
(select count_big(event_id) as total from #qualified_events) PC,
(select sum(sr.person_count) as total
  from @results_database_schema.cohort_inclusion_result sr
  CROSS JOIN (select count(*) as total_rules from @results_database_schema.cohort_inclusion where cohort_definition_id = @target_cohort_id) RuleTotal
  where sr.mode_id = 0 and sr.cohort_definition_id = @target_cohort_id and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - event

-- BEGIN: Inclusion Impact Analysis - person
-- calculte matching group counts
delete from @results_database_schema.cohort_inclusion_result where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
select @target_cohort_id as cohort_definition_id, inclusion_rule_mask, count_big(*) as person_count, 1 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from #best_events Q
  LEFT JOIN #inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from @results_database_schema.cohort_inclusion_stats where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select ir.cohort_definition_id, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 1 as mode_id
from @results_database_schema.cohort_inclusion ir
left join
(
  select i.inclusion_rule_id, count_big(i.event_id) as person_count
  from #best_events Q
  JOIN #inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from @results_database_schema.cohort_inclusion where cohort_definition_id = @target_cohort_id) RuleTotal
CROSS JOIN (select count_big(event_id) as total from #best_events) EventTotal
LEFT JOIN @results_database_schema.cohort_inclusion_result SR on SR.mode_id = 1 AND SR.cohort_definition_id = @target_cohort_id AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule' 
WHERE ir.cohort_definition_id = @target_cohort_id
;

-- calculate totals
delete from @results_database_schema.cohort_summary_stats where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select @target_cohort_id as cohort_definition_id, PC.total as person_count, coalesce(FC.total, 0) as final_count, 1 as mode_id
FROM
(select count_big(event_id) as total from #best_events) PC,
(select sum(sr.person_count) as total
  from @results_database_schema.cohort_inclusion_result sr
  CROSS JOIN (select count(*) as total_rules from @results_database_schema.cohort_inclusion where cohort_definition_id = @target_cohort_id) RuleTotal
  where sr.mode_id = 1 and sr.cohort_definition_id = @target_cohort_id and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - person

-- BEGIN: Censored Stats

-- END: Censored Stats

TRUNCATE TABLE #best_events;
DROP TABLE #best_events;

}

TRUNCATE TABLE #strategy_ends;
DROP TABLE #strategy_ends;


TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
