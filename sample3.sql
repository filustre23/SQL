with hours_offered as (

select 
reporting_week_ending, 
sum(case when practitioner_type = 'Therapist' then booked_slots end)/4::float as hours_booked_therapist, 
sum(case when practitioner_type = 'Therapist' and practitioner_accepting_new_patients = 'available' then available_slots end)/4::float as hours_unused_therapist,
sum(case when practitioner_type = 'Therapist' and practitioner_accepting_new_patients = 'available' then total_slots end)/4::float as new_pt_hours_offered_therapist, 
sum(case when practitioner_type = 'Therapist' then total_slots end)/4::float as all_hours_offered_therapist, 
(sum(case when practitioner_type = 'Therapist' then booked_slots end)/4::float)/(sum(case when practitioner_type = 'Therapist' then total_slots end )/4::float) as utilization_therapist_pct,
sum(case when practitioner_type = 'Provider' then booked_slots end)/4::float as hours_booked_provider, 
sum(case when practitioner_type = 'Provider' and practitioner_accepting_new_patients = 'available' then available_slots end)/4::float as hours_unused_provider,
sum(case when practitioner_type = 'Provider' and practitioner_accepting_new_patients = 'available' then total_slots end)/4::float as new_pt_hours_offered_provider, 
sum(case when practitioner_type = 'Provider' then total_slots end)/4::float as all_hours_offered_provider, 
(sum(case when practitioner_type = 'Provider' then booked_slots end)/4::float)/(sum(case when practitioner_type = 'Provider' then total_slots end )/4::float) as utilization_provider_pct
--alias table
from sample.table1
group by 1)

,avg_hrs_to_nxt_available as (

select
date_trunc('week',schedule_viewed_event_date_utc) + 6 as reporting_week_ending, 
avg(case when subscriber_plan_type_1 = 'Cash' and booking_type = 'Initial rx' then hours_from_schedule_viewed_to_next_available end) as cash_rx_initial_avg_hrs_to_nxt_available, 
avg(case when subscriber_plan_type_1 = 'Cash' and booking_type = 'Initial therapy' then hours_from_schedule_viewed_to_next_available end) as cash_therapy_initial_avg_hrs_to_nxt_available,
avg(case when subscriber_plan_type_1 = 'Insurance' and booking_type = 'Initial rx' then hours_from_schedule_viewed_to_next_available end) as insurance_rx_initial_avg_hrs_to_nxt_available, 
avg(case when subscriber_plan_type_1 = 'Insurance' and booking_type = 'Initial therapy' then hours_from_schedule_viewed_to_next_available end) as insurance_therapy_initial_avg_hrs_to_nxt_available
--alias table
from sample.table2
group by 1)

,states_avg_hrs_to_nxt_available_greater_than_72_hrs as (

select
date_trunc('week',schedule_viewed_event_date_utc) + 6 as reporting_week_ending, 
booking_state,
avg(case when subscriber_plan_type_1 = 'Cash' and booking_type = 'Initial rx' then hours_from_schedule_viewed_to_next_available end) as cash_rx_initial_avg_hrs_to_nxt_available, 
avg(case when subscriber_plan_type_1 = 'Cash' and booking_type = 'Initial therapy' then hours_from_schedule_viewed_to_next_available end) as cash_therapy_initial_avg_hrs_to_nxt_available,
avg(case when subscriber_plan_type_1 = 'Insurance' and booking_type = 'Initial rx' then hours_from_schedule_viewed_to_next_available end) as insurance_rx_initial_avg_hrs_to_nxt_available, 
avg(case when subscriber_plan_type_1 = 'Insurance' and booking_type = 'Initial therapy' then hours_from_schedule_viewed_to_next_available end) as insurance_therapy_initial_avg_hrs_to_nxt_available
--alias table
from sample.table3
group by 1,2)

,states_avg_hrs_to_nxt_available_greater_than_72_hrs_count as (

select 
reporting_week_ending, 
count(case when cash_rx_initial_avg_hrs_to_nxt_available > 72 then booking_state end) as cash_rx_greater_than_72_hrs_to_next_available_count,
count(case when cash_therapy_initial_avg_hrs_to_nxt_available > 72 then booking_state end) as cash_therapy_greater_than_72_hrs_to_next_available_count,
count(case when insurance_rx_initial_avg_hrs_to_nxt_available > 72 then booking_state end) as insurance_rx_greater_than_72_hrs_to_next_available_count,
count(case when insurance_therapy_initial_avg_hrs_to_nxt_available > 72 then booking_state end) as insurance_therapy_greater_than_72_hrs_to_next_available_count
from states_avg_hrs_to_nxt_available_greater_than_72_hrs
group by 1)

,no_show_rates as (

select 
date_trunc('week',booking_start_date_utc)::date + 6 as reporting_week_ending,
(sum(case when status = 'no-showed' and practitioner_type = 'Provider' then 1 else 0 end)) / (sum(case when practitioner_type = 'Provider' then 1 else null end))::float as provider_no_show_rate,
(sum(case when status = 'no-showed' and practitioner_type = 'Provider' and booking_sequence_type = 'initial' then 1 else 0 end)) / (sum(case when practitioner_type = 'Provider' and booking_sequence_type = 'initial' then 1 else null end))::float as provider_initial_no_show_rate,
(sum(case when status = 'no-showed' and practitioner_type = 'Therapist' then 1 else 0 end)) / (sum(case when practitioner_type = 'Therapist' then 1 else null end))::float as therapist_no_show_rate,
(sum(case when status = 'no-showed' and practitioner_type = 'Therapist' and booking_sequence_type = 'initial' then 1 else 0 end)) / (sum(case when practitioner_type = 'Therapist' then 1 else null end))::float as therapist_initial_no_show_rate
from sample.table4
group by 1)

,network_counts as (

select  
reporting_date_week as reporting_week_ending, 
sum(case when practitioner_type = 'Provider' then active_practitioners end) as total_providers, 
sum(case when specialty_final = 'Psychiatrist' then active_practitioners end) as total_psychiatrists, 
sum(case when specialty_final = 'PMHNP' then active_practitioners end) as total_psychiatristic_np,
sum(case when specialty_final = 'PCP' then active_practitioners end) as other_non_psych,
(max(case when medication_total_hours_offered != 0 then medication_total_hours_offered end))/sum(case when practitioner_type = 'Provider' then active_practitioners else null end) as avg_time_offered_per_provider,
sum(case when practitioner_type = 'Therapist' then active_practitioners end) as total_therapist, 
(max(case when therapy_total_hours_offered != 0 then therapy_total_hours_offered end))/sum(case when practitioner_type = 'Therapist' then active_practitioners else null end) as avg_time_offered_per_therapist, 
sum(case when practitioner_type = 'Provider' then active_practitioners end) + sum(case when practitioner_type = 'Therapist' then active_practitioners end) as network_count
from sample.table5
group by 1)

,member_counts as (

select 
reporting_date_week + 6 as reporting_week_ending, 
sum(active_subscribers) - sum(active_and_insurance_disengaged_subscribers) as active_subscribers_net_disengaged, 
sum(case when subscriber_last_plan_type in ('Medication','Medication Maintenance') then active_subscribers end) - sum(case when subscriber_last_plan_type in ('Medication','Medication Maintenance') then active_and_insurance_disengaged_subscribers end) as active_subscribers_net_disengaged_medication, 
sum(case when subscriber_last_plan_type = 'Therapy' then active_subscribers end) - sum(case when subscriber_last_plan_type = 'Therapy' then active_and_insurance_disengaged_subscribers end) as active_subscribers_net_disengaged_therapy, 
sum(case when subscriber_last_plan_type = 'Medication + Therapy' then active_subscribers end) - sum(case when subscriber_last_plan_type = 'Medication + Therapy' then active_and_insurance_disengaged_subscribers end) as active_subscribers_net_disengaged_medication_therapy
from sample.table
group by 1)

--main table
select 
a.reporting_week_ending::date,
a.hours_booked_therapist, 
a.hours_unused_therapist,
a.new_pt_hours_offered_therapist,
a.all_hours_offered_therapist,
a.utilization_therapist_pct,
a.hours_booked_provider, 
a.hours_unused_provider, 
a.new_pt_hours_offered_provider, 
a.all_hours_offered_provider,
a.utilization_provider_pct, 
b.cash_rx_initial_avg_hrs_to_nxt_available, 
b.cash_therapy_initial_avg_hrs_to_nxt_available,
b.insurance_rx_initial_avg_hrs_to_nxt_available, 
b.insurance_therapy_initial_avg_hrs_to_nxt_available, 
c.cash_rx_greater_than_72_hrs_to_next_available_count, 
c.cash_therapy_greater_than_72_hrs_to_next_available_count, 
c.insurance_rx_greater_than_72_hrs_to_next_available_count, 
c.insurance_therapy_greater_than_72_hrs_to_next_available_count, 
d.provider_no_show_rate, 
d.provider_initial_no_show_rate,
d.therapist_no_show_rate,
d.therapist_initial_no_show_rate,
e.total_providers,
e.total_psychiatrists,
e.total_psychiatristic_np,
e.other_non_psych,
e.avg_time_offered_per_provider,
e.total_therapist,
e.avg_time_offered_per_therapist, 
e.network_count, 
f.active_subscribers_net_disengaged, 
f.active_subscribers_net_disengaged_medication, 
f.active_subscribers_net_disengaged_therapy, 
f.active_subscribers_net_disengaged_medication_therapy
from hours_offered a
left join avg_hrs_to_nxt_available b on 
	(a.reporting_week_ending = b.reporting_week_ending)
left join states_avg_hrs_to_nxt_available_greater_than_72_hrs_count c on 
	(a.reporting_week_ending = c.reporting_week_ending)
left join no_show_rates d on 
	(a.reporting_week_ending = d.reporting_week_ending)
left join network_counts e on 
	(a.reporting_week_ending = e.reporting_week_ending)
left join member_counts f on 
	(a.reporting_week_ending = f.reporting_week_ending)
where a.reporting_week_ending::date >= '2021-01-01'
order by reporting_week_ending desc