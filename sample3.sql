--more than 10 tables from various data sources uniformed and joined into one model for summarized weekly reporting

with hours_offered as (

select 
reporting_week_ending, 
sum(case when doctor_type = 'Dentist' then booked_appointments end)/4::float as hours_booked_dentist, 
sum(case when doctor_type = 'Dentist' and doctor_accepting_new_patients = 'available' then available_appointments end)/4::float as hours_unused_dentist,
sum(case when doctor_type = 'Dentist' and doctor_accepting_new_patients = 'available' then total_appointments end)/4::float as new_pt_hours_offered_dentist, 
sum(case when doctor_type = 'Dentist' then total_appointments end)/4::float as all_hours_offered_dentist, 
(sum(case when doctor_type = 'Dentist' then booked_appointments end)/4::float)/(sum(case when doctor_type = 'Dentist' then total_appointments end )/4::float) as utilization_dentist_pct,
sum(case when doctor_type = 'Pediatricians' then booked_appointments end)/4::float as hours_booked_pediatricians, 
sum(case when doctor_type = 'Pediatricians' and doctor_accepting_new_patients = 'available' then available_appointments end)/4::float as hours_unused_pediatricians,
sum(case when doctor_type = 'Pediatricians' and doctor_accepting_new_patients = 'available' then total_appointments end)/4::float as new_pt_hours_offered_pediatricians, 
sum(case when doctor_type = 'Pediatricians' then total_appointments end)/4::float as all_hours_offered_pediatricians, 
(sum(case when doctor_type = 'Pediatricians' then booked_appointments end)/4::float)/(sum(case when doctor_type = 'Pediatricians' then total_appointments end )/4::float) as utilization_pediatricians_pct
--alias table
from sample.table1
group by 1)

,avg_hrs_to_nxt_available as (

select
date_trunc('week',schedule_viewed_event_date_utc) + 6 as reporting_week_ending, 
avg(case when subscriber_plan_type_1 = 'Credit' and booking_type = 'Initial dental' then hours_from_schedule_viewed_to_next_available end) as credit_rx_initial_avg_hrs_to_nxt_available, 
avg(case when subscriber_plan_type_1 = 'Credit' and booking_type = 'Initial surgery' then hours_from_schedule_viewed_to_next_available end) as credit_surgery_initial_avg_hrs_to_nxt_available,
avg(case when subscriber_plan_type_1 = 'Employer' and booking_type = 'Initial dental' then hours_from_schedule_viewed_to_next_available end) as employer_rx_initial_avg_hrs_to_nxt_available, 
avg(case when subscriber_plan_type_1 = 'Employer' and booking_type = 'Initial surgery' then hours_from_schedule_viewed_to_next_available end) as employer_surgery_initial_avg_hrs_to_nxt_available
--alias table
from sample.table2
group by 1)

,states_avg_hrs_to_nxt_available_greater_than_72_hrs as (

select
date_trunc('week',schedule_viewed_event_date_utc) + 6 as reporting_week_ending, 
booking_state,
avg(case when subscriber_plan_type_1 = 'Credit' and booking_type = 'Initial dental' then hours_from_schedule_viewed_to_next_available end) as credit_rx_initial_avg_hrs_to_nxt_available, 
avg(case when subscriber_plan_type_1 = 'Credit' and booking_type = 'Initial surgery' then hours_from_schedule_viewed_to_next_available end) as credit_surgery_initial_avg_hrs_to_nxt_available,
avg(case when subscriber_plan_type_1 = 'Employer' and booking_type = 'Initial dental' then hours_from_schedule_viewed_to_next_available end) as employer_rx_initial_avg_hrs_to_nxt_available, 
avg(case when subscriber_plan_type_1 = 'Employer' and booking_type = 'Initial surgery' then hours_from_schedule_viewed_to_next_available end) as employer_surgery_initial_avg_hrs_to_nxt_available
--alias table
from sample.table3
group by 1,2)

,states_avg_hrs_to_nxt_available_greater_than_72_hrs_count as (

select 
reporting_week_ending, 
count(case when Credit_rx_initial_avg_hrs_to_nxt_available > 72 then booking_state end) as Credit_rx_greater_than_72_hrs_to_next_available_count,
count(case when Credit_surgery_initial_avg_hrs_to_nxt_available > 72 then booking_state end) as Credit_surgery_greater_than_72_hrs_to_next_available_count,
count(case when employer_rx_initial_avg_hrs_to_nxt_available > 72 then booking_state end) as employer_rx_greater_than_72_hrs_to_next_available_count,
count(case when employer_surgery_initial_avg_hrs_to_nxt_available > 72 then booking_state end) as employer_surgery_greater_than_72_hrs_to_next_available_count
from states_avg_hrs_to_nxt_available_greater_than_72_hrs
group by 1)

,no_show_rates as (

select 
date_trunc('week',booking_start_date_utc)::date + 6 as reporting_week_ending,
(sum(case when status = 'no-showed' and doctor_type = 'Pediatricians' then 1 else 0 end)) / (sum(case when doctor_type = 'Pediatricians' then 1 else null end))::float as pediatricians_no_show_rate,
(sum(case when status = 'no-showed' and doctor_type = 'Pediatricians' and booking_sequence_type = 'initial' then 1 else 0 end)) / (sum(case when doctor_type = 'Pediatricians' and booking_sequence_type = 'initial' then 1 else null end))::float as Pediatricians_initial_no_show_rate,
(sum(case when status = 'no-showed' and doctor_type = 'Dentist' then 1 else 0 end)) / (sum(case when doctor_type = 'Dentist' then 1 else null end))::float as dentist_no_show_rate,
(sum(case when status = 'no-showed' and doctor_type = 'Dentist' and booking_sequence_type = 'initial' then 1 else 0 end)) / (sum(case when doctor_type = 'Dentist' then 1 else null end))::float as Dentist_initial_no_show_rate
from sample.table4
group by 1)

,network_counts as (

select  
reporting_date_week as reporting_week_ending, 
sum(case when doctor_type = 'Pediatricians' then active_doctors end) as total_pediatricianss, 
sum(case when specialty_final = 'Specialist' then active_doctors end) as total_specialists, 
sum(case when specialty_final = 'NP' then active_doctors end) as total_np,
sum(case when specialty_final = 'PCP' then active_doctors end) as other_non_psych,
(max(case when dental_total_hours_offered != 0 then dental_total_hours_offered end))/sum(case when doctor_type = 'Pediatricians' then active_doctors else null end) as avg_time_offered_per_Pediatricians,
sum(case when doctor_type = 'Dentist' then active_doctors end) as total_Dentist, 
(max(case when surgery_total_hours_offered != 0 then surgery_total_hours_offered end))/sum(case when doctor_type = 'Dentist' then active_doctors else null end) as avg_time_offered_per_Dentist, 
sum(case when doctor_type = 'Pediatricians' then active_doctors end) + sum(case when doctor_type = 'Dentist' then active_doctors end) as network_count
from sample.table5
group by 1)

,member_counts as (

select 
reporting_date_week + 6 as reporting_week_ending, 
sum(active_subscribers) - sum(active_and_employer_disengaged_subscribers) as active_subscribers_net_disengaged, 
sum(case when subscriber_last_plan_type in ('Dental','Dental Maintenance') then active_subscribers end) - sum(case when subscriber_last_plan_type in ('Dental','Dental Maintenance') then active_and_employer_disengaged_subscribers end) as active_subscribers_net_disengaged_dental, 
sum(case when subscriber_last_plan_type = 'Surgery' then active_subscribers end) - sum(case when subscriber_last_plan_type = 'Surgery' then active_and_employer_disengaged_subscribers end) as active_subscribers_net_disengaged_surgery, 
sum(case when subscriber_last_plan_type = 'Dental + Surgery' then active_subscribers end) - sum(case when subscriber_last_plan_type = 'Dental + Surgery' then active_and_employer_disengaged_subscribers end) as active_subscribers_net_disengaged_dental_surgery
from sample.table
group by 1)

--main table
select 
a.reporting_week_ending::date,
a.hours_booked_dentist, 
a.hours_unused_dentist,
a.new_pt_hours_offered_dentist,
a.all_hours_offered_dentist,
a.utilization_dentist_pct,
a.hours_booked_pediatricians, 
a.hours_unused_pediatricians, 
a.new_pt_hours_offered_pediatricians, 
a.all_hours_offered_pediatricians,
a.utilization_pediatricians_pct, 
b.credit_rx_initial_avg_hrs_to_nxt_available, 
b.credit_surgery_initial_avg_hrs_to_nxt_available,
b.employer_rx_initial_avg_hrs_to_nxt_available, 
b.employer_surgery_initial_avg_hrs_to_nxt_available, 
c.Credit_rx_greater_than_72_hrs_to_next_available_count, 
c.Credit_surgery_greater_than_72_hrs_to_next_available_count, 
c.employer_rx_greater_than_72_hrs_to_next_available_count, 
c.employer_surgery_greater_than_72_hrs_to_next_available_count, 
d.pediatricians_no_show_rate, 
d.pediatricians_initial_no_show_rate,
d.dentist_no_show_rate,
d.dentist_initial_no_show_rate,
e.total_pediatricianss,
e.total_specialists,
e.total_np,
e.other_non_psych,
e.avg_time_offered_per_pediatricians,
e.total_dentist,
e.avg_time_offered_per_dentist, 
e.network_count, 
f.active_subscribers_net_disengaged, 
f.active_subscribers_net_disengaged_dental, 
f.active_subscribers_net_disengaged_surgery, 
f.active_subscribers_net_disengaged_dental_surgery
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