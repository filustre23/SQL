with doctor_market_share_bookings_offerings as

(select 
reporting_week_ending::date,
reporting_week_ending_table_max::date,
datediff(day,reporting_week_ending::date,(select max(reporting_week_ending_table_max::date) reporting_week_ending_table_max from sample.table)) as days_from_reporting_week_to_max_week,
doctor_user_id,
doctor_id,
doctor_type,
full_name, 
reporting_state,
case when doctor_type = 'Pediatricians' then net_booked_appointments_medication_p else net_booked_appointments_therapy_p end as net_booked_appointments_p,
case when doctor_type = 'Pediatricians' then net_booked_appointments_medication else net_booked_appointments_therapy end as net_booked_appointments,
case when doctor_type = 'Pediatricians' then net_initial_booked_appointments_medication_p else net_initial_booked_appointments_therapy_p end as net_initial_booked_appointments_p,
case when doctor_type = 'Pediatricians' then net_initial_booked_appointments_medication else net_initial_booked_appointments_therapy end as net_initial_booked_appointments,
case when doctor_type = 'Pediatricians' then net_total_appointments_medication_p else net_total_appointments_therapy_p end as net_total_appointments_offered_p,
case when doctor_type = 'Pediatricians' then net_total_appointments_medication else net_total_appointments_therapy end as net_total_appointments_offered
--aliased table name
from sample.table)

,r7_appointments as 

(select 
doctor_type, 
sum(net_booked_appointments) as r7_net_booked_appointments, 
sum(net_initial_booked_appointments) as r7_net_initial_booked_appointments, 
sum(net_total_appointments_offered) as r7_net_total_appointments_offered
from (select
       reporting_state,
       doctor_type,
       max(net_booked_appointments) as net_booked_appointments,
       max(net_initial_booked_appointments) as net_initial_booked_appointments,
       sum(net_total_appointments_offered_p) as net_total_appointments_offered
       from doctor_market_share_bookings_offerings
       where days_from_reporting_week_to_max_week <= 6
       group by 1,2)
group by 1)

,r7_market_share_bookings_offerings as 

(select 
doctor_user_id, 
doctor_id,
full_name, 
doctor_type,
count(distinct reporting_state) as r7_reporting_states_count_p, 
sum(net_booked_appointments_p) as r7_net_booked_appointments_p, 
sum(net_initial_booked_appointments_p) as r7_net_initial_booked_appointments_p, 
sum(net_total_appointments_offered_p) as r7_net_total_appointments_offered_p
from doctor_market_share_bookings_offerings 
where days_from_reporting_week_to_max_week <= 6
group by 1,2,3,4)

,r28_appointments as 

(select 
doctor_type, 
sum(net_booked_appointments) as r28_net_booked_appointments, 
sum(net_initial_booked_appointments) as r28_net_initial_booked_appointments, 
sum(net_total_appointments_offered) as r28_net_total_appointments_offered
from (select
       reporting_week_ending,
       reporting_state,
       doctor_type,
       max(net_booked_appointments) as net_booked_appointments,
       max(net_initial_booked_appointments) as net_initial_booked_appointments,
       sum(net_total_appointments_offered_p) as net_total_appointments_offered
       from doctor_market_share_bookings_offerings
       where days_from_reporting_week_to_max_week <= 28
       group by 1,2,3)
group by 1)

,r28_market_share_bookings_offerings as 

(select 
doctor_user_id, 
doctor_id,
full_name,
doctor_type,
count(distinct reporting_state) as r28_reporting_states_count_p, 
sum(net_booked_appointments_p) as r28_net_booked_appointments_p, 
sum(net_initial_booked_appointments_p) as r28_net_initial_booked_appointments_p, 
sum(net_total_appointments_offered_p) as r28_net_total_appointments_offered_p
from doctor_market_share_bookings_offerings 
where days_from_reporting_week_to_max_week <= 28
group by 1,2,3,4)

select 
a.doctor_type,
a.doctor_id,
trim(a.first_name) || ' ' || trim(a.last_name) as full_name,
case when a.doctor_type = 'Pediatricians' then 'p'||a.doctor_id||'p'
      when a.doctor_type = 'Dentist' then 'd'||a.doctor_id||'d' end as doctor_user_id,
coalesce(b.r7_reporting_states_count_p,0) as r7_reporting_states_count_p,
coalesce(b.r7_net_booked_appointments_p,0) as r7_net_booked_appointments_p,
coalesce(d.r7_net_booked_appointments,0) as r7_net_booked_appointments, 
coalesce(b.r7_net_booked_appointments_p/nullif(d.r7_net_booked_appointments,0),0) as r7_net_booked_appointments_share,
coalesce(b.r7_net_initial_booked_appointments_p,0) as r7_net_initial_booked_appointments_p,
coalesce(d.r7_net_initial_booked_appointments,0) as r7_net_initial_booked_appointments,
coalesce(b.r7_net_initial_booked_appointments_p/nullif(d.r7_net_initial_booked_appointments,0),0) as r7_net_initial_booked_appointments_share,
coalesce(b.r7_net_total_appointments_offered_p,0) as r7_net_total_appointments_offered_p, 
coalesce(d.r7_net_total_appointments_offered,0) as r7_net_total_appointments_offered, 
coalesce(b.r7_net_booked_appointments_p/nullif(d.r7_net_total_appointments_offered,0),0) as r7_net_booked_from_total_appointments_share,
coalesce(b.r7_net_initial_booked_appointments_p/nullif(d.r7_net_total_appointments_offered,0),0) as r7_net_initial_booked_from_total_appointments_share,
coalesce(c.r28_reporting_states_count_p,0) as r28_reporting_states_count_p, 
coalesce(c.r28_net_booked_appointments_p,0) as r28_net_booked_appointments_p, 
coalesce(e.r28_net_booked_appointments,0) as r28_net_booked_appointments,
coalesce(c.r28_net_booked_appointments_p/nullif(e.r28_net_booked_appointments,0),0) as r28_net_booked_appointments_share,
coalesce(c.r28_net_initial_booked_appointments_p,0) as r28_net_initial_booked_appointments_p,
coalesce(e.r28_net_initial_booked_appointments,0) as r28_net_initial_booked_appointments, 
coalesce(c.r28_net_initial_booked_appointments_p/nullif(e.r28_net_initial_booked_appointments,0),0) as r28_net_initial_booked_appointments_share, 
coalesce(c.r28_net_total_appointments_offered_p,0) as r28_net_total_appointments_offered_p, 
coalesce(e.r28_net_total_appointments_offered,0) as r28_net_total_appointments_offered,
coalesce(c.r28_net_booked_appointments_p/nullif(e.r28_net_total_appointments_offered,0),0) as r28_net_booked_from_total_appointments_share,
coalesce(c.r28_net_initial_booked_appointments_p/nullif(e.r28_net_total_appointments_offered,0),0) as r28_net_initial_booked_from_total_appointments_share
--aliased table name
from sample.table2 a  
left join r7_market_share_bookings_offerings b on a.doctor_id = b.doctor_id and a.doctor_type = b.doctor_type
inner join r28_market_share_bookings_offerings c on a.doctor_id = c.doctor_id and a.doctor_type = c.doctor_type
left join r7_appointments d on a.doctor_type = d.doctor_type
left join r28_appointments e on a.doctor_type = e.doctor_type