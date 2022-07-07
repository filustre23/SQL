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

,r7_net_apointments_totals as 

(select 
doctor_type,
reporting_state,
sum(net_booked_appointments) as r7_net_booked_appointments,
sum(net_initial_booked_appointments) as r7_net_initial_booked_appointments,
sum(net_total_appointments_offered) as r7_net_total_appointments_offered
from (select 
		reporting_week_ending,
		doctor_type,
		reporting_state,
		max(net_booked_appointments) as net_booked_appointments, 
		max(net_initial_booked_appointments) as net_initial_booked_appointments,
		sum(net_total_appointments_offered_p) as net_total_appointments_offered
		from doctor_market_share_bookings_offerings
		where days_from_reporting_week_to_max_week <= 6
		group by 1,2,3)
group by 1,2)

,r28_net_apointments_totals as 

(select 
doctor_type,
reporting_state,
sum(net_booked_appointments) as r28_net_booked_appointments,
sum(net_initial_booked_appointments) as r28_net_initial_booked_appointments,
sum(net_total_appointments_offered) as r28_net_total_appointments_offered
from (select 
		reporting_week_ending,
		doctor_type,
		reporting_state,
		max(net_booked_appointments) as net_booked_appointments, 
		max(net_initial_booked_appointments) as net_initial_booked_appointments,
		sum(net_total_appointments_offered_p) as net_total_appointments_offered
		from doctor_market_share_bookings_offerings
		where days_from_reporting_week_to_max_week <= 28 
		group by 1,2,3)
group by 1,2)

,r7_r28_net_apointments_totals_state as 

(select 
a.*, 
b.r28_net_booked_appointments, 
b.r28_net_initial_booked_appointments,
b.r28_net_total_appointments_offered
from r7_net_slot_totals a 
inner join r28_net_slot_totals b on 
	(a.doctor_type = b.doctor_type and a.reporting_state = b.reporting_state))
	
,r7_net_apointments_p as 

(select 
doctor_user_id, 
doctor_id,
full_name,
doctor_type,
reporting_state,
sum(net_booked_appointments_p) as r7_net_booked_appointments_p, 
sum(net_initial_booked_appointments_p) as r7_net_initial_booked_appointments_p, 
sum(net_total_appointments_offered_p) as r7_net_total_appointments_offered_p
from doctor_market_share_bookings_offerings 
where days_from_reporting_week_to_max_week <= 6
group by 1,2,3,4,5)

,r28_net_apointments_p as 

(select 
doctor_user_id, 
doctor_id,
full_name,
doctor_type,
reporting_state,
sum(net_booked_appointments_p) as r28_net_booked_appointments_p, 
sum(net_initial_booked_appointments_p) as r28_net_initial_booked_appointments_p, 
sum(net_total_appointments_offered_p) as r28_net_total_appointments_offered_p
from doctor_market_share_bookings_offerings 
where days_from_reporting_week_to_max_week <= 28
group by 1,2,3,4,5)

,doctor_bookings_share_offering_state as 

(select 
a.doctor_user_id, 
a.doctor_type,
a.full_name,
a.reporting_state,
coalesce(b.r7_net_booked_appointments_p,0) as r7_net_booked_appointments_p,
coalesce(c.r7_net_booked_appointments,0) as r7_net_booked_appointments,
coalesce(b.r7_net_booked_appointments_p/nullif(c.r7_net_booked_appointments,0),0) as r7_net_booked_appointments_share,
coalesce(b.r7_net_initial_booked_appointments_p,0) as r7_net_initial_booked_appointments_p, 
coalesce(c.r7_net_initial_booked_appointments,0) as r7_net_initial_booked_appointments, 
coalesce(b.r7_net_initial_booked_appointments_p/nullif(c.r7_net_initial_booked_appointments,0),0) as r7_net_initial_booked_appointments_share,
coalesce(b.r7_net_total_appointments_offered_p,0) as r7_net_total_appointments_offered_p,
coalesce(c.r7_net_total_appointments_offered,0) as r7_net_total_appointments_offered,
coalesce(b.r7_net_total_appointments_offered_p,0)/nullif(coalesce(c.r7_net_total_appointments_offered,0),0) as r7_net_offers_from_total_offers_share,
coalesce(b.r7_net_booked_appointments_p/nullif(c.r7_net_total_appointments_offered,0),0) as r7_net_booked_from_total_appointments_share,
coalesce(b.r7_net_initial_booked_appointments_p/nullif(c.r7_net_total_appointments_offered,0),0) as r7_net_initial_booked_from_total_appointments_share,
coalesce(a.r28_net_booked_appointments_p,0) as r28_net_booked_appointments_p,
coalesce(c.r28_net_booked_appointments,0) as r28_net_booked_appointments,
coalesce(a.r28_net_booked_appointments_p/nullif(c.r28_net_booked_appointments,0),0) as r28_net_booked_appointments_share,
coalesce(a.r28_net_initial_booked_appointments_p,0) as r28_net_initial_booked_appointments_p,
coalesce(c.r28_net_initial_booked_appointments, 0) as r28_net_initial_booked_appointments,
coalesce(a.r28_net_initial_booked_appointments_p/nullif(c.r28_net_initial_booked_appointments,0),0) as r28_net_initial_booked_appointments_share, 
coalesce(a.r28_net_total_appointments_offered_p,0) as r28_net_total_appointments_offered_p, 
coalesce(c.r28_net_total_appointments_offered,0) as r28_net_total_appointments_offered, 
coalesce(a.r28_net_total_appointments_offered_p,0)/nullif(coalesce(c.r28_net_total_appointments_offered,0),0) as r28_net_offers_from_total_offers_share,
coalesce(a.r28_net_booked_appointments_p/nullif(c.r28_net_total_appointments_offered,0),0) as r28_net_booked_from_total_appointments_share,
coalesce(a.r28_net_initial_booked_appointments_p/nullif(c.r28_net_total_appointments_offered,0),0) as r28_net_initial_booked_from_total_appointments_share

from r28_net_apointments_p a 
left join r7_net_apointments_p b on 
	(a.doctor_id = b.doctor_id and a.doctor_type = b.doctor_type and a.reporting_state = b.reporting_state)
left join r7_r28_net_apointments_totals_state c on 
	(a.doctor_type = c.doctor_type and a.reporting_state = c.reporting_state))

select * from doctor_bookings_share_offering_state