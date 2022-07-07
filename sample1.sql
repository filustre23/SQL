with practitioner_market_share_bookings_offerings as

(select 
reporting_week_ending::date,
reporting_week_ending_table_max::date,
datediff(day,reporting_week_ending::date,(select max(reporting_week_ending_table_max::date) reporting_week_ending_table_max from sample.table)) as days_from_reporting_week_to_max_week,
practitioner_user_id,
practitioner_id,
practitioner_type,
full_name, 
reporting_state,
case when practitioner_type = 'Provider' then net_booked_slots_medication_p else net_booked_slots_therapy_p end as net_booked_slots_p,
case when practitioner_type = 'Provider' then net_booked_slots_medication else net_booked_slots_therapy end as net_booked_slots,
case when practitioner_type = 'Provider' then net_initial_booked_slots_medication_p else net_initial_booked_slots_therapy_p end as net_initial_booked_slots_p,
case when practitioner_type = 'Provider' then net_initial_booked_slots_medication else net_initial_booked_slots_therapy end as net_initial_booked_slots,
case when practitioner_type = 'Provider' then net_total_slots_medication_p else net_total_slots_therapy_p end as net_total_slots_offered_p,
case when practitioner_type = 'Provider' then net_total_slots_medication else net_total_slots_therapy end as net_total_slots_offered
--aliased table name
from sample.table)

,r7_net_slot_totals as 

(select 
practitioner_type,
reporting_state,
sum(net_booked_slots) as r7_net_booked_slots,
sum(net_initial_booked_slots) as r7_net_initial_booked_slots,
sum(net_total_slots_offered) as r7_net_total_slots_offered
from (select 
		reporting_week_ending,
		practitioner_type,
		reporting_state,
		max(net_booked_slots) as net_booked_slots, 
		max(net_initial_booked_slots) as net_initial_booked_slots,
		sum(net_total_slots_offered_p) as net_total_slots_offered
		from practitioner_market_share_bookings_offerings
		where days_from_reporting_week_to_max_week <= 6
		group by 1,2,3)
group by 1,2)

,r28_net_slot_totals as 

(select 
practitioner_type,
reporting_state,
sum(net_booked_slots) as r28_net_booked_slots,
sum(net_initial_booked_slots) as r28_net_initial_booked_slots,
sum(net_total_slots_offered) as r28_net_total_slots_offered
from (select 
		reporting_week_ending,
		practitioner_type,
		reporting_state,
		max(net_booked_slots) as net_booked_slots, 
		max(net_initial_booked_slots) as net_initial_booked_slots,
		sum(net_total_slots_offered_p) as net_total_slots_offered
		from practitioner_market_share_bookings_offerings
		where days_from_reporting_week_to_max_week <= 28 
		group by 1,2,3)
group by 1,2)

,r7_r28_net_slot_totals_state as 

(select 
a.*, 
b.r28_net_booked_slots, 
b.r28_net_initial_booked_slots,
b.r28_net_total_slots_offered
from r7_net_slot_totals a 
inner join r28_net_slot_totals b on 
	(a.practitioner_type = b.practitioner_type and a.reporting_state = b.reporting_state))
	
,r7_net_slot_p as 

(select 
practitioner_user_id, 
practitioner_id,
full_name,
practitioner_type,
reporting_state,
sum(net_booked_slots_p) as r7_net_booked_slots_p, 
sum(net_initial_booked_slots_p) as r7_net_initial_booked_slots_p, 
sum(net_total_slots_offered_p) as r7_net_total_slots_offered_p
from practitioner_market_share_bookings_offerings 
where days_from_reporting_week_to_max_week <= 6
group by 1,2,3,4,5)

,r28_net_slot_p as 

(select 
practitioner_user_id, 
practitioner_id,
full_name,
practitioner_type,
reporting_state,
sum(net_booked_slots_p) as r28_net_booked_slots_p, 
sum(net_initial_booked_slots_p) as r28_net_initial_booked_slots_p, 
sum(net_total_slots_offered_p) as r28_net_total_slots_offered_p
from practitioner_market_share_bookings_offerings 
where days_from_reporting_week_to_max_week <= 28
group by 1,2,3,4,5)

,practitioner_bookings_share_offering_state as 

(select 
a.practitioner_user_id, 
a.practitioner_type,
a.full_name,
a.reporting_state,
coalesce(b.r7_net_booked_slots_p,0) as r7_net_booked_slots_p,
coalesce(c.r7_net_booked_slots,0) as r7_net_booked_slots,
coalesce(b.r7_net_booked_slots_p/nullif(c.r7_net_booked_slots,0),0) as r7_net_booked_slots_share,
coalesce(b.r7_net_initial_booked_slots_p,0) as r7_net_initial_booked_slots_p, 
coalesce(c.r7_net_initial_booked_slots,0) as r7_net_initial_booked_slots, 
coalesce(b.r7_net_initial_booked_slots_p/nullif(c.r7_net_initial_booked_slots,0),0) as r7_net_initial_booked_slots_share,
coalesce(b.r7_net_total_slots_offered_p,0) as r7_net_total_slots_offered_p,
coalesce(c.r7_net_total_slots_offered,0) as r7_net_total_slots_offered,
coalesce(b.r7_net_total_slots_offered_p,0)/nullif(coalesce(c.r7_net_total_slots_offered,0),0) as r7_net_offers_from_total_offers_share,
coalesce(b.r7_net_booked_slots_p/nullif(c.r7_net_total_slots_offered,0),0) as r7_net_booked_from_total_slots_share,
coalesce(b.r7_net_initial_booked_slots_p/nullif(c.r7_net_total_slots_offered,0),0) as r7_net_initial_booked_from_total_slots_share,
coalesce(a.r28_net_booked_slots_p,0) as r28_net_booked_slots_p,
coalesce(c.r28_net_booked_slots,0) as r28_net_booked_slots,
coalesce(a.r28_net_booked_slots_p/nullif(c.r28_net_booked_slots,0),0) as r28_net_booked_slots_share,
coalesce(a.r28_net_initial_booked_slots_p,0) as r28_net_initial_booked_slots_p,
coalesce(c.r28_net_initial_booked_slots, 0) as r28_net_initial_booked_slots,
coalesce(a.r28_net_initial_booked_slots_p/nullif(c.r28_net_initial_booked_slots,0),0) as r28_net_initial_booked_slots_share, 
coalesce(a.r28_net_total_slots_offered_p,0) as r28_net_total_slots_offered_p, 
coalesce(c.r28_net_total_slots_offered,0) as r28_net_total_slots_offered, 
coalesce(a.r28_net_total_slots_offered_p,0)/nullif(coalesce(c.r28_net_total_slots_offered,0),0) as r28_net_offers_from_total_offers_share,
coalesce(a.r28_net_booked_slots_p/nullif(c.r28_net_total_slots_offered,0),0) as r28_net_booked_from_total_slots_share,
coalesce(a.r28_net_initial_booked_slots_p/nullif(c.r28_net_total_slots_offered,0),0) as r28_net_initial_booked_from_total_slots_share

from r28_net_slot_p a 
left join r7_net_slot_p b on 
	(a.practitioner_id = b.practitioner_id and a.practitioner_type = b.practitioner_type and a.reporting_state = b.reporting_state)
left join r7_r28_net_slot_totals_state c on 
	(a.practitioner_type = c.practitioner_type and a.reporting_state = c.reporting_state))

select * from practitioner_bookings_share_offering_state