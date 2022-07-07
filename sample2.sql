with practitioner_market_share_bookings_offerings as

(select 
reporting_week_ending::date,
reporting_week_ending_table_max::date,
datediff(day,reporting_week_ending::date,(select max(reporting_week_ending_table_max::date) reporting_week_ending_table_max from {{ref('practitioner_slot_opportunity')}})) as days_from_reporting_week_to_max_week,
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
from {{ref('practitioner_slot_opportunity')}})

,r7_slots as 

(select 
practitioner_type, 
sum(net_booked_slots) as r7_net_booked_slots, 
sum(net_initial_booked_slots) as r7_net_initial_booked_slots, 
sum(net_total_slots_offered) as r7_net_total_slots_offered
from (select
       reporting_state,
       practitioner_type,
       max(net_booked_slots) as net_booked_slots,
       max(net_initial_booked_slots) as net_initial_booked_slots,
       sum(net_total_slots_offered_p) as net_total_slots_offered
       from practitioner_market_share_bookings_offerings
       where days_from_reporting_week_to_max_week <= 6
       group by 1,2)
group by 1)

,r7_market_share_bookings_offerings as 

(select 
practitioner_user_id, 
practitioner_id,
full_name, 
practitioner_type,
count(distinct reporting_state) as r7_reporting_states_count_p, 
sum(net_booked_slots_p) as r7_net_booked_slots_p, 
sum(net_initial_booked_slots_p) as r7_net_initial_booked_slots_p, 
sum(net_total_slots_offered_p) as r7_net_total_slots_offered_p
from practitioner_market_share_bookings_offerings 
where days_from_reporting_week_to_max_week <= 6
group by 1,2,3,4)

,r28_slots as 

(select 
practitioner_type, 
sum(net_booked_slots) as r28_net_booked_slots, 
sum(net_initial_booked_slots) as r28_net_initial_booked_slots, 
sum(net_total_slots_offered) as r28_net_total_slots_offered
from (select
       reporting_week_ending,
       reporting_state,
       practitioner_type,
       max(net_booked_slots) as net_booked_slots,
       max(net_initial_booked_slots) as net_initial_booked_slots,
       sum(net_total_slots_offered_p) as net_total_slots_offered
       from practitioner_market_share_bookings_offerings
       where days_from_reporting_week_to_max_week <= 28
       group by 1,2,3)
group by 1)

,r28_market_share_bookings_offerings as 

(select 
practitioner_user_id, 
practitioner_id,
full_name,
practitioner_type,
count(distinct reporting_state) as r28_reporting_states_count_p, 
sum(net_booked_slots_p) as r28_net_booked_slots_p, 
sum(net_initial_booked_slots_p) as r28_net_initial_booked_slots_p, 
sum(net_total_slots_offered_p) as r28_net_total_slots_offered_p
from practitioner_market_share_bookings_offerings 
where days_from_reporting_week_to_max_week <= 28
group by 1,2,3,4)

select 
a.practitioner_type,
a.practitioner_id,
trim(a.first_name) || ' ' || trim(a.last_name) as full_name,
case when a.practitioner_type = 'Provider' then 'p'||a.practitioner_id||'p'
      when a.practitioner_type = 'Therapist' then 't'||a.practitioner_id||'p' end as practitioner_user_id,
coalesce(b.r7_reporting_states_count_p,0) as r7_reporting_states_count_p,
coalesce(b.r7_net_booked_slots_p,0) as r7_net_booked_slots_p,
coalesce(d.r7_net_booked_slots,0) as r7_net_booked_slots, 
coalesce(b.r7_net_booked_slots_p/nullif(d.r7_net_booked_slots,0),0) as r7_net_booked_slots_share,
coalesce(b.r7_net_initial_booked_slots_p,0) as r7_net_initial_booked_slots_p,
coalesce(d.r7_net_initial_booked_slots,0) as r7_net_initial_booked_slots,
coalesce(b.r7_net_initial_booked_slots_p/nullif(d.r7_net_initial_booked_slots,0),0) as r7_net_initial_booked_slots_share,
coalesce(b.r7_net_total_slots_offered_p,0) as r7_net_total_slots_offered_p, 
coalesce(d.r7_net_total_slots_offered,0) as r7_net_total_slots_offered, 
coalesce(b.r7_net_booked_slots_p/nullif(d.r7_net_total_slots_offered,0),0) as r7_net_booked_from_total_slots_share,
coalesce(b.r7_net_initial_booked_slots_p/nullif(d.r7_net_total_slots_offered,0),0) as r7_net_initial_booked_from_total_slots_share,
coalesce(c.r28_reporting_states_count_p,0) as r28_reporting_states_count_p, 
coalesce(c.r28_net_booked_slots_p,0) as r28_net_booked_slots_p, 
coalesce(e.r28_net_booked_slots,0) as r28_net_booked_slots,
coalesce(c.r28_net_booked_slots_p/nullif(e.r28_net_booked_slots,0),0) as r28_net_booked_slots_share,
coalesce(c.r28_net_initial_booked_slots_p,0) as r28_net_initial_booked_slots_p,
coalesce(e.r28_net_initial_booked_slots,0) as r28_net_initial_booked_slots, 
coalesce(c.r28_net_initial_booked_slots_p/nullif(e.r28_net_initial_booked_slots,0),0) as r28_net_initial_booked_slots_share, 
coalesce(c.r28_net_total_slots_offered_p,0) as r28_net_total_slots_offered_p, 
coalesce(e.r28_net_total_slots_offered,0) as r28_net_total_slots_offered,
coalesce(c.r28_net_booked_slots_p/nullif(e.r28_net_total_slots_offered,0),0) as r28_net_booked_from_total_slots_share,
coalesce(c.r28_net_initial_booked_slots_p/nullif(e.r28_net_total_slots_offered,0),0) as r28_net_initial_booked_from_total_slots_share
from {{ source('analytics_dev', 'omar_export_practitioner_status') }} a  
left join r7_market_share_bookings_offerings b on a.practitioner_id = b.practitioner_id and a.practitioner_type = b.practitioner_type
inner join r28_market_share_bookings_offerings c on a.practitioner_id = c.practitioner_id and a.practitioner_type = c.practitioner_type
left join r7_slots d on a.practitioner_type = d.practitioner_type
left join r28_slots e on a.practitioner_type = e.practitioner_type