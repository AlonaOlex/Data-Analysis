# Data-Analysis


# 1. Збір даних, які допоможуть аналізувати динаміку створення акаунтів, активність користувачів за листами (відправлення, відкриття, переходи),  а також оцінювати поведінку в категоріях, таких як інтервал відправлення, верифікація акаунтів і статус підписки. Дані дозволять порівнювати активність між країнами, визначати ключові ринки, сегментувати користувачів за різними параметрами.

WITH
email_metrics AS (
SELECT
   DATE_ADD(s.date, INTERVAL ems.sent_date DAY) AS sent_date,
    sp.country,
   ac.send_interval,
   ac.is_verified,
   ac.is_unsubscribed,
   COUNT(DISTINCT ems.id_message) AS sent_msg,
   COUNT(DISTINCT eo.id_message) AS open_msg,
   COUNT(DISTINCT ev.id_message) AS visit_msg
 FROM
   `DA.account_session` acs
 JOIN
   `DA.session` s
 ON
   acs.ga_session_id = s.ga_session_id
  join
  `DA.account`ac
  on acs.account_id = ac.id
 Left JOIN
   `DA.email_sent` ems
 ON
   Acs.account_id = ems.id_account
 LEFT JOIN
   `DA.email_open` eo
 ON
   ems.id_message = eo.id_message
 LEFT JOIN
   `DA.email_visit` ev
 ON
   ems.id_message = ev.id_message
left join `DA.session_params` sp
  on s.ga_session_id = sp.ga_session_id
 GROUP BY
   DATE_ADD(s.date, INTERVAL ems.sent_date DAY),
   sp.country,
   ac.send_interval,
   ac.is_verified,
   ac.is_unsubscribed),
accounts AS (
 SELECT
    s.date,
    count(distinct a.id) as account_cnt,
    a.send_interval,
    a.is_verified,
    is_unsubscribed,
    sp.country,
 FROM
`DA.account`a
Left join
   `DA.account_session` acs
On a.id=acs.account_id
 left JOIN
   `DA.session` s
 ON  acs.ga_session_id = s.ga_session_id
   left join `data-analytics-mate.DA.session_params`sp
  on acs.ga_session_id = sp.ga_session_id
group by
s.date,
a.send_interval,
a.is_verified,
is_unsubscribed,
sp.country
),
union1 as (
SELECT
    sent_date as date,
    country,
    0 as account_cnt,
    send_interval,
    is_verified,
    is_unsubscribed,
    sent_msg,
    open_msg,
    visit_msg
 FROM email_metrics
 UNION ALL
 SELECT
    date,
    country,
    account_cnt,
    send_interval,
    is_verified,
    is_unsubscribed,
    0 as sent_msg,
    0 as open_msg,
    0 as visit_msg
 FROM accounts ),
  final as (
 SELECT
    date,
    country,
    count(account_cnt) as account_cnt,
    send_interval,
    is_verified,
    is_unsubscribed,
    sum(sent_msg) as sent_msg,
    sum(open_msg) as open_msg,
    sum(visit_msg) as visit_msg
 From union1
    group by date,
country,
send_interval,
is_verified,
is_unsubscribed),
sums as (
Select *,
dense_rank() over (order by account_cnt desc) as rank_total_country_account_cnt,
dense_rank() over (order by sent_msg desc) as rank_total_country_sent_cnt
from (
Select *,
      sum(account_cnt) over (partition by country) as total_country_account_cnt,
      sum(sent_msg) over (partition by country) as total_country_sent_cnt,
from final)
)

Select *
from sums
where rank_total_country_account_cnt <= 10 or rank_total_country_sent_cnt
 <= 10

# 2. Загальний дохід (revenue) для кожного континенту. Дохід від категорії Bookcases & shelving units (revenue_from_bookcases). Відсоток доходу від категорії Bookcases & shelving units від загального доходу по континенту (revenue_from_bookcases_percent).

SELECT
sp.continent,
sum(p.price) as revenue,
sum(case when p.category = 'Bookcases & shelving units'then p.price end) as revenue_from_bookcases,
sum(case when p.category = 'Bookcases & shelving units'then p.price end)/sum(p.price) *100 as revenue_from_bookcases_percent
from `DA.session_params`sp
left join `DA.order`o
on sp.ga_session_id = o.ga_session_id
left join `DA.product`p
on o.item_id = p.item_id
Group by sp.continent;

# 3. Revenue and Cost. Виведення в одній таблиці суми доходів та витрат на рекламу по днях. 
SELECT
 date,'cost'as type,cost.cost
FROM `data-analytics-mate.DA.paid_search_cost`cost
UNION ALL
SELECT
date, 'revenue'as type,Sum(p.price) as cost
FROM `data-analytics-mate.DA.session`s
left join `data-analytics-mate.DA.order`o
on s.ga_session_id = o.ga_session_id
left join `data-analytics-mate.DA.product`p
on o.item_id = p.item_id
Group by date;

# 4. Кількість івентів, що мають тип user_engagement, але лише по тих сесіях, де в цілому було більше 2 будь-яких івентів в рамках сесії.
SELECT
count(*) as user_engagement_events
from `data-analytics-mate.DA.event_params` evp
join
(
SELECT ga_session_id,
count(event_name) as event_cnt
from `data-analytics-mate.DA.event_params`
Group by ga_session_id
HAVING count(event_name)>2
) events
on evp.ga_session_id = events.ga_session_id
AND evp.event_name='user_engagement'






