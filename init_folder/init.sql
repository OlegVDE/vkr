CREATE DATABASE IF NOT EXISTS test_db;

CREATE TABLE IF NOT EXISTS test_db.clients_info
(
  client String,
  hostname String,
  alias_list String,
  addres_list Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (client, hostname, alias_list);

create table if not exists test_db.log_info(
IP String,
timestamp String,
response Int16,
numbers Int32,
device_name String,
browser String
)
ENGINE = ReplacingMergeTree()
ORDER BY (IP, timestamp);

create view if not exists test_db.twin_table 
as select * 
from test_db.log_info li 
join test_db.clients_info ci
on li.IP = ci.client;

create view if not exists test_db.first_view as select * from (
with sur_key as (select 
generateUUIDv4() as surr_key, -- Суррогатный ключ устройства
hostname, -- Название устройства
count(hostname) as cnt_actions, --Количество совершенных действий для данного устройства
((count(hostname)/(select count(*) from test_db.log_info))*100) as percent_from_all -- Доля совершенных действий с данного устройства относительно других устройств
from test_db.twin_table tt 
group by hostname),
cli_info as (select 
hostname, -- Название устройства
count(client) as users, -- Количество пользователей
((count(client)/ (select count(*) from test_db.clients_info))*100) as user_percent_by_host -- Доля пользователей данного устройства от общего числа пользователей
from test_db.clients_info ci  
group by hostname),
response as (select hostname, 
count() as not_200_sum -- Количество ответов сервера, отличных от 200 на данном устройстве
from test_db.twin_table tt 
where response != 200
group by tt.hostname
order by tt.hostname )
select 
sur_key.surr_key,
sur_key.hostname,
cli_info.user_percent_by_host,
sur_key.cnt_actions,
sur_key.percent_from_all,
response.not_200_sum
from sur_key
join cli_info
on sur_key.hostname = cli_info.hostname
join response
on sur_key.hostname = response.hostname);

create view if not exists test_db.second_view as
SELECT hostname, browser, 
count(*) as brows_actions, 
count(hostname) over (partition by hostname) as cnt_brws_on_ip,
((count(*)/(sum(count(*)) over (partition by hostname)))*100) as percent_by_browser_action
from test_db.twin_table tt 
group by hostname, browser 
--order by hostname
; 

create view if not exists test_db.third_view as
select 
response, 
count(*) total__by_response
from test_db.twin_table tt 
-- where response != 200
group by response;