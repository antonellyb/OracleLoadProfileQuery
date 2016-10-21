with res_prof as
(
select min(sql_text) sql_text,
sum(executions) executions, 
sum(disk_reads) disk_reads,
sum(buffer_gets) buffer_gets,
sum(cpu_time) cpu_time,	
sum(elapsed_time) elapsed_time,
sum(rows_processed) rows_processed,
first_load_time,
max(last_load_time) last_load_time,	
hash_value,
address 
from v$sql
group by hash_value,address,first_load_time
)
select sql_text,
sum(executions)/greatest((sysdate-min(to_date(last_load_time,'YYYY-MM-DD/HH24:MI:SS'))),1) executions,
sum(rows_processed)/greatest((sysdate-min(to_date(last_load_time,'YYYY-MM-DD/HH24:MI:SS'))),1) rows_day, 
round(sum(disk_reads)/sum(executions)) disk,
round(sum(buffer_gets)/sum(executions)) buffer,
round((sum(disk_reads)+sum(buffer_gets))/sum(executions)) total_reads,
round(sum(rows_processed)/sum(executions)) rows_per_execution, 
round(sum(cpu_time)/sum(executions)/1000000,2) cpu,	
round(sum(elapsed_time)/sum(executions)/1000000,2) ela,
round((sum(disk_reads)+sum(buffer_gets))/greatest((sysdate-min(to_date(last_load_time,'YYYY-MM-DD/HH24:MI:SS'))),1)) total_reads_day,
round(sum(elapsed_time)/greatest(sysdate-min(to_date(last_load_time,'YYYY-MM-DD/HH24:MI:SS')),1)/1000000,2) ela_day, 
min(to_date(first_load_time,'YYYY-MM-DD/HH24:MI:SS')) first_load_time,
max(last_load_time) last_load_time,	
hash_value,
max(address),
round(
(sum(disk_reads)+sum(buffer_gets))/greatest((sysdate-min(to_date(last_load_time,'YYYY-MM-DD/HH24:MI:SS'))),1)/
sum((sum(disk_reads)+sum(buffer_gets))/greatest((sysdate-min(to_date(last_load_time,'YYYY-MM-DD/HH24:MI:SS'))),1)) over (partition by 1)) reads_day_perc,
round(
sum(elapsed_time)/greatest(sysdate-min(to_date(last_load_time,'YYYY-MM-DD/HH24:MI:SS')),1)/1000000/
sum(sum(elapsed_time)/greatest(sysdate-min(to_date(last_load_time,'YYYY-MM-DD/HH24:MI:SS')),1)/1000000) over (partition by 1)) ela_day_perc 
from res_prof 
where executions>0 and buffer_gets>=0 and disk_reads>=0
group by sql_text,hash_value
order by 9 desc