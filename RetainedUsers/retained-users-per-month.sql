--Write a query that gets the number of retained users per country and month. Here retention for a given month is defined as the number of users who logged in that month who also logged in the immediately previous month.
with a as (select distinct ul.user_id, ul.country, ul.login_date  from USER_LOGIN ul),
c as (select a.user_id, a.country, a.login_date from a join a b on (a.user_id = b.user_id and a.country = b.country and DATEDIFF(month,a.login_date,b.login_date) = -1))
select count(*) count, c.country, YEAR(login_date) year, MONTH(login_date) month from c GROUP BY country, YEAR(login_date), MONTH(login_date)