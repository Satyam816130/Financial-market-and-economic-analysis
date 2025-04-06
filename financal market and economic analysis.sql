create database global
use global

select * from df_finance

alter table df_finance
alter column date date

--best performing index

select stock_index, sum(trading_volume) as trading_vol
from df_finance
group by stock_index

--on which day investor earn more profit.

with cte as(
select datename(weekday,date) as days,year(date) as years,stock_index,round((close_price-open_price),2) as margin
from df_finance)

select days,years,stock_index,margin from(
select *, row_number() over(partition by stock_index order by margin desc) rnk
from cte ) c
where c.rnk=1

--which year worst index crash

select*from df_finance

with cte as(
select stock_index,year(date) year,open_price,close_price, (open_price- close_price) as [-ve_margin]
from df_finance)

select stock_index,year,[-ve_margin] from(
		select  stock_index,year,[-ve_margin], ROW_NUMBER() over(partition by stock_index order by [-ve_margin] ) rnk
		from cte ) c
where c.rnk=1


--gdp growth rate over years

select*from df_finance

with cte as(
	select stock_index, year(date) as years, sum( gdp_growth_rate) as total_gdp
	from df_finance
	group by stock_index, year(date))

select stock_index, years, total_gdp,
		round((total_gdp-lag(total_gdp) over(partition by stock_index order by years))
		/ nullif(lag(total_gdp) over(partition by stock_index order by years),0),2) as yoy_gdp
from cte 


-- debt to gdp ratio


with cte as(
select year(date) as year, sum(govt_debt) as total_debt, sum((gdp_growth_rate/100)) as gdp
from df_finance
group by year(date)
)

select year,total_debt,gdp, round((total_debt/nullif(gdp,0)),2) as debt_to_gdp
from cte 


--peak high and peak low of gold in which year.


with cte as(
		select year(date) as year, gold_per_ounce, ROW_NUMBER() over( order by gold_per_ounce desc) desc_rnk,
		ROW_NUMBER() over( order by gold_per_ounce asc) asc_rnk
		from df_finance) 

select year, 'peak high' as peak_usd
from cte 
where desc_rnk=1 
union all
select year, 'peak low' as peak_usd
from cte 
where asc_rnk=1 

--avg of merger & acquistion deals in 2007.

select stock_index, year(date) as year, avg(merger_acquisition_deal) as [avg_m&A]
from df_finance
where year(date)=2007
group by stock_index, year(date)
 

-- 2007 of 12 month moving avg for inflation rate.

 WITH MonthlyData AS (
    SELECT 
        YEAR(date) AS year, 
        MONTH(date) AS month,
        stock_index, 
        AVG(inflation_rate) AS monthly_avg_inflation
    FROM df_finance
	where YEAR(date) =2007
    GROUP BY YEAR(date), MONTH(date), stock_index
)
SELECT 
    year, 
    month, 
    stock_index, 
    AVG(monthly_avg_inflation) OVER (
        PARTITION BY stock_index 
        ORDER BY year * 12 + month 
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS mov_avg
FROM MonthlyData

--Calulate the Volatility of stock index 

select stock_index, year(date) as year, stdev(close_price -open_price) as mkt_volatility
from df_finance
group by stock_index,year(date)
order by mkt_volatility desc, year  desc 


