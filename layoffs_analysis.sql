
-- data cleaning

use world_layoffs;

select * from layoffs ;

create table layoffs1 like layoffs;


select * from layoffs1;

insert into layoffs1
select * from layoffs;
-- delete duplicates values
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions ) as `row_number` from layoffs1;

with duplicates_cte as(
select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions ) 
as `row_number` from layoffs1
)

select * from duplicates_cte where `row_number`>1;

select * from duplicates_cte where company='Casper';

select * from layoffs2;

insert into layoffs2
select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions ) 
as `row_number` from layoffs1;

select * from layoffs2 where `row_numbwe`>1; 


delete from layoffs2 where `row_numbwe` > 1 ;

select * from layoffs2;

-- standardizing data

select * from layoffs2;

select company,trim(company) from layoffs2;

update layoffs2 set company=trim(company);

select distinct industry from layoffs2 order by 1;

select distinct industry from layoffs2 where industry like 'Crypto%';

update layoffs2 set industry='Crypto' where industry like 'Crypto%';

select distinct industry from layoffs2;

update layoffs2 set industry=null where industry='';

select * from layoffs2
 where 
 industry is null  ;

select * from layoffs2 where company='Airbnb';

select t1.industry,t2.industry from layoffs2 t1  join layoffs2 t2
on t1.company=t2.company 
where (t1.industry is null or t1.industry='')
and t2.industry is not null;


update layoffs2 t1  join layoffs2 t2
on t1.company=t2.company set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;



select distinct location from layoffs2 order by 1;

select distinct country from layoffs2 order by 1;

select country,trim(trailing '.' from country) from layoffs2;

update layoffs2 set country=trim(trailing '.' from country)
where country like 'United States%';

select * from layoffs2;

select `date`, str_to_date(`date`,'%m/%d/%Y') 
from layoffs2;

update layoffs2 set `date`=str_to_date(`date`,'%m/%d/%Y');

select `date` from layoffs2;

alter table layoffs2 modify `date` Date;

select * from layoffs2;

select * from layoffs2 where 
total_laid_off is null
and percentage_laid_off is null;


delete from layoffs2 where 
total_laid_off is null
and percentage_laid_off is null;

alter table layoffs2 drop column row_numbwe;




-- exploratory data analysis


select max(total_laid_off),max(percentage_laid_off) from layoffs2;

select * from layoffs2 

where percentage_laid_off=1
order by total_laid_off desc;

select * from layoffs2
where percentage_laid_off=1
order by funds_raised_millions desc;


select company,sum(total_laid_off)from layoffs2
group by company
order by 2 desc;

select max(`date`), min(`date`) from layoffs2;

select industry,sum(total_laid_off) from layoffs2
group by industry
order by 2 desc;

select country ,sum(total_laid_off) from layoffs2
group by country
order by 2 desc;

select year(`date`) ,sum(total_laid_off) from layoffs2
group by year(`date`)
order by 2 desc;


select location ,sum(total_laid_off) from layoffs2
group by location
order by 2 desc;

select stage ,sum(total_laid_off) from layoffs2
group by stage
order by 2 desc;

select substring(`date`,1,7) as `month` ,sum(total_laid_off) from layoffs2
group by `month`
order by 2 desc;


with roling_total as(
select substring(`date`,1,7) as `month` ,sum(total_laid_off) as total from layoffs2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`,total,sum(total) over(partition by `month`)as roling_total
from roling_total;


select company,year(`date`),sum(total_laid_off)from layoffs2
group by company,year(`date`)
order by 1 asc;



with company_year(company,years,total_laid) as(
select company,year(`date`),sum(total_laid_off)from layoffs2
group by company,year(`date`)
),company_year_rank as(
select *,dense_rank() over(partition by years order by total_laid desc) as `rank` 
from company_year
where years is not null
)

select * 
from company_year_rank
where `rank`<=5;




select * from layoffs2;












