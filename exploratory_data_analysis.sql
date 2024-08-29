-- Exploratory Data Analysis

select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`), max(`date`)
from layoffs_staging2;

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select `date`, sum(total_laid_off)
from layoffs_staging2
group by `date`
order by 1 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select *
from layoffs_staging2;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select substring(`date`,1,7) as `Month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `Month`
order by 1;

with rolling_cte as
	(select substring(`date`,1,7) as `Month`, sum(total_laid_off) monthly_tlo
	from layoffs_staging2
	where substring(`date`,1,7) is not null
	group by `Month`
	order by 1)
select *, sum(monthly_tlo) over(order by `Month`) as rolling_total
from rolling_cte;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

with company_year_cte (company, years, sum_tlo) as
	(select company, year(`date`), sum(total_laid_off)
	from layoffs_staging2
	group by company, year(`date`)),
company_year_rank_cte as 
	(select *, 
	dense_rank() over(partition by years order by sum_tlo desc) as ranking
	from company_year_cte
	where years is not null)
select * 
from company_year_rank_cte
where ranking <= 5;









