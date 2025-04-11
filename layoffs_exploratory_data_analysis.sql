-- Exploratoty Data Analysis

USE world_layoffs;

SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT country
FROM layoffs_staging2;

SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;




SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, 
SUM(total_off)  OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY company ASC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year(company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *
FROM Company_Year;

WITH Company_Year(company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, 
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *-- EXPLORATORY DATA ANALYSIS ON LAYOFFS DATA
-- Database: world_layoffs
-- Table: layoffs_staging2

USE world_layoffs;

SELECT * 
FROM layoffs_staging2;

-- Max total layoffs and max layoff percentage
SELECT 
    MAX(total_laid_off) AS max_total_laid_off, 
    MAX(percentage_laid_off) AS max_percentage_laid_off
FROM layoffs_staging2;

-- Companies that laid off 100% of employees
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- Sorted by funds raised
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Top companies by total layoffs
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY company
ORDER BY total_layoffs DESC;

-- Date range of layoffs
SELECT 
    MIN(`date`) AS first_layoff_date, 
    MAX(`date`) AS latest_layoff_date
FROM layoffs_staging2;

-- Layoffs by industry
SELECT industry, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_layoffs DESC;

-- Layoffs by country
SELECT country, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY country
ORDER BY total_layoffs DESC;

-- Country list
SELECT DISTINCT country
FROM layoffs_staging2;

-- Daily layoffs
SELECT `date`, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY `date`
ORDER BY `date` DESC;

-- Year-wise layoffs
SELECT YEAR(`date`) AS year, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY year
ORDER BY year DESC;

-- Layoffs by funding stage(sorted by name)
SELECT stage, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY stage
ORDER BY stage;

-- Layoffs by funding stage
SELECT stage, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY stage
ORDER BY total_layoffs DESC;

-- Company-wise average layoff percentage
SELECT company, AVG(percentage_laid_off) AS avg_percentage
FROM layoffs_staging2
GROUP BY company
ORDER BY avg_percentage DESC;

-- Monthly layoffs(YYYY-MM format)
SELECT 
    SUBSTRING(`date`,1,7) AS `month`, 
    SUM(total_laid_off) AS monthly_layoffs
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `month`
ORDER BY `month` ASC;

-- Rolling total layoffs by month
WITH Rolling_Total AS(
    SELECT 
        SUBSTRING(`date`,1,7) AS `month`, 
        SUM(total_laid_off) AS total_off
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`,1,7) IS NOT NULL
    GROUP BY `month`
)
SELECT 
    `month`, 
    total_off, 
    SUM(total_off) OVER (ORDER BY `month`) AS rolling_total
FROM Rolling_Total;

-- Company-wise total layoffs
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY company
ORDER BY total_layoffs DESC;

-- Company-wise layoffs per year
SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY company, year
ORDER BY company ASC;

-- Sorted by total layoffs(company + year)
SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY company, year
ORDER BY total_layoffs DESC;

-- Company-wise yearly layoffs using CTE
WITH Company_Year(company, year, total_laid_off) AS (
    SELECT company, YEAR(`date`), SUM(total_laid_off)
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
)
SELECT * 
FROM Company_Year;

-- Top 5 companies per year by total layoffs
WITH Company_Year(company, year, total_laid_off) AS (
    SELECT company, YEAR(`date`), SUM(total_laid_off)
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS (
    SELECT *, 
           DENSE_RANK() OVER (PARTITION BY year ORDER BY total_laid_off DESC) AS ranking
    FROM Company_Year
    WHERE year IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
WHERE ranking <= 5;

FROM Company_Year_Rank
WHERE Ranking <= 5;













