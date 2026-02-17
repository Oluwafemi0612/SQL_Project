-- Exploratory Data AALysis (EDA)

SELECT total_laid_off
FROM layoffs_staging2
;


-- i need to change my total_laid_off column from text to INT in order to perform aggregated function
-- This does not work because my column is in float instead of INT 
ALTER TABLE layoffs_staging2
MODIFY COLUMN total_laid_off INT;


-- print out the floats values
SELECT DISTINCT total_laid_off
FROM layoffs_staging2
WHERE total_laid_off REGEXP '\\.';   -- finds values with decimal points

-- Using FLOOR() function to round it to the nearest whole number
UPDATE layoffs_staging2
SET total_laid_off = FLOOR(total_laid_off)
WHERE total_laid_off IS NOT NULL;

-- Now convert it back to integers(INT)
ALTER TABLE layoffs_staging2
MODIFY COLUMN total_laid_off INT;


SELECT*
FROM layoffs_staging2
;

-- Checking the highest number of laid_off that occur in a company

-- i need to remove the decimal first using FLOOR() method
UPDATE layoffs_staging2
SET percentage_laid_off = FLOOR(percentage_laid_off)
WHERE percentage_laid_off IS NOT NULL;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- ordering by their funding 
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Checking our Date ranges
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;


SELECT*
FROM layoffs_staging2
;

-- checking country with highest  laid_off
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- checking the DATE with the highest laid off
SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC;

-- checking the year with the highest laid off
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;


-- checking the company stage with  the highest laid off
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT company, SUM(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- checking the progression of lay_off (we can call this a rolling SUM)
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- Performing a rollinfg sum
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off
,SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_TOtal;


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,  YEAR(`date`)
ORDER BY company ASC;

-- Checking the country with the highest laidoff till date
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,  YEAR(`date`)
ORDER BY 3 DESC;

-- Checking the company that has highest laid off in each year
WITH Company_Year (company, years, total_laid_off) AS 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,  YEAR(`date`)
)
SELECT*, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY Ranking ASC;


-- Writing a query to see the top 5 company with highest laid off from 2020-2025
WITH Company_Year (company, years, total_laid_off) AS 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,  YEAR(`date`)
), Company_Year_Rank AS
(SELECT*, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;






