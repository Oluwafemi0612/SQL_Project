-- DATA CLEANING
-- Data cleaning is basically where you get it (Data) in a more usable format,-
-- so you fix a lot of the issuea in the raw data that when you start creating visualizations or start-
-- using it in your products that the data is actually useful and there aren't a lot of issues with it.
USE world_layoffs;
SELECT *
FROM layoffs;


-- MySQL Data cleaning pipelines
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- Remove Any Columns 

-- Creating a staging to keep the raw data file format incase of mistakes
CREATE TABLE layoffs_staging
LIKE layoffs;


SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


-- Identifying and removing Duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Partitioning in ROW_NUMBER is mostly advisable to be done to every column in the table(to be sure we are picking duplicates)
WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, industry,  `source`, stage, funds_raised, country, date_added) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;





WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, industry,  `source`, stage, funds_raised, country, date_added) AS row_num
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num >1;





CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `total_laid_off` text,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` int DEFAULT NULL,
  `country` text,
  `date_added` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, industry,  `source`, stage, funds_raised, country, date_added) AS row_num
FROM layoffs_staging;

-- Showing the Duplicates
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Deleting the Duplicates
DELETE
FROM layoffs_staging2
WHERE row_num > 1;


SELECT *
FROM layoffs_staging2;


-- Standardizing data
-- this is the process of finding issue in your data and fixing it
-- TRIM takes off the white space
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- UPDATE the table
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Taking look at the industry
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT *
FROM layoffs_staging2
;

-- Changing our DATE column from TEXT format to DATE format, this is needed for-
-- Time series Analysis. using STR_TO_DATE clause.
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;


-- Now we update 
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Confirming if it update in the table
SELECT `date`
FROM layoffs_staging2;

-- now changing the Date format from text to Date format
-- Don't ever alter your original table
-- Only alter your staging table
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Checking our Table to very all our changes
SELECT *
FROM layoffs_staging2;


-- Standadization step 3(working with NULL and Blank values
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- if the industry name is missing, we can populate the missing industry name witht the correct one
-- we need to join first (self JOIN)
SELECT t1.company, t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry is NULL OR t1.industry = '')
AND t2.industry IS NOT NULL ;

-- Now we update the data and populate it with the industry name
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry is NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- this is what worked for my dataset because i have a slight different dataset
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = 'Software'
WHERE (t1.industry is NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company = 'Appsmith';

SELECT *
FROM layoffs_staging2;

-- Now dropping the columns that are not needed in
ALTER TABLE layoffs_staging2
DROP COLUMN row_num, 
DROP COLUMN `source`;

-- i want to fill all the blank values with null
SELECT  COUNT(percentage_laid_off)
FROM layoffs_staging2
WHERE percentage_laid_off = '';

UPDATE layoffs_staging2
SET total_laid_off = 'NULL'
WHERE total_laid_off = '';

UPDATE layoffs_staging2
SET percentage_laid_off = 'NULL'
WHERE percentage_laid_off = '';


