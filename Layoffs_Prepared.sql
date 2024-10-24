-- Data Cleaning

SELECT *
FROM layoffs;

-- Creating a staging table to avoide altering maine table

CREATE TABLE layoffs_stage
LIKE layoffs;

SELECT *
FROM layoffs_stage;

INSERT INTO layoffs_stage
SELECT *
FROM layoffs;

-- Remove Duplicate

SELECT *,
ROW_NUMBER() OVER ( PARTITION BY company,location,
industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_stage;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER ( PARTITION BY company,location,
industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_stage
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_stage2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_stage2
SELECT *,
ROW_NUMBER() OVER ( PARTITION BY company,location,
industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_stage;

SELECT *
FROM layoffs_stage2
WHERE row_num > 1;

DELETE 
FROM layoffs_stage2
WHERE row_num > 1;

-- Standardizing the data

SELECT *
FROM layoffs_stage2;

SELECT company, TRIM(company)
FROM layoffs_stage2
ORDER BY 1;

UPDATE layoffs_stage2
SET company = TRIM(company);

SELECT *
FROM layoffs_stage2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_stage2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_stage2;

UPDATE layoffs_stage2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM layoffs_stage2;

ALTER TABLE layoffs_stage2
MODIFY COLUMN `date` DATE;

-- Removal / Populating of Blanks or nulls 

SELECT *
FROM layoffs_stage2
WHERE industry IS NULL
OR industry = '';

-- Using Join to populate the table
UPDATE layoffs_stage2
SET industry = null
WHERE industry = '';

SELECT t1.company, t1.location, t1.industry,
t2.company, t2.location, t2.industry
FROM layoffs_stage2 t1
JOIN layoffs_stage2 t2 ON 
	t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_stage2 t1
JOIN layoffs_stage2 t2 ON 
	t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_stage2
WHERE industry IS NULL
OR industry = '';

DELETE
FROM layoffs_stage2
WHERE company = "Bally's Interactive";

SELECT *
FROM layoffs_stage2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_stage2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT DISTINCT country
FROM layoffs_stage2
ORDER BY 1;

UPDATE layoffs_stage2
SET country = "United States"
WHERE country LIKE "United States%";

ALTER TABLE layoffs_stage2
DROP COLUMN row_num;

SELECT *
FROM layoffs_stage2;

-- Exploratory Analysis

SELECT company, SUM(total_laid_off)
FROM layoffs_stage2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_stage2;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_stage2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Calculating the rolling sum

SELECT SUBSTRING(`date`,1,7) AS `Month`,
SUM(total_laid_off) AS total_off
FROM layoffs_stage2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

WITH Rollingtotal_cte AS
(
SELECT SUBSTRING(`date`,1,7) AS `Month`,
SUM(total_laid_off) AS total_off
FROM layoffs_stage2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`, total_off
,SUM(total_off) OVER( ORDER BY `Month`) AS rolling_total
FROM Rollingtotal_cte;

SELECT company, SUM(total_laid_off)
FROM layoffs_stage2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`) ,SUM(total_laid_off)
FROM layoffs_stage2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC;

-- ranking company by their layoff by year

WITH company_year (Company, Years, total_laid_off) AS
(
SELECT company, YEAR(`date`) ,SUM(total_laid_off)
FROM layoffs_stage2
GROUP BY company,YEAR(`date`)
), COmpamy_year_Ranking AS
(SELECT *,
DENSE_RANK() OVER( PARTITION BY Years ORDER BY total_laid_off DESC) AS RANKING
FROM company_year
WHERE Years IS NOT NULL
)
SELECT *
FROM COmpamy_year_Ranking
WHERE RANKING <=5 ;