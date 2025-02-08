-- ******************************************************************
-- Data Cleaning and Standardization Script for Layoffs Data
--
-- This script performs the following steps:
-- 1. (Optional) Back up the raw data.
-- 2. Identify and remove duplicate records.
-- 3. Clean and standardize data (convert 'none' values to NULL, trim text,
--    standardize industry names, clean country names, and standardize dates).
-- 4. Update missing industry values using self-joins.
-- 5. Remove rows with missing critical numeric data.
-- 6. Final cleanup: remove unnecessary columns.
--
-- ******************************************************************

-- *************************************************************
-- STEP 0: BACKUP RAW DATA (OPTIONAL)
-- *************************************************************

-- Preview the raw data:
SELECT * FROM layoffs;

-- Create a backup (staging) table with the same structure as the original:
CREATE TABLE layoffs_staging LIKE layoffs;

-- Verify the backup table was created:
SELECT * FROM layoffs_staging;

-- Insert all data from the original table into the backup:
INSERT INTO layoffs_staging
SELECT * FROM layoffs;

-- *************************************************************
-- STEP 1: IDENTIFY DUPLICATE RECORDS
-- *************************************************************
-- Use a CTE with ROW_NUMBER() to label duplicates.
-- The PARTITION BY clause groups rows by key columns.
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, 
                            total_laid_off, percentage_laid_off, 
                            `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM layoffs_staging
)
-- Display duplicates (rows where row_num > 1):
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

-- (Optional) Check for a specific company (e.g., 'Casper'):
SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';

-- NOTE: MySQL does not allow DELETE directly from a CTE or derived table because 
-- they are not updatable targets. Therefore, we will disable the current cleaned
-- staging table by renaming it before creating a new one.

-- *************************************************************
-- TEMPORARILY DISABLE THE EXISTING CLEANED TABLE
-- *************************************************************
RENAME TABLE layoffs_staging2 TO my_temp_table_disabled2;
-- To re-enable later, you could rename it back:
-- RENAME TABLE my_temp_table_disabled2 TO layoffs_staging2;

-- *************************************************************
-- STEP 2: CREATE A NEW CLEANED STAGING TABLE (layoffs_staging2)
-- *************************************************************
CREATE TABLE layoffs_staging2 (
  company                 TEXT,
  location                TEXT,
  industry                TEXT,
  total_laid_off          INT DEFAULT NULL,
  percentage_laid_off     TEXT,
  `date`                  TEXT,
  stage                   TEXT,
  country                 TEXT,
  funds_raised_millions   INT DEFAULT NULL,
  row_num                 INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- *************************************************************
-- STEP 3: CLEAN DATA IN layoffs_staging (HANDLE 'none' VALUES)
-- *************************************************************
-- Disable safe update mode to allow updates without key-based WHERE clauses.
SET SQL_SAFE_UPDATES = 0;

-- Replace the string 'none' with NULL for key columns in layoffs_staging.
UPDATE layoffs_staging
SET total_laid_off = NULLIF(total_laid_off, 'none'),
    percentage_laid_off = NULLIF(percentage_laid_off, 'none'),
    funds_raised_millions = NULLIF(funds_raised_millions, 'none');

-- Verify the changes:
SELECT * FROM layoffs_staging;

-- *************************************************************
-- STEP 4: INSERT CLEANED DATA INTO layoffs_staging2 WITH ROW NUMBERS
-- *************************************************************
-- Insert data from the cleaned staging table into the new table while adding a
-- row number (to help identify duplicates).
INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, 
                        total_laid_off, percentage_laid_off, 
                        `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM layoffs_staging;

-- Verify the new table:
SELECT * FROM layoffs_staging2;

-- *************************************************************
-- STEP 5: REMOVE DUPLICATE RECORDS FROM layoffs_staging2
-- *************************************************************
-- Delete rows where row_num > 1 (i.e., duplicates), keeping only the first occurrence.
DELETE FROM layoffs_staging2
WHERE row_num > 1;

-- Confirm duplicates are removed:
SELECT * FROM layoffs_staging2;

-- *************************************************************
-- STEP 6: STANDARDIZE TEXT DATA
-- *************************************************************
-- (a) Trim whitespace from company names.
SELECT company, TRIM(company) AS trimmed_company
FROM layoffs_staging2;
  
UPDATE layoffs_staging2
SET company = TRIM(company);

-- (b) Standardize industry names: Convert any industry starting with 'Crypto' to 'Crypto'.
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- (c) Clean country names by removing trailing periods.
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) AS clean_country
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Confirm changes to country names:
SELECT DISTINCT country FROM layoffs_staging2 ORDER BY 1;

-- *************************************************************
-- STEP 7: STANDARDIZE DATE FORMATS
-- *************************************************************
-- Preview the current date values (stored as text).
SELECT `date` FROM layoffs_staging2;

-- Check for rows where the date is 'none':
SELECT `date`
FROM layoffs_staging2
WHERE `date` = 'none';

-- Update the date column:
-- 1. Convert 'None' to NULL.
UPDATE layoffs_staging2
SET `date` = NULLIF(`date`, 'None');

-- 2. Convert valid date strings to a proper date format using STR_TO_DATE.
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Verify the date conversion:
SELECT `date` FROM layoffs_staging2;

-- Alter the column type from TEXT to DATE.
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- *************************************************************
-- STEP 8: FURTHER CLEAN 'none' VALUES (CASE-INSENSITIVE)
-- *************************************************************
-- Update columns to set any value equal to 'none' (in any case) to NULL.
UPDATE layoffs_staging2
SET 
  company = CASE WHEN LOWER(company) = 'none' THEN NULL ELSE company END,
  location = CASE WHEN LOWER(location) = 'none' THEN NULL ELSE location END,
  industry = CASE WHEN LOWER(industry) = 'none' THEN NULL ELSE industry END,
  percentage_laid_off = CASE WHEN LOWER(percentage_laid_off) = 'none' THEN NULL ELSE percentage_laid_off END,
  `date` = CASE WHEN LOWER(`date`) = 'none' THEN NULL ELSE `date` END,
  stage = CASE WHEN LOWER(stage) = 'none' THEN NULL ELSE stage END,
  country = CASE WHEN LOWER(country) = 'none' THEN NULL ELSE country END;

-- *************************************************************
-- STEP 9: UPDATE MISSING INDUSTRY INFORMATION USING SELF-JOIN
-- *************************************************************
-- Identify pairs of rows with the same company and location where one record has
-- a missing industry (NULL or empty) and another has a non-NULL industry.
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
   AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
  AND t2.industry IS NOT NULL;

-- Update t1 to fill in the missing industry with the non-NULL value from t2.
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
   AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Confirm that the industry values have been updated.
SELECT * FROM layoffs_staging2 WHERE industry IS NULL;

-- *************************************************************
-- STEP 10: REMOVE RECORDS WITH MISSING CRITICAL NUMERIC DATA
-- *************************************************************
-- Identify records where both total_laid_off and percentage_laid_off are NULL.
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Delete those records.
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Verify deletion.
SELECT * FROM layoffs_staging2;

-- *************************************************************
-- STEP 11: FINAL CLEANUP - DROP UNNECESSARY COLUMNS
-- *************************************************************
-- Remove the temporary row_num column as it is no longer needed.
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- ******************************************************************
-- END OF SCRIPT
-- This script demonstrates backing up raw data, removing duplicates,
-- cleaning and standardizing text and date formats, filling in missing
-- values, and final cleanup, all in a structured and commented manner.
-- ******************************************************************