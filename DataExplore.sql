-- ******************************************************************
-- EXPLORATORY DATA ANALYSIS ON layoffs_staging2
-- ******************************************************************

-- 1. View the Entire Cleaned Data Table
SELECT *
FROM layoffs_staging2;

-- 2. Find the Maximum Values for total_laid_off and percentage_laid_off
--    (This helps you understand the upper bounds in your data.)
SELECT 
  MAX(total_laid_off) AS max_total_laid_off, 
  MAX(percentage_laid_off) AS max_percentage_laid_off
FROM layoffs_staging2;

-- 3. Find Records Where percentage_laid_off Equals 1, Ordered by funds_raised_millions Descending
--    (This might help identify outliers or specific conditions in your data.)
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- 4. Aggregate Total Layoffs by Company
--    (This summarizes the total layoffs per company, ordered from highest to lowest.)
SELECT company, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY company
ORDER BY total_laid_off_sum DESC;

-- 5. Get the Earliest and Latest Date in the Data
SELECT 
  MIN(`date`) AS earliest_date, 
  MAX(`date`) AS latest_date
FROM layoffs_staging2;

-- 6. Aggregate Total Layoffs by Country
--    (Adjust column names as needed; here, we assume the column is "country".)
SELECT country, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY country
ORDER BY total_laid_off_sum DESC;

-- 7. (Optional) Re-View the Entire Data Table for Verification
SELECT *
FROM layoffs_staging2;

-- 8. Aggregate Total Layoffs by Year
--    (Extract the year from the date and summarize layoffs per year.)
SELECT YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY year DESC;

-- 9. Aggregate Total Layoffs by Stage
--    (This groups the data by the stage column to see which stages have the most layoffs.)
SELECT stage, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY stage
ORDER BY total_laid_off_sum DESC;

-- 10. Aggregate Total Layoffs by Month
--     (Extracts the first 7 characters of the date (e.g., 'YYYY-MM') to group by month.)
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH` ASC;

-- 11. Calculate a Rolling (Cumulative) Total of Layoffs by Month
--     (Uses a CTE to first aggregate by month, then a window function to compute the cumulative sum.)
WITH Rolling_Total AS 
(
    SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, 
           SUM(total_laid_off) AS total_off
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `MONTH`
    ORDER BY `MONTH` ASC
)
SELECT 
  `MONTH`, 
  total_off,  -- Monthly total layoffs
  SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total  -- Cumulative total across months
FROM Rolling_Total;

-- 12. Aggregate Total Layoffs by Company and Year
--     (Groups data by company and year, then orders companies by total layoffs descending.)
SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY total_laid_off_sum DESC;

-- 13. Rank Companies by Total Layoffs per Year and Select the Top 5 for Each Year
--     This uses two CTEs: 
--     - Company_Year: Aggregates total layoffs per company per year.
--     - Company_Year_Rank: Applies a DENSE_RANK() partitioned by year.
WITH Company_Year AS (
    SELECT company, 
           YEAR(`date`) AS years, 
           SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
    ORDER BY total_laid_off DESC
), Company_Year_Rank AS (
    SELECT *,
           DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
    FROM Company_Year
    WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;