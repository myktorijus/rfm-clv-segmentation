-- quantity and unitprice have negative values, but without context i didn't exclude them (might be returns, or B2B customers?)

WITH
 aggregated_data AS (
  SELECT
   CustomerID,
   DATE_DIFF(DATE '2011-12-01', DATE(MAX(InvoiceDate)), DAY) AS recency,
   COUNT(DISTINCT InvoiceNo) AS frequency,
   CAST(ROUND(SUM(Quantity * UnitPrice), 0) AS INT64) AS monetary,
   Country
  FROM
   turing_data_analytics.rfm
  WHERE
   DATE(InvoiceDate) BETWEEN '2010-12-01' AND '2011-12-01'
  AND 
   CustomerID IS NOT NULL -- checking only customers that we know about (they're in our database and have customerid)
  GROUP BY
   CustomerID,
   Country
 ),
 quartiles AS (
  SELECT
  -- All percentiles for RECENCY
   APPROX_QUANTILES(recency, 100)[OFFSET(25)] AS r25,
   APPROX_QUANTILES(recency, 100)[OFFSET(50)] AS r50,
   APPROX_QUANTILES(recency, 100)[OFFSET(75)] AS r75,
  -- All percentiles for FREQUENCY
   APPROX_QUANTILES(frequency, 100)[OFFSET(25)] AS f25,
   APPROX_QUANTILES(frequency, 100)[OFFSET(50)] AS f50,
   APPROX_QUANTILES(frequency, 100)[OFFSET(75)] AS f75,
  -- All percentiles for MONETARY
   APPROX_QUANTILES(monetary, 100)[OFFSET(25)] AS m25,
   APPROX_QUANTILES(monetary, 100)[OFFSET(50)] AS m50,
   APPROX_QUANTILES(monetary, 100)[OFFSET(75)] AS m75
  FROM
   aggregated_data
 ),
 scores AS (
  SELECT
   customerid,
   country,
   recency,
   frequency,
   monetary,
  -- for recency assigning score with logic lower is better
  CASE
    WHEN recency <= r25 THEN 4
    WHEN recency <= r50 THEN 3
    WHEN recency <= r75 THEN 2
    ELSE 1
  END AS r_score,
  CASE
   WHEN frequency >= f75 THEN 4
   WHEN frequency >= f50 THEN 3
   WHEN frequency >= f25 THEN 2
   ELSE 1
  END AS f_score,
  CASE
   WHEN monetary >= m75 THEN 4
   WHEN monetary >= m50 THEN 3
   WHEN monetary >= m25 THEN 2
   ELSE 1
  END AS m_score
  FROM
   quartiles
  CROSS JOIN
   aggregated_data
 )
SELECT
  scores.*,
  ROUND((r_score + f_score + m_score) / 3, 2) AS rfm_score,
  -- segmenting the customers by RFM score
  CASE
    WHEN ROUND((r_score + f_score + m_score) / 3, 2) >= 4.0 THEN 'Champions'
    WHEN ROUND((r_score + f_score + m_score) / 3, 2) >= 3.5 THEN 'Loyal Customers'
    WHEN ROUND((r_score + f_score + m_score) / 3, 2) >= 3.0 THEN 'Potential Loyalists'
    WHEN ROUND((r_score + f_score + m_score) / 3, 2) >= 2.5 THEN 'Promising Customers'
    WHEN ROUND((r_score + f_score + m_score) / 3, 2) >= 2.0 THEN 'Needs Attention'
    WHEN ROUND((r_score + f_score + m_score) / 3, 2) >= 1.5 THEN 'At Risk'
    ELSE 'Lost'
  END AS segment
FROM
  scores;