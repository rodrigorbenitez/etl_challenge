/* How many total messages are being sent every day? */

SELECT
  DATE(createdAt) AS date,
  COUNT(message) AS total_messages
FROM
  messages
GROUP BY
  1

/* Are there any users that did not receive any message? */

WITH
  CTE AS (
  SELECT
    DISTINCT user_id
  FROM
    users)
SELECT
  DISTINCT user_id
FROM
  cte a
LEFT OUTER JOIN
  messages b
ON
  a.user_id=b.receiverId
WHERE
  b.receiverId IS NULL


/* How many active subscriptions do we have today? */

SELECT
  status,
  COUNT(*) AS total
FROM
  subscriptions
WHERE
  status = 'Active'
GROUP BY
  1

/* How much is the average price ticket (sum amount subscriptions / count subscriptions) breakdown by year/month (format YYYY-MM)? */

SELECT
  strftime('%Y-%m', DATE(createdAt)) AS createdAt,
  ROUND(SUM(amount)/ COUNT(createdAt),2) AS avg_price_ticket
FROM
  subscriptions
WHERE
  createdAt IS NOT NULL
GROUP BY
  1