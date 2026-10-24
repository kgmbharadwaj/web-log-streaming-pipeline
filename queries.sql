-- 1. Traffic per minute
SELECT strftime('%Y-%m-%d %H:%M', ts) AS minute, COUNT(*) AS requests
FROM web_logs GROUP BY minute ORDER BY minute;

-- 2. Overall error rate (4xx + 5xx)
SELECT COUNT(*) AS total_requests,
       SUM(status >= 400) AS errors,
       ROUND(100.0 * SUM(status >= 400) / COUNT(*), 2) AS error_rate_pct
FROM web_logs;

-- 3. Status code distribution
SELECT status, COUNT(*) AS hits,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM web_logs), 2) AS pct
FROM web_logs GROUP BY status ORDER BY hits DESC;

-- 4. Error rate per page (worst first)
SELECT path, COUNT(*) AS requests,
       SUM(status >= 400) AS errors,
       ROUND(100.0 * SUM(status >= 400) / COUNT(*), 2) AS error_rate_pct
FROM web_logs GROUP BY path ORDER BY error_rate_pct DESC;

-- 5. Top 5 most visited pages
SELECT path, COUNT(*) AS hits FROM web_logs GROUP BY path ORDER BY hits DESC LIMIT 5;

-- 6. Slowest endpoints (average response time)
SELECT path, ROUND(AVG(response_ms), 1) AS avg_ms, MAX(response_ms) AS max_ms
FROM web_logs GROUP BY path ORDER BY avg_ms DESC;

-- 7. Top 5 IPs by traffic
SELECT ip, COUNT(*) AS requests FROM web_logs GROUP BY ip ORDER BY requests DESC LIMIT 5;
