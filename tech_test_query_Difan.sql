--Generate every day in June, July, August and September 2020 (one row per day).
WITH date_range AS (
    SELECT generate_series(                                 
        '2020-06-01'::date,
        '2020-09-30'::date,
        '1 day'::interval
    )::date AS dt_report
),
--Filter the enabled accounts and remove the duplicate accounts from the 'users' table
enabled_users AS (                                          
    SELECT DISTINCT login_hash, server_hash, currency
    FROM users
    WHERE enable = 1
),
-- Select distinct combinations of login_hash, server_hash, and symbol from the 'trades' table
distinct_user_symbol AS (                                  
    SELECT DISTINCT login_hash, server_hash, symbol
    FROM trades
),
-- Create a dataset with all enabled users, their server, traded symbols, and the date range, make sure it has everyday's report. 
formatted_report AS (
    SELECT 
        d.dt_report,
        u.login_hash,
        u.server_hash,
        dus.symbol,
        u.currency
    FROM enabled_users u
    JOIN distinct_user_symbol dus
        ON u.login_hash = dus.login_hash
        AND u.server_hash = dus.server_hash
    CROSS JOIN date_range d
),
--- Calculate total volume of trades per user, per symbol, per symbol per day.
daily_volume AS (
    SELECT
        DATE(t.close_time) AS trade_date,
        t.login_hash,
        t.server_hash,
        t.symbol,
        SUM(volume) AS volumes_per_day
    FROM trades t
    GROUP BY DATE(t.close_time), t.login_hash, t.server_hash, t.symbol 
),
--- Count the number of trades per userer, per day.
daily_trades AS (
    SELECT
        DATE(t.close_time) AS trade_date,
        t.login_hash,
        COUNT(t.ticket_hash) AS trades_per_day
    FROM trades t
    GROUP BY DATE(t.close_time), t.login_hash
),
-- Accumulate trading metrics for each user, symbol, and date
accumulated_metrics AS (
    SELECT 
        fr.*,
        -- Sum of trade volumes over the past 7 days (rolling window).
        COALESCE(SUM(dv.volumes_per_day) OVER (
            PARTITION BY fr.login_hash, fr.server_hash, fr.symbol 
            ORDER BY fr.dt_report 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ), 0) AS sum_volume_prev_7d,
        -- Cumulative sum of all trade volumes since the user's first recorded trade.
        COALESCE(SUM(dv.volumes_per_day) OVER (
            PARTITION BY fr.login_hash, fr.server_hash, fr.symbol 
            ORDER BY fr.dt_report 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 0) AS sum_volume_prev_all,
        -- Sum of trade volumes specifically for August 2020.
        COALESCE(SUM(CASE 
            WHEN fr.dt_report >= '2020-08-01' AND fr.dt_report < '2020-09-01' 
            THEN dv.volumes_per_day 
            ELSE 0 
        END) OVER (
            PARTITION BY fr.login_hash, fr.server_hash, fr.symbol 
            ORDER BY fr.dt_report
        ), 0) AS sum_volume_2020_08,
        -- Count of trades over the past 7 days (rolling window)
        COALESCE(SUM(dt.trades_per_day) OVER (
            PARTITION BY fr.login_hash
            ORDER BY fr.dt_report 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ), 0) AS count_trades_prev_7d
    FROM formatted_report fr
    LEFT JOIN daily_volume dv
        ON fr.login_hash = dv.login_hash 
        AND fr.server_hash = dv.server_hash 
        AND fr.symbol = dv.symbol
        AND fr.dt_report = dv.trade_date
    LEFT JOIN daily_trades dt
        ON fr.dt_report = dt.trade_date 
        AND fr.login_hash = dt.login_hash
),
-- Find the first recorded trade date for each user-server-symbol pair..
first_trade_date AS (
    SELECT 
        login_hash,
        server_hash,
        symbol,
        MIN(close_time) AS date_first_trade
    FROM trades
    GROUP BY login_hash, server_hash, symbol
),
-- Query to get rankings, and row numbers
rank_row AS(
	SELECT 
	    a.*,
	    DENSE_RANK() OVER(partition by a.dt_report ORDER BY a.sum_volume_prev_7d DESC) AS rank_volume_symbol_prev_7d,   -- Partitioned by dt_report(date), meaning ranking for everyday.   
	    DENSE_RANK() OVER(partition by a.dt_report ORDER BY a.count_trades_prev_7d DESC) AS rank_count_prev_7d,         -- Partitioned by dt_report(date), meaning ranking for everyday.   
	    fd.date_first_trade,
	    ROW_NUMBER() OVER(ORDER BY a.dt_report,a.login_hash,a.server_hash,a.symbol) AS row_number
	FROM accumulated_metrics a
	JOIN first_trade_date fd
	    ON fd.login_hash = a.login_hash 
	    AND fd.server_hash = a.server_hash 
	    AND fd.symbol = a.symbol
)
-- Final query to extract relevant trade metrics and required features.
select
	dt_report,
	login_hash,
	server_hash,
	symbol,
	currency,
	sum_volume_prev_7d,
	sum_volume_prev_all,
	rank_volume_symbol_prev_7d,
	rank_count_prev_7d,
	sum_volume_2020_08,
	date_first_trade,
	row_number
FROM rank_row
ORDER BY row_number DESC;