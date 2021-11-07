SELECT FIRST(open, dt)
FROM stock_price
WHERE stock_id = 15
    AND DATE(dt) = '2020-10-01';
SELECT stock_id,
    symbol,
    SUM(volume) AS total_volume
FROM stock_price
    JOIN stock on stock.id = stock_price.stock_id
WHERE DATE(dt) = '2021-02-05'
GROUP BY stock_id,
    symbol
ORDER BY total_volume ASC
LIMIT 10;
-- histogram
SELECT HISTOGRAM(close, 50, 52, 4)
FROM stock_price
WHERE stock_id = 15;
-- time bucket
SELECT TIME_BUCKET(INTERVAL '1 hour', dt) AS bucket,
    FIRST(open, dt),
    MAX(high),
    MIN(low),
    last(close, dt)
FROM stock_price
WHERE stock_id = 15
GROUP BY bucket
ORDER BY bucket DESC;
-- time bucket gapfill
SELECT TIME_BUCKET_GAPFILL('5 min', dt, NOW() - INTERVAL '5 day', NOW()) AS bar,
    LOCF(AVG(close)) as close2
FROM stock_price
WHERE stock_id = 15
    and dt > now() - INTERVAL '5 day'
GROUP BY bar,
    stock_id
ORDER BY bar;
-- Materialized Views
CREATE MATERIALIZED VIEW hourly_bars WITH (timescaledb.continuous) AS
SELECT stock_id,
    TIME_BUCKET(INTERVAL '1 hour', dt) AS bucket,
    FIRST(open, dt) as open,
    MAX(high) as high,
    MIN(low) as low,
    LAST(close, dt) as close,
    SUM(volume) as volume
FROM stock_price
GROUP BY stock_id,
    bucket;
CREATE MATERIALIZED VIEW daily_bars WITH (timescaledb.continuous) AS
SELECT stock_id,
    TIME_BUCKET(INTERVAL '1 day', dt) AS bucket,
    FIRST(open, dt) as open,
    MAX(high) as high,
    MIN(low) as low,
    LAST(close, dt) as close,
    SUM(volume) as volume
FROM stock_price
GROUP BY stock_id,
    bucket;
-- 20 Day Moving Average
SELECT AVG(close)
FROM (
        SELECT *
        FROM daily_bars
        WHERE stock_id = 15
        ORDER BY bucket DESC
        LIMIT 20
    ) a;
-- Moving average with window functions
SELECT bucket,
    AVG(close) OVER (
        ORDER BY bucket ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma_20
FROM daily_bars
WHERE stock_id = 15
ORDER BY bucket DESC;
-- Highest daily returns
WITH prev_day_closing AS (
    SELECT stock_id,
        bucket,
        close,
        LEAD(close) OVER (
            PARTITION BY stock_id
            ORDER BY bucket DESC
        ) AS prev_day_closing_price
    FROM daily_bars
),
daily_factor AS (
    SELECT stock_id,
        bucket,
        close / prev_day_closing_price AS daily_factor
    FROM prev_day_closing
)
SELECT bucket,
    LAST(stock_id, daily_factor) AS stock_id,
    MAX(daily_factor) AS max_daily_factor
FROM daily_factor
    JOIN stock ON stock.id = daily_factor.stock_id
GROUP BY bucket
ORDER BY bucket DESC,
    max_daily_factor DESC;
-- Bullish Engulfing Pattern
SELECT *
FROM (
        SELECT bucket,
            open,
            close,
            stock_id,
            LAG(close, 1) OVER (
                PARTITION BY stock_id
                ORDER BY bucket
            ) previous_close,
            LAG(open, 1) OVER (
                PARTITION BY stock_id
                ORDER BY bucket
            ) previous_open
        FROM daily_bars
    ) a
WHERE previous_close < previous_open
    AND close > previous_open
    AND open < previous_close
    AND bucket = '2021-02-05';
-- Three Bar Breakout
SELECT *
FROM (
        SELECT day,
            close,
            volume,
            stock_id,
            LAG(close, 1) OVER (
                PARTITION BY stock_id
                ORDER BY day
            ) previous_close,
            LAG(volume, 1) OVER (
                PARTITION BY stock_id
                ORDER BY day
            ) previous_volume,
            LAG(close, 2) OVER (
                PARTITION BY stock_id
                ORDER BY day
            ) previous_previous_close,
            LAG(volume, 2) OVER (
                PARTITION BY stock_id
                ORDER BY day
            ) previous_previous_volume,
            LAG(close, 3) OVER (
                PARTITION BY stock_id
                ORDER BY day
            ) previous_previous_previous_close,
            LAG(volume, 3) OVER (
                PARTITION BY stock_id
                ORDER BY day
            ) previous_previous_previous_volume
        FROM daily_bars
    ) a
WHERE close > previous_previous_previous_close
    and previous_close < previous_previous_close
    and previous_close < previous_previous_previous_close
    AND volume > previous_volume
    and previous_volume < previous_previous_volume
    and previous_previous_volume < previous_previous_previous_volume
    AND day = '2021-02-05';