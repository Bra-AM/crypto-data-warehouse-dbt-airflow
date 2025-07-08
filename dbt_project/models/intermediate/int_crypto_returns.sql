{{ config(materialized='table') }}

with price_data as (
    select * from stg_crypto_prices
),

-- Calculate returns and price movements (PhD Math: Log Returns for Mathematical Properties)
returns_calculation as (
    select 
        symbol,
        price_timestamp,
        price_usd,
        volume_24h,
        
        -- Simple returns (traditional finance)
        (price_usd / lag(price_usd) over (
            partition by symbol 
            order by price_timestamp
        ) - 1) as simple_return,
        
        -- Log returns (PhD approach: better for mathematical analysis)
        -- Natural log of price ratios - mathematically superior for analysis
        ln(price_usd / lag(price_usd) over (
            partition by symbol 
            order by price_timestamp
        )) as log_return,
        
        -- Price differences (absolute change)
        price_usd - lag(price_usd) over (
            partition by symbol 
            order by price_timestamp
        ) as price_change_usd,
        
        -- Volume change analysis
        (volume_24h / lag(volume_24h) over (
            partition by symbol 
            order by price_timestamp
        ) - 1) as volume_change_pct
        
    from price_data
),

-- Advanced volatility calculations (PhD Math: Multiple Time Windows)
volatility_metrics as (
    select 
        *,
        
        -- 7-day rolling volatility (standard deviation of log returns)
        -- Using log returns is mathematically more robust
        case 
            when count(*) over (
                partition by symbol 
                order by price_timestamp 
                rows between 6 preceding and current row
            ) >= 7 then
                sqrt(var_pop(log_return) over (
                    partition by symbol 
                    order by price_timestamp 
                    rows between 6 preceding and current row
                ))
            else null 
        end as volatility_7d,
        
        -- 30-day rolling volatility
        case 
            when count(*) over (
                partition by symbol 
                order by price_timestamp 
                rows between 29 preceding and current row
            ) >= 30 then
                sqrt(var_pop(log_return) over (
                    partition by symbol 
                    order by price_timestamp 
                    rows between 29 preceding and current row
                ))
            else null 
        end as volatility_30d,
        
        -- Mean returns for risk-adjusted metrics
        avg(log_return) over (
            partition by symbol 
            order by price_timestamp 
            rows between 29 preceding and current row
        ) as mean_return_30d,
        
        -- Moving averages (technical analysis)
        avg(price_usd) over (
            partition by symbol 
            order by price_timestamp 
            rows between 6 preceding and current row
        ) as sma_7d,
        
        avg(price_usd) over (
            partition by symbol 
            order by price_timestamp 
            rows between 29 preceding and current row
        ) as sma_30d
        
    from returns_calculation
),

-- Advanced statistical measures (PhD Math: Risk Metrics and Outlier Detection)
statistical_analysis as (
    select 
        *,
        
        -- Sharpe Ratio approximation (risk-adjusted returns)
        -- PhD approach: Using log returns and proper volatility scaling
        case 
            when volatility_30d > 0 and volatility_30d is not null
            then mean_return_30d / volatility_30d
            else null 
        end as sharpe_ratio_30d,
        
        -- Z-score for outlier detection (normalized returns)
        -- PhD statistical approach: standardized returns
        case 
            when volatility_30d > 0 and volatility_30d is not null
            then (log_return - mean_return_30d) / volatility_30d
            else null 
        end as return_zscore,
        
        -- Technical indicators
        case 
            when sma_7d > sma_30d then 'bullish'
            when sma_7d < sma_30d then 'bearish'
            else 'neutral'
        end as trend_signal,
        
        -- Relative Strength Index (RSI) - simplified version
        -- PhD approach: Mathematical momentum indicator
        case 
            when avg(case when log_return > 0 then log_return else 0 end) over (
                partition by symbol 
                order by price_timestamp 
                rows between 13 preceding and current row
            ) = 0 then 0
            when avg(case when log_return < 0 then abs(log_return) else 0 end) over (
                partition by symbol 
                order by price_timestamp 
                rows between 13 preceding and current row
            ) = 0 then 100
            else 100 - (100 / (1 + 
                avg(case when log_return > 0 then log_return else 0 end) over (
                    partition by symbol 
                    order by price_timestamp 
                    rows between 13 preceding and current row
                ) / 
                avg(case when log_return < 0 then abs(log_return) else 0 end) over (
                    partition by symbol 
                    order by price_timestamp 
                    rows between 13 preceding and current row
                )
            ))
        end as rsi_14d
        
    from volatility_metrics
),

-- Final mathematical model with advanced risk metrics
final_model as (
    select 
        *,
        
        -- Value at Risk (VaR) at 95% confidence level
        -- PhD approach: Using normal distribution assumption
        case 
            when volatility_30d is not null and mean_return_30d is not null
            then mean_return_30d - (1.645 * volatility_30d)  -- 95% VaR
            else null 
        end as var_95_1d,
        
        -- Maximum drawdown calculation
        max(price_usd) over (
            partition by symbol 
            order by price_timestamp 
            rows unbounded preceding
        ) as running_max_price,
        
        -- Risk categorization based on statistical analysis
        case 
            when abs(return_zscore) > 3 then 'extreme_outlier'
            when abs(return_zscore) > 2 then 'moderate_outlier'
            when volatility_30d > 0.05 then 'high_volatility'
            when volatility_30d > 0.02 then 'medium_volatility'
            else 'low_volatility'
        end as risk_category,
        
        -- Market regime detection (PhD approach: Statistical classification)
        case 
            when volatility_30d > 0.04 and mean_return_30d < -0.01 then 'bear_high_vol'
            when volatility_30d > 0.04 and mean_return_30d > 0.01 then 'bull_high_vol'
            when volatility_30d <= 0.02 and mean_return_30d > 0.005 then 'bull_low_vol'
            when volatility_30d <= 0.02 and mean_return_30d < -0.005 then 'bear_low_vol'
            else 'neutral'
        end as market_regime
        
    from statistical_analysis
)

-- Final output with all mathematical transformations
select 
    symbol,
    price_timestamp,
    price_usd,
    volume_24h,
    simple_return,
    log_return,
    price_change_usd,
    volume_change_pct,
    volatility_7d,
    volatility_30d,
    mean_return_30d,
    sma_7d,
    sma_30d,
    sharpe_ratio_30d,
    return_zscore,
    trend_signal,
    rsi_14d,
    var_95_1d,
    running_max_price,
    -- Maximum drawdown percentage
    case 
        when running_max_price > 0 
        then ((price_usd - running_max_price) / running_max_price) * 100
        else 0 
    end as max_drawdown_pct,
    risk_category,
    market_regime,
    
    -- Cumulative returns using log returns (PhD approach)
    exp(sum(coalesce(log_return, 0)) over (
        partition by symbol 
        order by price_timestamp 
        rows unbounded preceding
    )) - 1 as cumulative_return_pct
    
from final_model
where log_return is not null  -- Remove first row with null returns
