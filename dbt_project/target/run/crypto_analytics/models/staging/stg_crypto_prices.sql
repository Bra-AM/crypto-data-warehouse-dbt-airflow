
  
    
    
    create  table main."stg_crypto_prices"
    as
        -- models/staging/stg_crypto_prices.sql


with raw_data as (
    select 
        symbol,
        timestamp,
        price,
        volume,
        market_cap,
        extracted_at
    from raw_crypto_prices  -- Direct table reference instead of source
),

cleaned_data as (
    select 
        upper(trim(symbol)) as symbol,
        datetime(timestamp) as price_timestamp,
        cast(price as real) as price_usd,
        cast(volume as real) as volume_24h,
        cast(coalesce(market_cap, 0) as real) as market_cap_usd,
        datetime(extracted_at) as data_extracted_at,
        
        -- Data quality flags
        case 
            when price <= 0 then 'invalid_price'
            when volume < 0 then 'invalid_volume'
            else 'valid'
        end as data_quality_flag
        
    from raw_data
)

select * from cleaned_data
where data_quality_flag = 'valid'

  