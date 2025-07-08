
    
    

select
    id as unique_field,
    count(*) as n_records

from raw_crypto."raw_crypto_prices"
where id is not null
group by id
having count(*) > 1


