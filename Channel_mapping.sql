SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.limit.query.max.table.partition=50000;
SET hive.merge.sparkfiles=false;
SET hive.strict.checks.large.query=false;

SET spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps;
SET spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps;

SET hive.vectorized.execution.enabled=false;
SET hive.execution.engine=tez;
SET tez.queue.name=adtech_production_jobs;

-- TO prevent OOM error
SET spark.executor.memory=12288m;
SET spark.yarn.executor.memoryOverhead=3072;
SET hive.auto.convert.join=false;

use adtech_test;
DROP TABLE IF EXISTS dim_driver_channel PURGE;
CREATE TABLE dim_driver_channel stored as orc as
SELECT
  *
from
  mdh.enriched_attribution_store
where
  lower(entity_type) = 'driver'
  and event_type = 'Signup'
  and attribution_type = 'lca'
;
DROP TABLE IF EXISTS dim_client_channel PURGE;
CREATE TABLE dim_client_channel stored as orc as
SELECT
  *
from
  mdh.enriched_attribution_store
where
  lower(entity_type) = 'rider'
  and event_type = 'Signup'
  and attribution_type = 'lca'
;
DROP TABLE IF EXISTS channel_mapping_marketing_ad_mdl PURGE;

CREATE TABLE channel_mapping_marketing_ad_mdl stored as orc as
WITH country AS (
    SELECT
        country_id,
        max(mega_region) as mega_region
    FROM mdh.dim_city_mdl
    Group By 1
)
  --perform spend specific subchannel, channel, channel group transformations
  --the subchannel and channel transformations below come from the code which builds fact_marketing_spend
  --complication is that to generate subchannel, 4 different fields are used.  These 4 fields have various fill
  --rates across channels
  SELECT
        coalesce(dc.mega_region, country.mega_region) as mega_region,
        CASE
          --keyset for facebook needs to check for "nara"
          WHEN lower(dma.channel_name) = 'facebook' THEN
            concat(coalesce(dc.mega_region, country.mega_region,'missing_mega_region')
              , '-' , coalesce(dma.publisher_name,'missing_publisher')
              , '-' , coalesce(dma.source, 'missing_source')
              , '-' , coalesce(dma.channel_name, 'missing_channel')
              , '-' , coalesce(dma.channel_group, 'missing_group')
              , '-' , coalesce(CASE WHEN lower(dma.campaign_name) like '%nara%' then 'nara' ELSE coalesce(dma.publisher_name,'missing_publisher') END)
              , '-' , coalesce(dma.channel_sub_group, 'missing_sub_group')
          )
          ELSE
            concat(coalesce(dc.mega_region, country.mega_region,'missing_mega_region')
              , '-' , coalesce(dma.publisher_name,'missing_publisher')
              , '-' , coalesce(dma.source, 'missing_source')
              , '-' , coalesce(dma.channel_name, 'missing_channel')
              , '-' , coalesce(dma.channel_group, 'missing_group')
              , '-' , coalesce(dma.channel_sub_group, 'missing_sub_group')
            )
          END as key_set,
        --  FMS source aka subchannel logic
        CASE
            WHEN lower(dma.channel_name) in ('adwords', 'bing') THEN dma.publisher_name
            WHEN lower(dma.channel_name) = 'facebook' THEN
                CASE WHEN lower(dma.campaign_name) like '%nara%' then 'nara' ELSE dma.publisher_name END
            WHEN lower(dma.channel_name) = 'fetchme' and lower(dma.publisher_name) = 'twitter' THEN 'twitter'
            WHEN lower(dma.channel_name) = 'fetchme' and lower(dma.publisher_name) != 'twitter' THEN 'fetch'
            WHEN lower(dma.channel_name) = 'initiativeradio' THEN 'radio'
            WHEN lower(dma.channel_name) = 'initiativetv' THEN 'tv'
            WHEN lower(dma.channel_name) = 'initiativeooh' THEN 'ooh'
            WHEN lower(dma.channel_name) in ('inmobi','inmobi-additional') THEN 'inmobi'
            -- Mytarget was always categorizing everything under mytarget-ok, hence we will keep it that way in FMS
            WHEN lower(dma.channel_name) = 'mytarget' THEN 'mytarget-ok'
            WHEN lower(dma.channel_name) = 'yahoogemini' AND dma.publisher_name is NULL THEN 'ynative'
            WHEN lower(dma.channel_name) = 'yahoogemini' THEN dma.publisher_name
            WHEN lower(dma.channel_name) in ( 'yandex','twitter','jobcasedigital','mailru','latamoffline','omddigital') THEN dma.source
            WHEN lower(dma.channel_name) = 'apple' THEN dma.source
            WHEN lower(dma.channel_name) = 'doubleclickbidmanager' THEN 'doubleclick'
            WHEN lower(dma.channel_name) = 'impactradius' THEN dma.source
            WHEN lower(dma.channel_name) = 'clickcast'  and lower(dma.source) like 'ziprecruiter%' THEN 'ziprecruiter'
            WHEN lower(dma.channel_name) = 'clickcast'  and lower(dma.source) like 'jobs2careers%' THEN 'jobs2careers'
            WHEN lower(dma.channel_name) = 'yahoojp' THEN dma.source
            WHEN lower(dma.channel_name) in ('naver','forward3d') AND lower(dma.source) = 'forward3d-brand' AND lower(dma.channel_group) = 'search' THEN 'naver-brand'
            WHEN lower(dma.channel_name) in ('naver','forward3d') AND lower(dma.source) = 'forward3d-nonbrand' AND lower(dma.channel_group) = 'search' THEN 'naver-nonbrand'
            WHEN lower(dma.channel_name) in ('naver','forward3d') AND lower(dma.source) = 'forward3d-display' AND lower(dma.channel_group) = 'display' THEN 'naver-display'
            WHEN lower(dma.channel_name) = 'the_trade_desk' THEN dma.source
            ELSE lower(dma.channel_name)
        END as subchannel, --subchannel

        -- all FMS channel logic
        CASE
            WHEN dma.channel_group = 'Search' AND COALESCE(dma.channel_sub_group, '') != '' THEN
                CONCAT('Search-', initcap(dma.channel_sub_group))
            WHEN lower(dma.channel_name) in ('adwords', 'bing', 'admarketplace', 'yandex', 'apple') THEN
                CONCAT(dma.channel_group, coalesce(CONCAT('-', dma.channel_sub_group), ''))
            WHEN lower(dma.channel_name) = 'fetchme' and publisher_name = 'twitter' THEN
                'Social'
            WHEN lower(dma.channel_name) in ('initiativeooh', 'initiativeradio', 'initiativetv', 'omdcinema', 'omdprint', 'press', 'latamoffline') THEN
                'Mass-Media'
            WHEN lower(dma.channel_name) = 'mediamath' THEN
                'Programmatic Display'
            ELSE
                dma.channel_group
        END as channel, --channel

        -- these are all paid channel_groups by default
        'Paid' as channel_group,
        lower(dma.product_name) as product_name

    FROM euclid.dim_marketing_ad dma
      LEFT JOIN mdh.dim_city_mdl dc ON dc.city_id = cast(trim(dma.city_id) as int)
      LEFT JOIN country ON country.country_id = cast(trim(dma.country_id) as int)
    GROUP BY 1,2,3,4,5,6
;


DROP TABLE IF EXISTS channel_mapping_rider_mdl PURGE;
CREATE TABLE channel_mapping_rider_mdl stored as orc as
WITH country AS (
    SELECT
        country_id,
        max(mega_region) as mega_region
    FROM mdh.dim_city_mdl
    Group By 1
)
  SELECT
        -- perform rider specific subchannel, channel, channel group transformations
        coalesce(dc.mega_region, country.mega_region, 'missing_mega_region') as mega_region,
        concat(coalesce(dc.mega_region, country.mega_region,'missing_mega_region')
          , '-' , coalesce(lower(sub_channel), 'missing_subchannel')
          , '-' , coalesce(channel,'missing_channel')
          , '-' , coalesce(channel_group,'missing_group')
        ) AS key_set,
        lower(sub_channel) AS subchannel,
        channel AS channel,
        channel_group AS channel_group
    FROM dwh.dim_client AS riders
      LEFT JOIN dim_client_channel AS channel ON riders.user_uuid = channel.user_uuid
      LEFT JOIN mdh.dim_city_mdl dc ON dc.city_id = riders.signup_city_id
      LEFT JOIN country ON country.country_id = dc.country_id
    GROUP BY 1,2,3,4,5
;


DROP TABLE IF EXISTS channel_mapping_driver_mdl PURGE;
CREATE TABLE channel_mapping_driver_mdl stored as orc as
WITH country AS (
    SELECT
        country_id,
        max(mega_region) as mega_region
    FROM mdh.dim_city_mdl
    Group By 1
)
  SELECT
        -- perform rider specific subchannel, channel, channel group transformations
        coalesce(dc.mega_region, country.mega_region, 'missing_mega_region') as mega_region,
        concat(coalesce(dc.mega_region, country.mega_region,'missing_mega_region')
          , '-' , coalesce(lower(sub_channel), 'missing_subchannel')
          , '-' , coalesce(channel,'missing_channel')
          , '-' , coalesce(channel_group,'missing_group')
        ) AS key_set,
        lower(sub_channel) AS subchannel,
        channel AS channel,
        channel_group AS channel_group
    FROM dwh.dim_driver AS drivers
      LEFT JOIN dim_driver_channel AS channels ON drivers.driver_uuid = channels.driver_uuid
      -- LEFT JOIN euclid.dim_driver_channel_new as channels ON drivers.driver_uuid = channels.driver_uuid
      LEFT JOIN mdh.dim_city_mdl dc ON dc.city_id = drivers.signup_city_id
      LEFT JOIN country ON country.country_id = dc.country_id
    GROUP BY 1,2,3,4,5
;


DROP TABLE IF EXISTS analytics_channel_mapping_mdl_tmp PURGE;
CREATE TABLE analytics_channel_mapping_mdl_tmp STORED AS ORC AS
WITH union_set AS (
SELECT
  mega_region,
  key_set,
  subchannel,
  channel,
  channel_group,
  case when product_name = 'rider' then true else false end as is_rider,
  case when product_name = 'driver' then true else false end as is_driver
  FROM channel_mapping_marketing_ad_mdl
UNION all
SELECT
  mega_region,
  key_set,
  subchannel,
  channel,
  channel_group,
  true as is_rider,
  false as is_driver
  FROM channel_mapping_rider_mdl
UNION all
SELECT
  mega_region,
  key_set,
  subchannel,
  channel,
  channel_group,
  false as is_rider,
  true as is_driver
  FROM channel_mapping_driver_mdl

),
--selects distinct mega_regions from dwh.dim_city
megaregion AS (
  SELECT
    DISTINCT mega_region
  FROM
    dwh.dim_city
  WHERE
    mega_region IS NOT NULL
),

craigslist_adposter as (
  SELECT
    '-missing_publisher-craigslist-craigslist-adposter-JobBoards-before_20170425-missing_sub_group' as key_set,
    'craigslist_adposter' as subchannel
  UNION ALL
  SELECT
    '-missing_publisher-craigslist-craigslist-adposter-JobBoards-on_after_20170425-missing_sub_group' as key_set,
    'craigslist' as subchannel
)

SELECT
  -- execute any common subchannel, channel, channel group transformations which can be applied regardless of source
  mega_region,
  key_set,
  lower(CASE
      WHEN lower(subchannel) = 'yahoo ad manager' THEN 'ynative'
      WHEN subchannel = 'ironsource-global' THEN 'ironsource' --from marketing spend
      WHEN subchannel = 'tsp' THEN 'twitter' --from marketing spend
      WHEN subchannel like '%radio%' THEN 'radio' --from driver
      WHEN subchannel like '%tv%' THEN 'tv' --from driver
      WHEN subchannel is null AND  union_set.channel_group IN('Organic','Partner_account') THEN 'Organic'
      WHEN subchannel is null AND  union_set.channel_group IN('Polymorph') THEN 'Polymorph'
      WHEN subchannel is null AND  union_set.channel_group IN('Other_Promo',  'HQ_Marketing_Promo','Promo','DSR') THEN 'Promo'
      WHEN subchannel is null AND  union_set.channel_group IN ('Referral') THEN 'Referral'
      ELSE subchannel
  END) AS subchannel,
  CASE
    WHEN mega_region = 'APACX' AND lower(subchannel) IN('ynative', 'jampp','yahoo ad manager') THEN 'Programmatic Display'
    WHEN mega_region = 'APACX' AND lower(subchannel) IN('smartly.io') THEN 'Social'
    WHEN lower(subchannel) IN('jampp', 'liftoff') THEN 'Programmatic Display'
    WHEN subchannel = 'tv' THEN 'TV' --from marketing spend
    WHEN subchannel = 'radio' THEN 'Radio' --from marketing spend
    WHEN subchannel = 'youtube' THEN 'Video' --from driver
    WHEN lower(channel) = 'mass-media' THEN 'Mass-Media'
    WHEN channel is null AND  union_set.channel_group IN('Organic','Partner_account') THEN 'Organic'
    WHEN channel is null AND  union_set.channel_group IN('Polymorph') THEN 'Polymorph'
    WHEN channel is null AND  union_set.channel_group IN('Other_Promo',  'HQ_Marketing_Promo','Promo','DSR') THEN 'Promo'
    WHEN channel is null AND  union_set.channel_group IN ('Referral') THEN 'Referral'
    ELSE channel
  END as channel,
  CASE
    WHEN union_set.channel = 'JobBoards' THEN 'Paid_Other'
    WHEN union_set.channel_group = 'Paid' THEN
      CASE WHEN union_set.channel in ('Search-Brand', 'Search-Driver') THEN 'Paid_Brand'
        ELSE 'Paid_Other'
      END
    WHEN union_set.channel_group IN ('Referral') THEN 'Referral'
    WHEN union_set.channel_group IN ('Other_Promo',  'HQ_Marketing_Promo','DSR','Promo') THEN 'Promo'
    WHEN union_set.channel_group IN ('R2D') THEN 'Polymorph'
    WHEN union_set.channel_group IN ('Organic','Offline','Partner_account') THEN 'Organic'
    WHEN union_set.channel_group IN ('Dost') THEN 'Dost'
    WHEN union_set.channel IN ('Dost') THEN 'Dost'
    WHEN union_set.channel IN ('R2D') THEN 'Polymorph'
    WHEN union_set.channel IN ('Organic') THEN 'Organic'
    WHEN union_set.channel IN ('Referral') THEN 'Referral'
    WHEN union_set.channel IN ('Offline') THEN 'Organic'
    ELSE 'Others'
  END as channel_group,
  current_timestamp as `_etl_load_timestamp`,
  max(is_rider) as is_rider,
  max(is_driver) as is_driver
  FROM union_set
  GROUP BY 1,2,3,4,5

UNION ALL

SELECT
  megaregion.mega_region as mega_region,
  concat(megaregion.mega_region, craigslist_adposter.key_set) as key_set,
  craigslist_adposter.subchannel as subchannel,
  'JobBoards' as channel,
  'Paid_Other' as channel_group,
  current_timestamp as `_etl_load_timestamp`,
  false as is_rider,
  true as is_driver
  FROM craigslist_adposter
  CROSS JOIN megaregion
  ;

DROP TABLE IF EXISTS adtech_stage.analytics_channel_mapping_mdl PURGE;
ALTER TABLE analytics_channel_mapping_mdl_tmp rename to adtech_stage.analytics_channel_mapping_mdl_test;
