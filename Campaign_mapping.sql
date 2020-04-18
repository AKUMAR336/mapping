SET hive.exec.compress.output=true;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.strict.checks.large.query=false;
SET hive.cbo.enable=false;
SET hive.vectorized.execution.enabled=false;

USE adtech_test;
--get list of valid objective names from dim marketing ad for later verification
DROP TABLE IF EXISTS campaign_dma_spend_types_mdl PURGE;
CREATE TABLE campaign_dma_spend_types_mdl stored as orc as
SELECT DISTINCT
    UPPER(objective_name) spend_type
from euclid.dim_marketing_ad
where objective_name is not null
;


--get list of campaign values from dim marketing ad
DROP TABLE IF EXISTS dma_campaign_grain_mdl PURGE;
CREATE TABLE dma_campaign_grain_mdl stored as orc as
select
    'dma' as origin,
    campaign_name as dma_campaign,
    min(country_id) as dma_country_id,
    min(city_id) as dma_city_id,
    max(product_name) as dma_product,
    max(billing_type) as dma_billing,
    max(os_targeted) as dma_os_targeted,
    max(objective_name) as dma_objective,
    max(publisher_name) as dma_publisher_name
from euclid.dim_marketing_ad
where coalesce(campaign_name, '') != ''
group by 1,2
;


-- No need to merge temp files and no need to do stats gathering
SET hive.merge.sparkfiles=false;
SET hive.stats.autogather=false;

-- To avoid stats miscomputation specifically in `mdh.dim_driver_channel`
--analyze table mdh.enriched_attribution_store compute statistics noscan;

--parse campaign name values from each table
DROP TABLE IF EXISTS campaign_parser_mdl PURGE;
CREATE TABLE campaign_parser_mdl stored as orc as
with cleaner as (
    select
        origin,
        row_uuid,
        coalesce(
            split(coalesce(split(raw_campaign, '\\|\\|')[1], raw_campaign), 'campaign_')[1],
            split(raw_campaign, '\\|\\|')[1],
            raw_campaign
        ) as cleaned_campaign
    from (
        select
            'rider' as origin,
            entity_id as row_uuid,
            campaign_name as raw_campaign
        from  mdh.enriched_attribution_store
        where coalesce(campaign_name, '') != ''
        and lower(entity_type) = 'rider'
        and event_type = 'Signup'
        and attribution_type = 'lca'
        union all
        select
            'driver' as origin,
            entity_id as row_uuid,
            campaign_tag as raw_campaign
        from  mdh.enriched_attribution_store
        where coalesce(campaign_tag, '') != ''
        and lower(entity_type) = 'driver'
        and event_type = 'Signup'
         and attribution_type = 'lca'
        union all
        select
            'eats_order' as origin,
            event_uuid as row_uuid,
            campaign_name as raw_campaign
        from  mdh.enriched_attribution_store
        where coalesce(campaign_name, '') != ''
        and  lower(entity_type) = 'eater'
         and event_type in ('Order', 'FirstOrder')
        and attribution_type = 'lca'
    ) sources
),
cm_parser as (
    select
        c.origin,
        c.row_uuid,
        cm.sub_channel publisher,
        cm.country_id country,
        cm.city_id city,
        NULL geo, -- expects eg. US-Philadelphia but cm.region is eg. US_CANADA
        cm.product product,
        cm.platform ad_type,
        cm.objective objective,
        cm.bid_type billing_type,
        cm.language language,
        NULL targeting_group
    from cleaner c
    join euclid.managed_campaign_details cm
        on regexp_extract(lower(c.cleaned_campaign), '^cm([0-9]+)-', 1) = cm.id
),
underscore_parser as (
    select
        origin,
        row_uuid,
        split(cleaned_campaign, '_')[0] as publisher,
        split(cleaned_campaign, '_')[1] as country,
        split(cleaned_campaign, '_')[2] as city,
        split(cleaned_campaign, '_')[3] as geo,
        split(cleaned_campaign, '_')[4] as product,
        split(cleaned_campaign, '_')[5] as ad_type,
        split(cleaned_campaign, '_')[6] as objective,
        split(cleaned_campaign, '_')[7] as billing_type,
        split(cleaned_campaign, '_')[8] as language,
        split(cleaned_campaign, '_')[9] as targeting_group
    from cleaner
    where
        cleaned_campaign rlike '^[^_]*?_[0-9]+_-?[0-9]+'
),
caret_parser as (
    select
        origin,
        row_uuid,
        NULL as publisher,
        split(split(cleaned_campaign, '>')[5], '-')[1] as country,
        split(split(cleaned_campaign, '>')[6], '-')[1] as city,
        NULL as geo,
        split(cleaned_campaign, '>')[2] as product,
        split(cleaned_campaign, '>')[3] as ad_type,
        NULL as objective,      -- split(cleaned_campaign, '>')[7] not always there
        NULL as billing_type,
        NULL as language,
        NULL as targeting_group
    from cleaner
    where lower(cleaned_campaign) like '%>city-%'
)
select distinct
    c.origin,
    c.row_uuid,
    c.cleaned_campaign,
    coalesce(cm.publisher, up.publisher, cp.publisher) as publisher,
    coalesce(cm.country, up.country, cp.country) as country,
    coalesce(cm.city, up.city, cp.city) as city,
    coalesce(cm.geo, up.geo, cp.geo) as geo,
    coalesce(cm.product, up.product, cp.product) as product,
    coalesce(cm.ad_type, up.ad_type, cp.ad_type) as ad_type,
    --case statement to set invalid parsed spend types to Other
    --valid spend types are those which exist in dim_marketing_ad
    CASE
        WHEN cm_dma_spend_type.spend_type is not null THEN cm.objective
        WHEN up_dma_spend_type.spend_type is not null THEN up.objective
        WHEN cp_dma_spend_type.spend_type is not null THEN cp.objective
        ELSE 'OTHERS'
    END as objective,
    coalesce(cm.billing_type, up.billing_type, cp.billing_type) as billing_type,
    coalesce(cm.language, up.language, cp.language) as language,
    coalesce(cm.targeting_group, up.targeting_group, cp.targeting_group) as targeting_group
from cleaner c
left join cm_parser cm
    on c.origin = cm.origin and c.row_uuid = cm.row_uuid
left join underscore_parser up
    on c.origin = up.origin and c.row_uuid = up.row_uuid
left join caret_parser cp
    on c.origin = cp.origin and c.row_uuid = cp.row_uuid
left join campaign_dma_spend_types_mdl as cm_dma_spend_type
    on UPPER(cm.objective) = cm_dma_spend_type.spend_type
left join campaign_dma_spend_types_mdl as up_dma_spend_type
    on UPPER(up.objective) = up_dma_spend_type.spend_type
left join campaign_dma_spend_types_mdl as cp_dma_spend_type
    on UPPER(cp.objective) = cp_dma_spend_type.spend_type
;
-- Resetting back
SET hive.merge.sparkfiles=true;
SET hive.stats.autogather=true;


DROP TABLE IF EXISTS analytics_rider_campaign_parser_uuid_mdl_tmp PURGE;
CREATE TABLE analytics_rider_campaign_parser_uuid_mdl_tmp stored as orc as
select
    parser.row_uuid as user_uuid,
    coalesce(parser.cleaned_campaign, dma.dma_campaign) as campaign_name,
    parser.cleaned_campaign as parsed_cleaned_campaign,
    parser.publisher as parsed_publisher,
    parser.country as parsed_country_id,
    parser.city as parsed_city_id,
    parser.geo as parsed_geo,
    parser.product as parsed_product,
    parser.ad_type as parsed_ad_type,
    parser.objective as parsed_objective,
    parser.billing_type as parsed_billing_type,
    parser.language as parsed_language,
    parser.targeting_group as parsed_targeting_group,
    dma.dma_campaign as dma_campaign,
    dma.dma_country_id as dma_country_id,
    dma.dma_city_id as dma_city_id,
    dma.dma_product as dma_product,
    dma.dma_billing as dma_billing,
    dma.dma_os_targeted as dma_os_targeted,
    dma.dma_objective as dma_objective,
    dma.dma_publisher_name as dma_publisher_name,
    current_timestamp as `_etl_load_timestamp`
    from campaign_parser_mdl parser
    left join dma_campaign_grain_mdl dma on parser.cleaned_campaign = dma.dma_campaign
    where
        coalesce(parser.cleaned_campaign, dma.dma_campaign) != ''
        and parser.origin = 'rider'
;

DROP TABLE IF EXISTS analytics_rider_campaign_parser_uuid_mdl PURGE;
ALTER TABLE analytics_rider_campaign_parser_uuid_mdl_tmp rename to analytics_rider_campaign_parser_uuid_mdl;


DROP TABLE IF EXISTS analytics_driver_campaign_parser_uuid_mdl_tmp PURGE;
CREATE TABLE analytics_driver_campaign_parser_uuid_mdl_tmp stored as orc as
select
    parser.row_uuid as driver_uuid,
    coalesce(parser.cleaned_campaign, dma.dma_campaign) as campaign_name,
    parser.cleaned_campaign as parsed_cleaned_campaign,
    parser.publisher as parsed_publisher,
    parser.country as parsed_country_id,
    parser.city as parsed_city_id,
    parser.geo as parsed_geo,
    parser.product as parsed_product,
    parser.ad_type as parsed_ad_type,
    parser.objective as parsed_objective,
    parser.billing_type as parsed_billing_type,
    parser.language as parsed_language,
    parser.targeting_group as parsed_targeting_group,
    dma.dma_campaign as dma_campaign,
    dma.dma_country_id as dma_country_id,
    dma.dma_city_id as dma_city_id,
    dma.dma_product as dma_product,
    dma.dma_billing as dma_billing,
    dma.dma_os_targeted as dma_os_targeted,
    dma.dma_objective as dma_objective,
    dma.dma_publisher_name as dma_publisher_name,
    current_timestamp as `_etl_load_timestamp`
    from campaign_parser_mdl parser
    left join dma_campaign_grain_mdl dma on parser.cleaned_campaign = dma.dma_campaign
    where
        coalesce(parser.cleaned_campaign, dma.dma_campaign) != ''
        and parser.origin = 'driver'
;

DROP TABLE IF EXISTS analytics_driver_campaign_parser_uuid_mdl PURGE;
ALTER TABLE analytics_driver_campaign_parser_uuid_mdl_tmp rename to analytics_driver_campaign_parser_uuid_mdl;


DROP TABLE IF EXISTS analytics_eater_campaign_parser_uuid_mdl_tmp PURGE;
CREATE TABLE analytics_eater_campaign_parser_uuid_mdl_tmp stored as orc as
select
    parser.row_uuid as workflow_uuid,
    coalesce(parser.cleaned_campaign, dma.dma_campaign) as campaign_name,
    parser.cleaned_campaign as parsed_cleaned_campaign,
    parser.publisher as parsed_publisher,
    parser.country as parsed_country_id,
    parser.city as parsed_city_id,
    parser.geo as parsed_geo,
    parser.product as parsed_product,
    parser.ad_type as parsed_ad_type,
    parser.objective as parsed_objective,
    parser.billing_type as parsed_billing_type,
    parser.language as parsed_language,
    parser.targeting_group as parsed_targeting_group,
    dma.dma_campaign as dma_campaign,
    dma.dma_country_id as dma_country_id,
    dma.dma_city_id as dma_city_id,
    dma.dma_product as dma_product,
    dma.dma_billing as dma_billing,
    dma.dma_os_targeted as dma_os_targeted,
    dma.dma_objective as dma_objective,
    dma.dma_publisher_name as dma_publisher_name,
    current_timestamp as `_etl_load_timestamp`
    from campaign_parser_mdl parser
    left join dma_campaign_grain_mdl dma on parser.cleaned_campaign = dma.dma_campaign
    where
        coalesce(parser.cleaned_campaign, dma.dma_campaign) != ''
        and parser.origin = 'eats_order'
;

DROP TABLE IF EXISTS adtech_stage.analytics_eater_campaign_parser_uuid_mdl PURGE;
ALTER TABLE analytics_eater_campaign_parser_uuid_mdl_tmp rename to adtech_stage.analytics_eater_campaign_parser_uuid_mdl;


--merge parsed data with matching dim_marketing_ad data when found
DROP TABLE IF EXISTS analytics_campaign_mapping_mdl_tmp PURGE;
CREATE TABLE analytics_campaign_mapping_mdl_tmp stored as orc AS
select
    coalesce(parser.cleaned_campaign, dma.dma_campaign) as campaign_name,
    parser.cleaned_campaign as parsed_cleaned_campaign,
    parser.publisher as parsed_publisher,
    parser.country as parsed_country_id,
    parser.city as parsed_city_id,
    parser.geo as parsed_geo,
    parser.product as parsed_product,
    parser.ad_type as parsed_ad_type,
    parser.objective as parsed_objective,
    parser.billing_type as parsed_billing_type,
    parser.language as parsed_language,
    parser.targeting_group as parsed_targeting_group,
    dma.dma_campaign as dma_campaign,
    dma.dma_country_id as dma_country_id,
    dma.dma_city_id as dma_city_id,
    dma.dma_product as dma_product,
    dma.dma_billing as dma_billing,
    dma.dma_os_targeted as dma_os_targeted,
    dma.dma_objective as dma_objective,
    dma.dma_publisher_name as dma_publisher_name,
    current_timestamp as `_etl_load_timestamp`,
    coalesce(parser.origin, dma.origin) as origin
from (
    select distinct
        origin,
        cleaned_campaign,
        publisher,
        country,
        city,
        geo,
        product,
        ad_type,
        objective,
        billing_type,
        language,
        targeting_group
    from campaign_parser_mdl
    where origin in ('rider', 'driver')
) parser
full outer join dma_campaign_grain_mdl dma on parser.cleaned_campaign = dma.dma_campaign
WHERE
    coalesce(parser.cleaned_campaign, dma.dma_campaign) != ''
;

DROP TABLE adtech_stage.analytics_campaign_mapping_mdl PURGE;
ALTER TABLE analytics_campaign_mapping_mdl_tmp rename to adtech_stage.analytics_campaign_mapping_mdl;
