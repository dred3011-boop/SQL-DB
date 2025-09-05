WITH new_cte AS (
	SELECT ad_date, url_parameters,
	COALESCE(spend, 0) AS spend,
	COALESCE(impressions, 0) AS impressions,
	COALESCE(reach, 0) AS reach,
	COALESCE(clicks, 0) AS clicks,
	COALESCE(leads, 0) AS leads,
	COALESCE(value, 0) AS value
FROM facebook_ads_basic_daily

UNION ALL

	SELECT ad_date, url_parameters,
	COALESCE(spend, 0) AS spend,
	COALESCE(impressions, 0) AS impressions,
	COALESCE(reach, 0) AS reach,
	COALESCE(clicks, 0) AS clicks,
	COALESCE(leads, 0) AS leads,
	COALESCE(value, 0) AS value
FROM google_ads_basic_daily
),
monthly_data AS (
SELECT date_trunc('month', ad_date)::DATE AS ad_month,
		
	CASE WHEN lower(substring(url_parameters, 'utm_campaign=([^&]+)')) = 'nan' THEN NULL 
	     ELSE lower(public.urldecode(substring(url_parameters, 'utm_campaign=([^&]+)')))
	END AS utm_campaign,
	
	
	sum(spend) AS total_spend,
	sum(impressions) AS total_impressions,
	sum(clicks) AS total_clicks,
	CASE WHEN sum(clicks) >0 
	THEN sum(spend)::NUMERIC / sum(clicks) END AS CPC,
	CASE WHEN sum(impressions) >0
	THEN sum(clicks)::NUMERIC / sum(impressions) END AS CTR,
	CASE WHEN sum(impressions) >0
	THEN 1000*sum(spend)::NUMERIC / sum(impressions) ELSE 0 END AS  CPM,
	CASE WHEN sum(spend) >0
	THEN sum(value)::NUMERIC / sum(spend) END AS ROMI
	
	
FROM new_cte
GROUP BY ad_month, utm_campaign
ORDER BY ad_month, utm_campaign
),

monthly_data_and_previous_month AS (
		SELECT *,
		LAG(CTR) OVER(PARTITION BY utm_campaign ORDER BY ad_month) AS previous_month_CTR,
		LAG(CPM) OVER(PARTITION BY utm_campaign ORDER BY ad_month) AS previous_month_CPM,
		LAG(ROMI) OVER(PARTITION BY utm_campaign ORDER BY ad_month) AS previous_month_ROMI
		FROM monthly_data
		
)


SELECT *,
		(CTR - previous_month_CTR) / NULLIF(previous_month_CTR, 0) AS CTR_difference,
		(CPM - previous_month_CPM) / NULLIF(previous_month_CPM, 0) AS CPM_difference,
		CASE WHEN previous_month_ROMI = 0 AND ROMI > 0 THEN 1
		ELSE ROMI/previous_month_ROMI - 1 END AS ROMI_difference
FROM monthly_data_and_previous_month


