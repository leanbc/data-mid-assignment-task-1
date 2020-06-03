BEGIN;

TRUNCATE TABLE bip_performance.user_performance;

INSERT  INTO bip_performance.user_performance(
                                            user_id
                                            ,"date"
                                            ,ctr
                                            ,inserted_at)

WITH card_viewed AS (

SELECT
  md5_user_id AS user_id
, md5_session_id AS session_id
, timestamp_::DATE AS "date"
, attributes_id
FROM
  landing_performance.performance_for_insert
WHERE
    event_name in ('my_news_card_viewed','top_news_card_viewed')
    AND md5_session_id is not null),

article_viewed AS (
  SELECT
    md5_user_id AS user_id
  , md5_session_id AS session_id
  , timestamp_::DATE AS "date"
  , attributes_id
  FROM
  landing_performance.performance_for_insert
  WHERE
    event_name='article_viewed'
    AND md5_session_id is not null),

merged AS (
  SELECT 
    c.user_id
  , c."date"
  , c.attributes_id
  , coalesce(CASE WHEN a.attributes_id IS NOT NULL THEN 1 ELSE 0 END) AS viewed
  FROM card_viewed c
    LEFT JOIN article_viewed a USING (user_id, "date", session_id, attributes_id)
)

SELECT
  user_id
, "date"
, Sum(viewed)*1.0/count(attributes_id)*1.0 AS ctr
, current_timestamp AS inserted_at
FROM merged
GROUP BY 
  user_id
, "date"
, inserted_at;

COMMIT;