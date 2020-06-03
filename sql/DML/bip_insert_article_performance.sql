BEGIN;

TRUNCATE TABLE bip_performance.article_performance;

INSERT  INTO bip_performance.article_performance(article_id
                                            ,"date"
                                            ,title
                                            ,category
                                            ,card_views
                                            ,article_views
                                            ,inserted_at)

SELECT
    attributes_id AS article_id
    ,timestamp_::DATE AS "date"
    ,attributes_title AS title
    ,attributes_category AS category
    ,SUM(CASE WHEN event_name IN ('my_news_card_viewed','top_news_card_viewed') THEN 1 ELSE 0 END)  as card_views
    ,SUM(CASE WHEN event_name = 'article_viewed' THEN 1 ELSE 0 END) as article_views
    ,current_timestamp AS inserted_at
FROM
    landing_performance.performance_for_insert
WHERE
    event_name in ('my_news_card_viewed','top_news_card_viewed','article_viewed')
GROUP BY
    article_id
    ,"date"
    ,title
    ,category
    ,inserted_at ;

COMMIT;