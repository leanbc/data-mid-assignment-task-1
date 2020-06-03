BEGIN;

DROP TABLE IF EXISTS bip_performance.article_performance CASCADE;

CREATE TABLE bip_performance.article_performance (

    article_id VARCHAR(100)
    ,"date" DATE
    ,title TEXT
    ,category VARCHAR(100)
    ,card_views INT
    ,article_views INT
    ,inserted_at TIMESTAMP
);

COMMIT;