BEGIN;

DROP TABLE IF EXISTS bip_performance.user_performance CASCADE;


CREATE TABLE bip_performance.user_performance (

    user_id VARCHAR(100)
    ,"date" DATE
    ,ctr FLOAT
    ,inserted_at TIMESTAMP
);

COMMIT;