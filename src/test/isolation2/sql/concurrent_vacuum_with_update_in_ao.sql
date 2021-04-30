-- This sql is to test vacuum concurrent with update in ao table
CREATE extension if not exists gp_inject_fault;

CREATE TABLE sales_row (id int, date date, amt decimal(10,2))
WITH (appendonly=true) DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
( START (date '2008-01-01') INCLUSIVE
END (date '2009-01-01') EXCLUSIVE
EVERY (INTERVAL '1 month') );

INSERT INTO sales_row VALUES (generate_series(1,1000),'2008-01-01',10);

UPDATE sales_row SET amt = amt + 1;

-- inject fault on ''vacuum_hold_lock''
SELECT gp_inject_fault('vacuum_hold_lock', 'suspend', dbid)
FROM gp_segment_configuration WHERE ROLE = 'p' AND content = -1;

1&: vacuum sales_row;

-- wait session1 until session1 arrive 'vacuum_hold_lock'
SELECT gp_wait_until_triggered_fault('vacuum_hold_lock', 1, dbid)
FROM gp_segment_configuration WHERE ROLE = 'p' AND content = -1;

2&: UPDATE sales_row SET amt = amt + 1;

-- wait 2s because we want session2 arrive the acquire lock code 
-- waitting for locks which hold by session1
SELECT pg_sleep(2);

-- release the lock holding by session1
SELECT gp_inject_fault('vacuum_hold_lock', 'reset', dbid)
FROM gp_segment_configuration WHERE ROLE = 'p' AND content = -1;

1<:
2<:

1q:
2q:

DROP TABLE sales_row;

-- second, we will test sql from cache plan.
CREATE TABLE sales_row (id int, date date, amt decimal(10,2))
WITH (appendonly=true) DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
( START (date '2008-01-01') INCLUSIVE
END (date '2009-01-01') EXCLUSIVE
EVERY (INTERVAL '1 month') );

INSERT INTO sales_row VALUES (generate_series(1,1000),'2008-01-01',10);

-- prepare statement as test and execute multi times
-- when prepare statement execute multi times, the
-- statemnt will be cached.
2: PREPARE test AS UPDATE sales_row SET amt = amt + 1;

2: EXECUTE test;

2: EXECUTE test;

2: EXECUTE test;

2: EXECUTE test;

2: EXECUTE test;

-- inject fault on ''vacuum_hold_lock''
SELECT gp_inject_fault('vacuum_hold_lock', 'suspend', dbid)
FROM gp_segment_configuration WHERE ROLE = 'p' AND content = -1;

1&: vacuum sales_row;

-- wait session1 until session1 arrive 'vacuum_hold_lock'
SELECT gp_wait_until_triggered_fault('vacuum_hold_lock', 1, dbid)
FROM gp_segment_configuration WHERE ROLE = 'p' AND content = -1;

2&: EXECUTE test;

-- wait 2s because we want session2 arrive the acquire lock code 
-- waitting for locks which hold by session1
SELECT pg_sleep(2);

-- release the lock holding by session1
SELECT gp_inject_fault('vacuum_hold_lock', 'reset', dbid)
FROM gp_segment_configuration WHERE ROLE = 'p' AND content = -1;

1<:
2<:

1q:
2q:

DROP TABLE sales_row;
