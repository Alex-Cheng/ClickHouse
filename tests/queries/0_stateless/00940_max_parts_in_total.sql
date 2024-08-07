drop table if exists max_parts_in_total;
create table max_parts_in_total (x UInt64) ENGINE = MergeTree PARTITION BY x ORDER BY x SETTINGS max_parts_in_total = 10;

SET max_insert_threads = 1;
INSERT INTO max_parts_in_total SELECT number FROM numbers(10);
SELECT 1;
INSERT INTO max_parts_in_total SELECT 123; -- { serverError TOO_MANY_PARTS }

drop table max_parts_in_total;
