-- Portable SQL92 edge-case fixture for the shared JDBC correctness matrix.
--
-- Loads unchanged on H2, PostgreSQL and MySQL. The file is UTF-8; row id 6 stores
-- an emoji + CJK string to prove Unicode survives the JDBC round-trip end to end.
-- All temporal values are plain string literals (implicitly coerced on INSERT) so
-- no vendor-specific date syntax is required.
--
-- Rows are deterministic so JdbcTestQuerySet can encode exact expected results
-- (see the edge_* scenarios there):
--   id 1  -> label IS NULL                          (NULL handling)
--   id 2  -> num_val / big_val IS NULL              (NULL handling)
--   id 3  -> empty-string label                     (must round-trip as '' not NULL)
--   id 4  -> num_val = Integer.MAX_VALUE, big_val = Long.MAX_VALUE
--   id 5  -> num_val = Integer.MIN_VALUE, big_val = Long.MIN_VALUE
--   id 6  -> Unicode label (emoji + CJK)
--   id 7  -> daylight-saving-gap timestamp          (timezone edge value)
CREATE TABLE edge_cases (
    id INTEGER,
    label VARCHAR(64),
    num_val INTEGER,
    big_val BIGINT,
    ts_val TIMESTAMP
);

INSERT INTO edge_cases (id, label, num_val, big_val, ts_val) VALUES
    (1, NULL,          100,         100,                  '2021-06-15 12:00:00'),
    (2, 'present',     NULL,        NULL,                 '2021-06-15 12:00:00'),
    (3, '',            0,           0,                    '1970-01-01 00:00:00'),
    (4, 'maxint',      2147483647,  9223372036854775807,  '2038-01-19 03:14:07'),
    (5, 'minint',      -2147483648, -9223372036854775808, '1901-12-13 20:45:52'),
    (6, '😀你好',       42,          42,                   '2021-12-31 23:59:59'),
    (7, 'tz-dst-gap',  7,           7,                    '2021-03-14 02:30:00');
