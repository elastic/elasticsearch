-- Portable SQL92 type-coverage fixture for the shared JDBC correctness matrix.
--
-- Loads unchanged on H2, PostgreSQL and MySQL: every column type is either SQL92
-- (INTEGER, VARCHAR, DATE, TIMESTAMP) or a widely supported standard extension
-- (BIGINT, DOUBLE PRECISION, BOOLEAN). Temporal and boolean values are written as
-- plain string / TRUE|FALSE literals so each engine applies its own implicit
-- coercion on INSERT rather than relying on a vendor-specific literal syntax.
--
-- Data is deliberately deterministic so JdbcTestQuerySet can encode exact expected
-- results (see the types_* scenarios there):
--   * dbl_val = 2.5 for both id 2 and id 4 (double equality selects two rows)
--   * bool_val = TRUE for id 1 and id 3
--   * long_val = 9223372036854775807 (Long.MAX_VALUE) on id 4
CREATE TABLE types_matrix (
    id INTEGER,
    long_val BIGINT,
    dbl_val DOUBLE PRECISION,
    str_val VARCHAR(32),
    bool_val BOOLEAN,
    date_val DATE,
    ts_val TIMESTAMP
);

INSERT INTO types_matrix (id, long_val, dbl_val, str_val, bool_val, date_val, ts_val) VALUES
    (1, 1000000000000, 1.5, 'alpha', TRUE,  '2020-01-01', '2020-01-01 08:00:00'),
    (2, 2000000000000, 2.5, 'beta',  FALSE, '2020-06-15', '2020-06-15 14:30:00'),
    (3, 3000000000000, 3.5, 'gamma', TRUE,  '2020-12-31', '2020-12-31 23:59:59'),
    (4, 9223372036854775807, 2.5, 'delta', FALSE, '1999-12-31', '1999-12-31 23:59:59');
