-- Portable SQL92 adversarial fixture for the JDBC WHERE-pushdown parity ITs.
--
-- Loads unchanged on H2, PostgreSQL and MySQL: only INTEGER, VARCHAR and BOOLEAN
-- columns, with TRUE/FALSE and plain string literals so each engine applies its
-- own implicit coercion on INSERT. NOT part of the default portable fixture set
-- (JdbcTestQuerySet.Fixture#PUSHDOWN_PARITY is opt-in), so the existing shared
-- correctness matrix suites (H2JdbcIT / PostgresJdbcIT) are unaffected -- only the
-- pushdown parity ITs enable it.
--
-- The data is deliberately ADVERSARIAL so the on/off parity assertions actually
-- exercise the row-skipping hazards a naive pushdown could get wrong:
--   * NULLs           -> id 6 has n IS NULL; id 7 has name IS NULL (three-valued logic)
--   * empty string    -> id 5 has name = '' (must round-trip as '' not NULL, and match name == "")
--   * duplicate rows  -> ids 2 & 8 are identical in (n, name, flag), so KEEP name over
--                        name == "banana" returns TWO 'banana' rows (true multiset, not a set)
--   * duplicate keys  -> n = 30 on ids 3 & 7, n = 20 on ids 2 & 8 (range/IN cardinality)
--   * keyword RECHECK -> name equality/LIKE is pushed as RECHECK; the engine re-checks byte-exact
CREATE TABLE pushdown_parity (
    id INTEGER,
    n INTEGER,
    name VARCHAR(32),
    flag BOOLEAN
);

INSERT INTO pushdown_parity (id, n, name, flag) VALUES
    (1, 10,   'apple',  TRUE),
    (2, 20,   'banana', FALSE),
    (3, 30,   'cherry', TRUE),
    (4, 40,   'apple',  FALSE),
    (5, 50,   '',       TRUE),
    (6, NULL, 'date',   FALSE),
    (7, 30,   NULL,     TRUE),
    (8, 20,   'banana', FALSE);
