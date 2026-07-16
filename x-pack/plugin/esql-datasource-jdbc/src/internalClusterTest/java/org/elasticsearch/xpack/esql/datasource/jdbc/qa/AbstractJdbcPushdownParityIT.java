/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasource.jdbc.JdbcRuntimeConfig;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.ExpectedColumn;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.Fixture;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.RowOrder;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;

/**
 * Result-parity harness for JDBC bare-column WHERE pushdown. Runs a fixed set of bare-column filter queries against
 * the adversarial {@link Fixture#PUSHDOWN_PARITY} fixture and asserts, for every query, the EXACT expected result
 * (columns + rows, ordered or as a multiset) — the single golden truth.
 * <p>
 * <b>How this proves on-vs-off parity.</b> The {@code esql.jdbc.pushdown.enabled} node setting is seeded ONCE at node
 * startup ({@link org.elasticsearch.xpack.esql.datasource.jdbc.JdbcRuntimeConfig} has no dynamic cluster-settings hook
 * on the current SPI), so a single node cannot flip pushdown mid-suite. Instead each concrete subclass fixes the
 * setting for its whole node via {@link #nodeSettings} ({@link #pushdownEnabledForSuite()}), and BOTH the
 * pushdown-ENABLED and pushdown-DISABLED subclasses assert the SAME golden rows. Identical-result parity between
 * pushdown on and off is therefore established transitively (on == golden == off), and — being pinned to the correct
 * answer rather than merely "the two agree" — it also catches a bug that would corrupt BOTH paths identically.
 * <p>
 * <b>WHERE-emitted proof.</b> A green parity run over benign data can hide a silently-dropped pushdown (the engine
 * would just compute the right answer anyway). So each query also asserts the generated scan SQL: with pushdown
 * ENABLED the DEBUG {@code JDBC scan SQL: [...]} line from {@code JdbcQueryBuilder} MUST contain {@code WHERE} (and,
 * where unambiguous, the expected operator token); with pushdown DISABLED it MUST NOT contain {@code WHERE} (the
 * connector emits an unfiltered scan and the engine filters). The SQL is captured via {@link MockLog} on the
 * in-JVM node's {@code JdbcQueryBuilder} logger (raised to DEBUG for the duration of the test). Bound values never
 * appear in that log line (only {@code ?} placeholders), so no credential/value leaks into the assertion surface.
 * <p>
 * <b>Adversarial coverage.</b> The fixture carries NULLs, an empty string, duplicate rows and duplicate keys, so the
 * parity assertions exercise three-valued logic, empty-string matching, keyword RECHECK, and true multiset
 * cardinality — the row-skipping hazards a naive pushdown could get wrong (see the fixture header).
 */
public abstract class AbstractJdbcPushdownParityIT extends AbstractJdbcDatabaseIT {

    /** ES|QL {@code outputType()} strings for the fixture columns. */
    private static final String INTEGER = "integer";
    private static final String KEYWORD = "keyword";

    /**
     * Fully-qualified name of the (package-private) {@code JdbcQueryBuilder} logger. It cannot be referenced as a
     * {@code Class} literal from this package, so the scan-SQL {@link MockLog} capture keys on the name string.
     */
    private static final String JDBC_QUERY_BUILDER_LOGGER = "org.elasticsearch.xpack.esql.datasource.jdbc.JdbcQueryBuilder";

    /** Whether this suite's node has WHERE pushdown enabled. Drives both the setting and the WHERE-emitted assertion. */
    protected abstract boolean pushdownEnabledForSuite();

    /**
     * Only the adversarial parity fixture is loaded (the shared correctness matrix has no scenarios for it, so the
     * inherited {@code testSharedCorrectnessMatrix} runs zero scenarios and passes trivially here).
     */
    @Override
    protected Set<Fixture> enabledFixtures() {
        return EnumSet.of(Fixture.PUSHDOWN_PARITY);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(JdbcRuntimeConfig.PUSHDOWN_ENABLED.getKey(), pushdownEnabledForSuite())
            .build();
    }

    /**
     * One parity case: the ES|QL text (over the parity dataset), the exact expected columns and rows, and — for the
     * WHERE-emitted proof — the operator token the pushed SQL must contain when pushdown is on ({@code null} when the
     * exact spelling is ambiguous, e.g. an AND/OR/NOT/range that the optimizer may re-shape, in which case only the
     * presence of {@code WHERE} is asserted).
     */
    private record ParityQuery(
        String id,
        String esql,
        List<ExpectedColumn> columns,
        List<List<Object>> rows,
        RowOrder order,
        String pushedOpToken
    ) {}

    private static ExpectedColumn col(String name, String type) {
        return new ExpectedColumn(name, type);
    }

    /**
     * The bare-column filter matrix. Every ES|QL predicate here is pushable for the fixture's INTEGER/KEYWORD/BOOLEAN
     * columns: numeric/boolean {@code = <> < <= > >=}, {@code IN}, range, {@code IS [NOT] NULL}, keyword {@code =}
     * (RECHECK) and {@code LIKE} (RECHECK), plus {@code AND}/{@code OR}/{@code NOT} combinations. Keyword ordering /
     * {@code <>} / {@code BETWEEN} are intentionally absent — they are refused under {@code GenericDialect} (no
     * collation guarantee), so asserting they were pushed would be wrong.
     */
    private List<ParityQuery> parityQueries(String dataset) {
        List<ParityQuery> q = new ArrayList<>();
        // -- numeric comparisons ---------------------------------------------------------------------------------
        q.add(idQuery("eq_int", "WHERE n == 30", List.of(row(3), row(7)), RowOrder.ORDERED, "\"n\" = ?", dataset));
        // n != 30 is normalized by the ES|QL optimizer to NOT(n == 30) (rendered NOT ("n" = ?)), not the <> spelling;
        // the exact negation form is an optimizer choice, so assert WHERE-only here (parity + WHERE-emitted still hold).
        q.add(idQuery("neq_int", "WHERE n != 30", List.of(row(1), row(2), row(4), row(5), row(8)), RowOrder.ORDERED, null, dataset));
        q.add(idQuery("lt_int", "WHERE n < 30", List.of(row(1), row(2), row(8)), RowOrder.ORDERED, "\"n\" < ?", dataset));
        q.add(
            idQuery("lte_int", "WHERE n <= 30", List.of(row(1), row(2), row(3), row(7), row(8)), RowOrder.ORDERED, "\"n\" <= ?", dataset)
        );
        q.add(idQuery("gt_int", "WHERE n > 30", List.of(row(4), row(5)), RowOrder.ORDERED, "\"n\" > ?", dataset));
        q.add(idQuery("gte_int", "WHERE n >= 30", List.of(row(3), row(4), row(5), row(7)), RowOrder.ORDERED, "\"n\" >= ?", dataset));
        // -- IN / range ------------------------------------------------------------------------------------------
        q.add(idQuery("in_int", "WHERE n IN (10, 50)", List.of(row(1), row(5)), RowOrder.ORDERED, " IN (", dataset));
        // range may be re-shaped into BETWEEN or a two-sided comparison by the optimizer; assert WHERE-only.
        q.add(
            idQuery(
                "range_int",
                "WHERE n > 10 AND n < 50",
                List.of(row(2), row(3), row(4), row(7), row(8)),
                RowOrder.ORDERED,
                null,
                dataset
            )
        );
        // -- IS [NOT] NULL ---------------------------------------------------------------------------------------
        q.add(idQuery("is_null", "WHERE n IS NULL", List.of(row(6)), RowOrder.ORDERED, "IS NULL", dataset));
        q.add(
            idQuery(
                "is_not_null",
                "WHERE name IS NOT NULL",
                List.of(row(1), row(2), row(3), row(4), row(5), row(6), row(8)),
                RowOrder.ORDERED,
                "IS NOT NULL",
                dataset
            )
        );
        // -- boolean ---------------------------------------------------------------------------------------------
        q.add(idQuery("bool_eq", "WHERE flag == true", List.of(row(1), row(3), row(5), row(7)), RowOrder.ORDERED, "\"flag\" = ?", dataset));
        // -- keyword (RECHECK) -----------------------------------------------------------------------------------
        q.add(idQuery("kw_eq_single", "WHERE name == \"cherry\"", List.of(row(3)), RowOrder.ORDERED, "\"name\" = ?", dataset));
        q.add(idQuery("kw_empty_string", "WHERE name == \"\"", List.of(row(5)), RowOrder.ORDERED, "\"name\" = ?", dataset));
        q.add(idQuery("kw_like_prefix", "WHERE name LIKE \"ap*\"", List.of(row(1), row(4)), RowOrder.ORDERED, "\"name\" LIKE ?", dataset));
        // keyword equality that RECHECKs AND returns DUPLICATE rows -> true multiset (KEEP name, no SORT).
        q.add(
            new ParityQuery(
                "kw_eq_duplicates",
                "FROM " + dataset + " | WHERE name == \"banana\" | KEEP name",
                List.of(col("name", KEYWORD)),
                List.of(List.of("banana"), List.of("banana")),
                RowOrder.UNORDERED,
                "\"name\" = ?"
            )
        );
        // -- AND / OR / NOT --------------------------------------------------------------------------------------
        q.add(idQuery("and_combo", "WHERE n == 20 AND flag == false", List.of(row(2), row(8)), RowOrder.ORDERED, " AND ", dataset));
        q.add(idQuery("or_combo", "WHERE n == 10 OR name == \"cherry\"", List.of(row(1), row(3)), RowOrder.ORDERED, " OR ", dataset));
        // NOT(n == 30): the optimizer may render NOT (...) or rewrite to <>; assert WHERE-only.
        q.add(
            idQuery("not_combo", "WHERE NOT (n == 30)", List.of(row(1), row(2), row(4), row(5), row(8)), RowOrder.ORDERED, null, dataset)
        );
        return q;
    }

    /** A parity query that projects a single {@code id} INTEGER column, sorted by id for an order-stable comparison. */
    private static ParityQuery idQuery(
        String id,
        String whereClause,
        List<List<Object>> rows,
        RowOrder order,
        String pushedOpToken,
        String dataset
    ) {
        String esql = "FROM " + dataset + " | " + whereClause + " | KEEP id | SORT id";
        return new ParityQuery(id, esql, List.of(col("id", INTEGER)), rows, order, pushedOpToken);
    }

    private static List<Object> row(Object v) {
        return List.of(v);
    }

    /**
     * The single parity test. For every {@link ParityQuery}: capture the {@code JdbcQueryBuilder} scan SQL, run the
     * query, assert the exact columns + rows (the golden truth, identical in both the enabled and disabled subclass),
     * then assert the WHERE clause was (enabled) or was not (disabled) pushed into the scan SQL.
     */
    public void testBareColumnFilterParityAndPushdown() {
        String dataset = datasetNameFor(Fixture.PUSHDOWN_PARITY);
        boolean enabled = pushdownEnabledForSuite();

        // JdbcQueryBuilder is package-private, so its logger is referenced by fully-qualified name rather than .class.
        // Raise it to DEBUG so the scan-SQL log line reaches the MockLog appender (the default test level is INFO).
        // Restored in finally to avoid leaking DEBUG noise into other tests in the same forked JVM.
        Level previous = org.apache.logging.log4j.LogManager.getLogger(JDBC_QUERY_BUILDER_LOGGER).getLevel();
        Loggers.setLevel(org.apache.logging.log4j.LogManager.getLogger(JDBC_QUERY_BUILDER_LOGGER), Level.DEBUG);
        try {
            for (ParityQuery pq : parityQueries(dataset)) {
                try (var mockLog = MockLog.capture(JDBC_QUERY_BUILDER_LOGGER)) {
                    // A scan must always run (proves the connector executed and logged for this query).
                    mockLog.addExpectation(
                        new MockLog.SeenEventExpectation(
                            "scan ran [" + pq.id() + "]",
                            JDBC_QUERY_BUILDER_LOGGER,
                            Level.DEBUG,
                            "*JDBC scan SQL: [*]*"
                        )
                    );
                    if (enabled) {
                        mockLog.addExpectation(
                            new MockLog.SeenEventExpectation(
                                "WHERE pushed [" + pq.id() + "]",
                                JDBC_QUERY_BUILDER_LOGGER,
                                Level.DEBUG,
                                "*JDBC scan SQL: [*WHERE*]*"
                            )
                        );
                        if (pq.pushedOpToken() != null) {
                            mockLog.addExpectation(
                                new MockLog.SeenEventExpectation(
                                    "op pushed [" + pq.id() + "] token=[" + pq.pushedOpToken() + "]",
                                    JDBC_QUERY_BUILDER_LOGGER,
                                    Level.DEBUG,
                                    "*JDBC scan SQL: [*" + pq.pushedOpToken() + "*]*"
                                )
                            );
                        }
                    } else {
                        mockLog.addExpectation(
                            new MockLog.UnseenEventExpectation(
                                "WHERE NOT pushed [" + pq.id() + "]",
                                JDBC_QUERY_BUILDER_LOGGER,
                                Level.DEBUG,
                                "*JDBC scan SQL: [*WHERE*]*"
                            )
                        );
                    }

                    try (EsqlQueryResponse response = run(pq.esql(), queryTimeout())) {
                        assertColumns(pq, response.columns());
                        assertRows(pq, getValuesList(response));
                    }
                    mockLog.assertAllExpectationsMatched();
                }
            }
        } finally {
            Loggers.setLevel(org.apache.logging.log4j.LogManager.getLogger(JDBC_QUERY_BUILDER_LOGGER), previous);
        }
    }

    private static void assertColumns(ParityQuery pq, List<? extends ColumnInfo> actual) {
        List<ExpectedColumn> expected = pq.columns();
        assertEquals("column count for [" + pq.id() + "]", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("column[" + i + "] name for [" + pq.id() + "]", expected.get(i).name(), actual.get(i).name());
            assertEquals("column[" + i + "] outputType for [" + pq.id() + "]", expected.get(i).type(), actual.get(i).outputType());
        }
    }

    private static void assertRows(ParityQuery pq, List<List<Object>> actual) {
        List<List<Object>> expected = pq.rows();
        if (pq.order() == RowOrder.ORDERED) {
            assertEquals("rows (ordered) for [" + pq.id() + "]", expected, actual);
        } else {
            assertEquals("row count for [" + pq.id() + "]; expected " + expected + " but got " + actual, expected.size(), actual.size());
            assertEquals(
                "rows (unordered multiset) for [" + pq.id() + "]; expected " + expected + " but got " + actual,
                multiset(expected),
                multiset(actual)
            );
        }
    }

    private static Map<List<Object>, Integer> multiset(List<List<Object>> rows) {
        Map<List<Object>, Integer> counts = new LinkedHashMap<>();
        for (List<Object> row : rows) {
            counts.merge(row, 1, Integer::sum);
        }
        return counts;
    }
}
