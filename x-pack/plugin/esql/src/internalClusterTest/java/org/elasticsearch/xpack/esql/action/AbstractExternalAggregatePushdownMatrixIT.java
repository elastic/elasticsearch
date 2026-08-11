/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Per-format matrix for warm aggregate (MIN/MAX) short-circuit over {@code FROM <external-dataset>}.
 *
 * <p>Each test runs the SAME aggregate twice on the SAME coordinator: a cold pass that scans every row and
 * harvests the canonical-stripe stats, then a warm pass that must be served entirely from those stats
 * ({@code documentsFound == 0}) and return a value IDENTICAL to the cold pass. Asserting cold == warm is the
 * load-bearing check: the short-circuit is correct only if the stat the harvest stored equals what a full
 * MIN/MAX scan computes. The double column makes this explicit — the harvest must read it in the column's
 * resolved type (the type the aggregate uses), never a divergent one.
 *
 * <p>This base owns the {@code @Test} bodies; each concrete subclass binds them to one format by supplying
 * {@link #format()}, {@link #formatPlugins()} and a {@link #writeFixture(Path, int)} that lays the same
 * logical columns out in that format. Mirrors {@link AbstractExternalMetadataMatrixIT}.
 *
 * <p>Datasource/dataset registration, the SPI-extension wiring, the feature-flag gate and the local-path
 * allowlist all come from {@link AbstractExternalDataSourceIT}; datasets registered via
 * {@link #registerDataset(String, String)} / {@link #registerFormatDataset(String, String, Map)} are
 * auto-torn-down.
 */
// numDataNodes=1: multi-node dataset publication trips an unrelated ProjectMetadata.Builder assertion
// already on main (same reason AbstractExternalMetadataMatrixIT pins it); a single node also makes the
// per-coordinator stats cache deterministic without pinning queries to a specific node.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public abstract class AbstractExternalAggregatePushdownMatrixIT extends AbstractExternalDataSourceIT {

    protected static final int ROWS = 50;

    /** The format name passed as the dataset's {@code format} setting (e.g. {@code "csv"}). */
    protected abstract String format();

    /**
     * Write a {@code rows}-row fixture into {@code dir} and return the resource URI string. The fixture has
     * three columns over {@code i} in {@code [0, rows)}: {@code emp_no} (a whole number {@code i}),
     * {@code label} (the keyword {@link #label(int)}, zero-padded under {@link Locale#ROOT} so lexicographic
     * order == numeric order), and {@code val} (the double {@code i + 0.5}).
     */
    protected abstract String writeFixture(Path dir, int rows) throws Exception;

    /** Single-thread the parse so the cold harvest is deterministic; pins the plan shape these tests assert. */
    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    @Before
    public void writeFixtureAndRegister() throws Exception {
        String fixtureUri = writeFixture(createTempDir(), ROWS);
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", fixtureUri, Map.of("format", format()));
    }

    public void testMinMaxNumericColdThenWarmShortCircuits() {
        assertColdThenWarmShortCircuit("employees", "STATS lo = MIN(emp_no), hi = MAX(emp_no)", ROWS, rows -> {
            assertThat("MIN(emp_no)", ((Number) rows.get(0).get(0)).longValue(), equalTo(0L));
            assertThat("MAX(emp_no)", ((Number) rows.get(0).get(1)).longValue(), equalTo((long) (ROWS - 1)));
        });
    }

    public void testMinMaxKeywordColdThenWarmShortCircuits() {
        assertColdThenWarmShortCircuit("employees", "STATS lo = MIN(label), hi = MAX(label)", ROWS, rows -> {
            assertThat("MIN(label)", String.valueOf(rows.get(0).get(0)), equalTo(label(0)));
            assertThat("MAX(label)", String.valueOf(rows.get(0).get(1)), equalTo(label(ROWS - 1)));
        });
    }

    public void testMinMaxDoubleColdThenWarmShortCircuits() {
        assertColdThenWarmShortCircuit("employees", "STATS lo = MIN(val), hi = MAX(val)", ROWS, rows -> {
            assertThat("MIN(val)", ((Number) rows.get(0).get(0)).doubleValue(), equalTo(0.5));
            assertThat("MAX(val)", ((Number) rows.get(0).get(1)).doubleValue(), equalTo((ROWS - 1) + 0.5));
        });
    }

    /** The keyword value for row {@code i}: zero-padded under {@link Locale#ROOT} so digits are ASCII and
     *  lexicographic order matches numeric order regardless of the randomized test locale. */
    protected static String label(int i) {
        return String.format(Locale.ROOT, "k%04d", i);
    }

    /** Registers a dataset under the already-created {@code local_ds} data source, in this format. */
    protected void registerDataset(String datasetName, String fixtureUri) {
        registerFormatDataset(datasetName, fixtureUri, Map.of());
    }

    /**
     * Registers a dataset in this format, merging {@code extraSettings} (e.g. {@code multi_value_syntax})
     * on top of the mandatory {@code format} setting. Registered under {@code local_ds} and auto-torn-down
     * via the base's {@code cleanupRegistry()}. Named distinctly from the base's {@code test_ds}-scoped
     * {@code registerDataset(name, uri, settings)} convenience, which it deliberately does not override.
     */
    protected void registerFormatDataset(String datasetName, String fixtureUri, Map<String, Object> extraSettings) {
        Map<String, Object> settings = new HashMap<>();
        settings.put("format", format());
        settings.putAll(extraSettings);
        registerDataset(datasetName, "local_ds", fixtureUri, settings);
    }

    /**
     * Runs {@code FROM <fromTarget> | <statsClause>} cold (must scan {@code expectedScanRows}) then warm (must
     * short-circuit, 0 docs), applying {@code assertValues} to BOTH result sets so the warm short-circuit is
     * proven equal to the cold full scan.
     */
    protected void assertColdThenWarmShortCircuit(
        String fromTarget,
        String statsClause,
        int expectedScanRows,
        Consumer<List<List<Object>>> assertValues
    ) {
        String query = "FROM " + fromTarget + " | " + statsClause;
        try (var cold = run(syncEsqlQueryRequest(query).profile(true), TIMEOUT)) {
            assertThat("cold pass scans every row", cold.documentsFound(), equalTo((long) expectedScanRows));
            assertValues.accept(getValuesList(cold));
        }
        try (var warm = run(syncEsqlQueryRequest(query).profile(true), TIMEOUT)) {
            assertThat("warm pass must short-circuit (0 docs scanned)", warm.documentsFound(), equalTo(0L));
            assertValues.accept(getValuesList(warm)); // same assertion as cold == value-parity
        }
    }

    /**
     * Type-agnostic cold==warm check for {@code MIN}/{@code MAX} over one column: a cold pass scans and
     * harvests the column's extrema, then the warm pass MUST short-circuit (0 docs) and return values EQUAL
     * to the cold pass's, and non-null. This does not hard-code the rendered value, so it works for any
     * supported type — it catches the whole class of "warm serves the wrong/NULL value for a type that has
     * no (or a wrong) buildBlock arm" bugs (the shape of the {@code MIN/MAX(ip)}-returns-NULL defect). If a
     * type is not harvested/servable at all, the warm pass won't short-circuit and this fails loudly.
     */
    protected void assertColdEqualsWarmMinMax(String dataset, String column) {
        String query = "FROM " + dataset + " | STATS lo = MIN(" + column + "), hi = MAX(" + column + ")";
        Object coldMin;
        Object coldMax;
        try (var cold = run(syncEsqlQueryRequest(query).profile(true), TIMEOUT)) {
            assertThat("cold pass scans every row [" + column + "]", cold.documentsFound(), equalTo((long) ROWS));
            List<Object> row = getValuesList(cold).get(0);
            coldMin = row.get(0);
            coldMax = row.get(1);
            assertNotNull("cold MIN(" + column + ")", coldMin);
            assertNotNull("cold MAX(" + column + ")", coldMax);
        }
        try (var warm = run(syncEsqlQueryRequest(query).profile(true), TIMEOUT)) {
            assertThat("warm pass must short-circuit [" + column + "]", warm.documentsFound(), equalTo(0L));
            List<Object> row = getValuesList(warm).get(0);
            assertThat("warm MIN(" + column + ") == cold", String.valueOf(row.get(0)), equalTo(String.valueOf(coldMin)));
            assertThat("warm MAX(" + column + ") == cold", String.valueOf(row.get(1)), equalTo(String.valueOf(coldMax)));
        }
    }
}
