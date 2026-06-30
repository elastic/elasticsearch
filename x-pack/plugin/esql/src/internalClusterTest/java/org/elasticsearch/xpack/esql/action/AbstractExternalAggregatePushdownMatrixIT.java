/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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
 */
// numDataNodes=1: multi-node dataset publication trips an unrelated ProjectMetadata.Builder assertion
// already on main (same reason AbstractExternalMetadataMatrixIT pins it); a single node also makes the
// per-coordinator stats cache deterministic without pinning queries to a specific node.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public abstract class AbstractExternalAggregatePushdownMatrixIT extends AbstractEsqlIntegTestCase {

    protected static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    protected static final int ROWS = 50;

    /** The format name passed as the dataset's {@code format} setting (e.g. {@code "csv"}). */
    protected abstract String format();

    /** Reader plugin(s) backing this format, registered as node plugins for SPI discovery. */
    protected abstract Collection<Class<? extends Plugin>> formatPlugins();

    /**
     * Write a {@code rows}-row fixture into {@code dir} and return the resource URI string. The fixture has
     * three columns over {@code i} in {@code [0, rows)}: {@code emp_no} (a whole number {@code i}),
     * {@code label} (the keyword {@link #label(int)}, zero-padded under {@link Locale#ROOT} so lexicographic
     * order == numeric order), and {@code val} (the double {@code i + 0.5}).
     */
    protected abstract String writeFixture(Path dir, int rows) throws Exception;

    /** Minimal pass-through validator registered for type {@code test}; accepts any resource scheme. */
    public static final class TestDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
            return Map.of("test", new TestValidator());
        }
    }

    private static final class TestValidator implements DataSourceValidator {
        @Override
        public String type() {
            return "test";
        }

        @Override
        public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
            Map<String, DataSourceSetting> out = new HashMap<>();
            for (Map.Entry<String, Object> e : datasourceSettings.entrySet()) {
                out.put(e.getKey(), new DataSourceSetting(e.getValue(), e.getKey().startsWith("secret_")));
            }
            return out;
        }

        @Override
        public Map<String, Object> validateDataset(
            Map<String, DataSourceSetting> datasourceSettings,
            String resource,
            Map<String, Object> datasetSettings
        ) {
            return datasetSettings == null ? Map.of() : new HashMap<>(datasetSettings);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.addAll(formatPlugins());
        plugins.add(TestDataSourcePlugin.class);
        return plugins;
    }

    /** Single-thread the parse so the cold harvest is deterministic; pins the plan shape these tests assert. */
    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
    }

    @Before
    public void writeFixtureAndRegister() throws Exception {
        String fixtureUri = writeFixture(createTempDir(), ROWS);
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(PutDatasetAction.INSTANCE, putDatasetRequest("employees", "local_ds", fixtureUri, Map.of("format", format())))
        );
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
        assertAcked(
            client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(datasetName, "local_ds", fixtureUri, Map.of("format", format())))
        );
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

    private static PutDataSourceAction.Request putDataSourceRequest(String name, Map<String, Object> settings) {
        return new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, name, "test", null, new HashMap<>(settings));
    }

    private static PutDatasetAction.Request putDatasetRequest(
        String name,
        String dataSource,
        String resource,
        Map<String, Object> settings
    ) {
        return new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, dataSource, resource, null, new HashMap<>(settings));
    }
}
