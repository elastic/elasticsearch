/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Cross-format PARITY guardrail for the warm-aggregate invariant under a ROW DROP. The same scenario runs
 * against CSV, TSV, and NDJSON (via {@link #format()} + {@link #buildContent}), and every format must behave
 * IDENTICALLY -- this is the structural guard against the readers diverging again (the CSV {@code rowsSkipped}
 * vs NDJSON {@code errorCount} split that produced the A1 regression).
 *
 * <p>The invariant, asserted for every format:
 * <ul>
 *   <li><b>Clean, multi-stripe:</b> warm MIN/MAX AND COUNT all short-circuit ({@code documentsFound == 0}).</li>
 *   <li><b>A dropped row (error_mode=skip_row, malformed row mid-file):</b> warm MIN/MAX AND COUNT all STILL
 *       short-circuit over the surviving rows. The cache fingerprint pins error_mode, so a full scan drops the
 *       SAME row -- every statistic (COUNT and extrema alike) over the survivors is exact vs that scan, and COUNT
 *       is not singled out.</li>
 * </ul>
 *
 * <p>A small stripe grid ({@code esql.source.cache.stripe.size=64kb}) over a &gt;1MB uncompressed file forces many
 * stripes split across parallel chunk boundaries -- the layout where the offset/row-alignment invariant bites.
 *
 * <p>Datasource/dataset registration, the SPI-extension wiring, the feature-flag gate and the local-path
 * allowlist all come from {@link AbstractExternalDataSourceIT}; the {@code local_ds} data source and every
 * registered dataset are auto-torn-down by the base's {@code cleanupRegistry()}.
 */
// numDataNodes=1: mirrors the aggregate matrix IT (multi-node dataset publication trips an unrelated assertion).
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public abstract class AbstractExternalRowDropParityIT extends AbstractExternalDataSourceIT {

    // Overrides the base's 30s TIMEOUT: the 60k-row fixtures need the longer budget.
    protected static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(60);
    protected static final int ROWS = 60_000; // ~2MB decompressed -> multiple 1MB chunks, many 64kb stripes

    /** The dataset {@code format} setting (e.g. {@code "csv"}). */
    protected abstract String format();

    /** File extension for the (pre-gzip) fixture, e.g. {@code ".csv"}. */
    protected abstract String fileExtension();

    /**
     * The raw fixture text: {@code rows} rows with an integer {@code id} column running {@code 0..rows-1}. When
     * {@code malformed} is true, the row at {@code rows/2} is replaced by a row the format drops under skip_row
     * (a non-numeric id for CSV/TSV, a non-JSON line for NDJSON) -- a MID-file row, so it is never the MIN or MAX.
     */
    protected abstract String buildContent(int rows, boolean malformed);

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("esql.source.cache.stripe.size", "64kb").build();
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 4).build());
    }

    @Before
    public void registerDataSource() {
        registerDataSource("local_ds", Map.of());
    }

    /** Clean multi-stripe: warm MIN/MAX AND COUNT all short-circuit. */
    public void testCleanMultiStripeWarmMinMaxAndCountShortCircuit() throws Exception {
        String ds = registerFixture("clean_" + format(), false, Map.of());
        assertColdThenWarm(ds, "STATS lo = MIN(id), hi = MAX(id), c = COUNT(*)", ROWS, 0L, rows -> {
            assertThat("MIN(id)", ((Number) rows.get(0).get(0)).longValue(), equalTo(0L));
            assertThat("MAX(id)", ((Number) rows.get(0).get(1)).longValue(), equalTo((long) (ROWS - 1)));
            assertThat("COUNT(*)", ((Number) rows.get(0).get(2)).longValue(), equalTo((long) ROWS));
        });
    }

    /**
     * THE PARITY INVARIANT under a row drop: warm MIN/MAX AND COUNT all short-circuit over the surviving rows.
     * The dropped row is mid-file (never the MIN or MAX), and the cache fingerprint pins error_mode -- so a full
     * scan drops the SAME row and every statistic over the survivors (COUNT and extrema alike) is exact vs that
     * scan. COUNT is NOT treated differently from the extrema. Identical for every format.
     */
    public void testRowDropWarmMinMaxAndCountAllShortCircuitOverSurvivors() throws Exception {
        String ds = registerFixture("drop_" + format(), true, Map.of("error_mode", "skip_row"));
        assertColdThenWarm(ds, "STATS lo = MIN(id), hi = MAX(id), c = COUNT(*)", ROWS - 1, 0L, rows -> {
            assertThat("MIN(id) over survivors", ((Number) rows.get(0).get(0)).longValue(), equalTo(0L));
            assertThat("MAX(id) over survivors", ((Number) rows.get(0).get(1)).longValue(), equalTo((long) (ROWS - 1)));
            assertThat("COUNT(*) over survivors", ((Number) rows.get(0).get(2)).longValue(), equalTo((long) (ROWS - 1)));
        });
    }

    private String registerFixture(String datasetName, boolean malformed, Map<String, Object> extraSettings) throws Exception {
        // Uncompressed on purpose: a >1MB file is split into several record-aligned macro-splits (multi-chunk),
        // and the 64kb stripe grid means stripes straddle chunk boundaries -- the layout the invariant guards.
        Path file = createTempDir().resolve(datasetName + fileExtension());
        Files.writeString(file, buildContent(ROWS, malformed));
        Map<String, Object> settings = new HashMap<>();
        settings.put("format", format());
        settings.putAll(extraSettings);
        registerDataset(datasetName, "local_ds", StoragePath.fileUri(file), settings);
        return datasetName;
    }

    private void assertColdThenWarm(
        String ds,
        String statsClause,
        long expectedColdScanRows,
        long expectedWarmScanRows,
        Consumer<List<List<Object>>> assertValues
    ) {
        String query = "FROM " + ds + " | " + statsClause;
        try (var cold = run(syncEsqlQueryRequest(query).profile(true), TIMEOUT)) {
            assertThat("cold pass scan rows", cold.documentsFound(), equalTo(expectedColdScanRows));
            assertValues.accept(getValuesList(cold));
        }
        try (var warm = run(syncEsqlQueryRequest(query).profile(true), TIMEOUT)) {
            assertThat("warm pass scan rows (0 == short-circuit)", warm.documentsFound(), equalTo(expectedWarmScanRows));
            assertValues.accept(getValuesList(warm));
        }
    }
}
