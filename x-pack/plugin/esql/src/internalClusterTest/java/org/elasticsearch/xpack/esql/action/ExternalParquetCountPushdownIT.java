/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration test verifying COUNT(*) pushdown for local Parquet files.
 * When pushdown fires, the data driver uses a LocalSourceExec (no Async operators)
 * instead of scanning all row groups through AsyncExternalSourceOperatorFactory.
 */
public class ExternalParquetCountPushdownIT extends AbstractEsqlIntegTestCase {

    /**
     * Re-enables extension loading that {@link EsqlPluginWithEnterpriseOrTrialLicense} suppresses.
     * Without this, DataSourcePlugin implementations (Parquet, HTTP) are not discovered.
     */
    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(ParquetDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .putList(ExternalSourceSettings.LOCAL_ALLOWED_PATHS.getKey(), createTempDir().getParent().toString())
            .build();
    }

    @Before
    public void requireLocalFilesEnabled() {
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testCountStarPushdown() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 300;
        Path parquetFile = writeParquetFile(totalRows, 100);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(parquetFile) + "\" | STATS c = COUNT(*)";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request)) {
                List<? extends ColumnInfo> columns = response.columns();
                assertThat(columns.size(), equalTo(1));
                assertThat(columns.get(0).name(), equalTo("c"));

                List<List<Object>> rows = getValuesList(response);
                assertThat(rows.size(), equalTo(1));
                assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo((long) totalRows));

                assertNoPushdownBypass(response);
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    public void testCountStarPushdownSingleRowGroup() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 50;
        Path parquetFile = writeParquetFile(totalRows, totalRows + 1);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(parquetFile) + "\" | STATS c = COUNT(*)";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request)) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo((long) totalRows));

                assertNoPushdownBypass(response);
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    public void testCountStarPushdownManyRowGroups() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 1000;
        // rowGroupSize=50 bytes forces ~20 row groups for 1000 rows
        Path parquetFile = writeParquetFile(totalRows, 50);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(parquetFile) + "\" | STATS c = COUNT(*)";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request)) {
                List<List<Object>> rows = getValuesList(response);
                long count = ((Number) rows.get(0).get(0)).longValue();
                assertThat(count, equalTo((long) totalRows));

                assertNoPushdownBypass(response);
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    public void testCountStarPushdownCoordinatorOnly() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 500;
        Path parquetFile = writeParquetFile(totalRows, 80);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(parquetFile) + "\" | STATS c = COUNT(*)";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);
            // Force the coordinator-only distribution strategy via the dedicated query pragma
            // (the EXTERNAL command's WITH-clause does not bridge into pragmas).
            request.pragmas(
                new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "coordinator_only").build())
            );
            request.acceptedPragmaRisks(true);

            try (var response = run(request)) {
                List<List<Object>> rows = getValuesList(response);
                long count = ((Number) rows.get(0).get(0)).longValue();
                assertThat(count, equalTo((long) totalRows));

                assertNoPushdownBypass(response);
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * End-to-end pin for the unknown-key rejection path. A query with a typo'd configuration key
     * must surface as {@code IllegalArgumentException} naming the typo and the recognised options,
     * proving the {@code ExternalSourceFactory.validateConfig} SPI hook fires before any read.
     */
    public void testUnknownConfigKeyIsRejectedAtPlanningTime() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path parquetFile = writeParquetFile(10, 100);
        try {
            String query = "EXTERNAL \""
                + StoragePath.fileUri(parquetFile)
                + "\" WITH { \"obviously_not_a_real_key\": \"x\" } | STATS c = COUNT(*)";
            var request = syncEsqlQueryRequest(query);

            Exception e = expectThrows(Exception.class, () -> { run(request).close(); });
            // The validator's IllegalArgumentException is wrapped twice on the way up
            // (resolveSingleSource → ExternalSourceResolver). Walk the cause chain to find it.
            Throwable validatorIae = null;
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t instanceof IllegalArgumentException
                    && t.getMessage() != null
                    && t.getMessage().contains("obviously_not_a_real_key")) {
                    validatorIae = t;
                    break;
                }
            }
            assertNotNull("expected validator IAE mentioning 'obviously_not_a_real_key' in cause chain of: " + e, validatorIae);
            assertThat(validatorIae.getMessage(), containsString("unknown option"));
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * End-to-end regression for the {@code COUNT(<col>)} correctness bug: over an external Parquet
     * file with an all-null column whose {@code null_count} footer statistic is absent (written here
     * with per-column statistics disabled), {@code COUNT(always_null)} must return the non-null count
     * (0), not the row count (200). Because the null count is unknown from the footer, the aggregate
     * pushdown must decline and fall back to actually reading the column (an {@code External*} source
     * operator appears), whereas a column that does carry a {@code null_count} ({@code rare}) still
     * pushes down from statistics (served from a {@code LocalSourceExec}, no source read), and
     * {@code COUNT(*)} is unaffected.
     */
    public void testCountOverAllNullColumnWithoutNullCountStatFallsBackToZero() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 200;
        int rareNonNull = 4;
        Path parquetFile = writeNullableParquetFile(totalRows, rareNonNull);
        try {
            String uri = StoragePath.fileUri(parquetFile);

            // COUNT(always_null): null_count stat absent -> pushdown must decline and scan the column,
            // returning the true non-null count of 0 (the bug returned the row count, 200).
            try (var response = runCount(uri, "non_null = COUNT(always_null)")) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(0L));
                assertPushdownBypassed(response);
            }

            // COUNT(rare): 196 of 200 null, null_count stat present -> pushdown fires from statistics
            // and returns rowCount - nullCount = 4.
            try (var response = runCount(uri, "c = COUNT(rare)")) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo((long) rareNonNull));
                assertPushdownFired(response);
            }

            // COUNT(*) is answered from the row count regardless and stays pushed down.
            try (var response = runCount(uri, "c = COUNT(*)")) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo((long) totalRows));
                assertPushdownFired(response);
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    private EsqlQueryResponse runCount(String uri, String statsClause) {
        var request = syncEsqlQueryRequest("EXTERNAL \"" + uri + "\" | STATS " + statsClause);
        request.profile(true);
        return run(request);
    }

    /**
     * Asserts that no Async* operator appears in any driver profile.
     * When pushdown fires, the plan is a LocalSourceExec — there is no
     * AsyncExternalSourceOperatorFactory executing file reads.
     * <p>
     * Weaker than {@link #assertPushdownFired}, which matches the actual {@code External*} profile
     * operator names; prefer that for new assertions. This helper stays for the COUNT(*) tests below.
     */
    private static void assertNoPushdownBypass(EsqlQueryResponse response) {
        var profile = response.profile();
        assertNotNull("profile must be present (request had profile=true)", profile);

        for (var driver : profile.drivers()) {
            for (var op : driver.operators()) {
                assertFalse(
                    "expected no Async* operators (pushdown should have fired) but found: " + op.operator(),
                    op.operator().startsWith("Async")
                );
            }
        }
    }

    /**
     * Inverse of {@link #assertNoPushdownBypass}: asserts the query actually read the external source,
     * proving the aggregate pushdown declined rather than being answered from statistics. When
     * pushdown fires the source is replaced by a {@code LocalSourceExec} and no {@code External*}
     * operator runs; when it declines the file is scanned via an {@code AsyncExternalSourceOperator}
     * or a synchronous {@code ExternalSourceOperator}/{@code ExternalFieldExtractOperator} (which
     * one depends on the chosen distribution). Match on the {@code External} substring to stay robust
     * across those shapes. Used for the unknown-{@code null_count} case.
     */
    private static void assertPushdownBypassed(EsqlQueryResponse response) {
        var profile = response.profile();
        assertNotNull("profile must be present (request had profile=true)", profile);

        boolean readSource = false;
        for (var driver : profile.drivers()) {
            for (var op : driver.operators()) {
                if (op.operator().contains("External")) {
                    readSource = true;
                }
            }
        }
        assertTrue("expected an External* source operator (pushdown must decline for an unknown null_count)", readSource);
    }

    /**
     * Stronger complement of {@link #assertPushdownBypassed} for the "pushdown fired" controls: when
     * the aggregate is answered from statistics the {@code ExternalSourceExec} is replaced by a
     * {@code LocalSourceExec}, so no {@code External*} source operator runs at all. This observes the
     * actual profile operator names ({@code ExternalDataSourceOperator} / {@code ExternalSourceOperator}
     * / {@code ExternalFieldExtractOperator}) rather than the {@code Async*} class-name proxy.
     */
    private static void assertPushdownFired(EsqlQueryResponse response) {
        var profile = response.profile();
        assertNotNull("profile must be present (request had profile=true)", profile);

        for (var driver : profile.drivers()) {
            for (var op : driver.operators()) {
                assertFalse(
                    "expected pushdown from statistics (no source read) but found: " + op.operator(),
                    op.operator().contains("External")
                );
            }
        }
    }

    private Path writeParquetFile(int rowCount, int rowGroupSize) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message test { required int64 id; required binary name (UTF8); required int32 value; }"
        );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(rowGroupSize)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("name", "row_" + i);
                g.add("value", i * 10);
                writer.write(g);
            }
        }

        Path tempFile = createTempDir().resolve("pushdown_test.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    /**
     * Writes a single-row-group file with three columns: {@code id} (no nulls), {@code rare}
     * ({@code rowCount - rareNonNull} nulls, statistics on) and {@code always_null} (all null,
     * statistics <b>disabled</b> so its footer carries no {@code null_count}). This reproduces the
     * mixed situation from the bug report where only {@code always_null} lacks the statistic.
     */
    private Path writeNullableParquetFile(int rowCount, int rareNonNull) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message test { required int64 id; optional double rare; optional double always_null; }"
        );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                // Disable statistics only for always_null so its footer omits null_count, while
                // id and rare keep theirs.
                .withStatisticsEnabled("always_null", false)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                if (i < rareNonNull) {
                    g.add("rare", (double) i);
                }
                // always_null: never assigned -> all rows null.
                writer.write(g);
            }
        }

        Path tempFile = createTempDir().resolve("null_count_test.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        baos.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        baos.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }
}
