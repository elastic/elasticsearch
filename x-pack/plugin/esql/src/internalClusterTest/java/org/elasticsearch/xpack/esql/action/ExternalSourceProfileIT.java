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
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetReaderStatus;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderStatus;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * End-to-end coverage that the profile-observability fields added to {@code AsyncExternalSourceOperator.Status}
 * and {@code EsqlQueryProfile.dataset_resolution} are populated when EXTERNAL and FROM &lt;dataset&gt;
 * queries execute against a local Parquet fixture.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalSourceProfileIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    /**
     * {@link EsqlPluginWithEnterpriseOrTrialLicense} suppresses {@link ExtensiblePlugin#loadExtensions}
     * to keep the IT base lean (extensions can pull in heavy deps); we need extensions ON so the
     * datasource plugins added in {@link #nodePlugins()} can register their format readers / storage
     * providers via SPI. This subclass restores the default behavior — call {@code super} explicitly.
     */
    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    /** Pass-through validator for type {@code test} so dataset registration does not reject {@code file://} URIs. */
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
                out.put(e.getKey(), new DataSourceSetting(e.getValue(), false));
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
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(ParquetDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        plugins.add(TestDataSourcePlugin.class);
        return plugins;
    }

    /** Pin the planner to deterministic shapes so the AsyncExternalSourceOperator is reliably present. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
    }

    @After
    public void cleanupRegistry() throws Exception {
        try {
            client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { "profile_ds" }))
                .get(30, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // best-effort
        }
        try {
            client().execute(
                DeleteDataSourceAction.INSTANCE,
                new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { "profile_src" })
            ).get(30, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // best-effort
        }
    }

    public void testExternalQueryProfileFieldsArePopulated() throws Exception {
        Path parquetFile = writeParquetFile(300, 100);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(parquetFile) + "\" | LIMIT 5";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request, TIMEOUT)) {
                assertNotNull("profile must be present (request had profile=true)", response.profile());
                AsyncExternalSourceOperator.Status status = findAsyncExternalSourceStatus(response);
                // splitsTotal/currentSplit/processNanos are set synchronously by the operator and producer
                // before the consumer can observe EOF, so they are reliable across single-file paths.
                assertThat("process_nanos should be populated by the read loop", status.processNanos(), greaterThan(0L));
                assertThat(status.splitsTotal(), greaterThanOrEqualTo(1));
                assertThat(status.currentSplit(), greaterThanOrEqualTo(1));
                // bytesRead/splitsProcessed/formatReader live behind a producer-completion async hop on the
                // single-file path; the consumer can observe EOF before they're written, so the only reliable
                // assertion here is that the fields are present (i.e. not negative for the numeric ones).
                assertThat(status.splitsProcessed(), greaterThanOrEqualTo(0));
                assertThat(status.bytesRead(), greaterThanOrEqualTo(0L));
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    public void testExternalScanCountersArePopulated() throws Exception {
        // Many rows with small row groups produce several row-group splits for a single file.
        Path parquetFile = writeParquetFile(300, 25);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(parquetFile) + "\" | LIMIT 5";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request, TIMEOUT)) {
                assertNotNull("execution info must be present", response.getExecutionInfo());
                EsqlQueryProfile profile = response.getExecutionInfo().queryProfile();
                // A single Parquet file is scanned, split into one or more row-group ranges.
                assertEquals("exactly one file scanned", 1, profile.filesScanned());
                assertThat("at least one split scanned", profile.splitsScanned(), greaterThanOrEqualTo(1));
                assertThat("bytes scanned populated", profile.bytesScanned(), greaterThan(0L));
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * A {@code COUNT(*)} that can be answered from the source's row-count metadata must not scan any
     * files: split discovery is skipped (see {@code ComputeService.canSkipSplitDiscovery}), so the
     * scan counters stay at zero and are omitted from the profile JSON.
     */
    public void testMetadataOnlyCountStarScansNoFiles() throws Exception {
        Path parquetFile = writeParquetFile(300, 25);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(parquetFile) + "\" | STATS c = COUNT(*)";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request, TIMEOUT)) {
                assertNotNull("execution info must be present", response.getExecutionInfo());
                EsqlQueryProfile profile = response.getExecutionInfo().queryProfile();
                assertEquals("metadata-only COUNT(*) must scan no files", 0, profile.filesScanned());
                assertEquals("metadata-only COUNT(*) must scan no splits", 0, profile.splitsScanned());
                assertEquals("metadata-only COUNT(*) must scan no bytes", 0L, profile.bytesScanned());
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * The three-state warm-aggregate profiling signal (esql-planning#909), end to end over CSV.
     * CSV has no embedded row count, so the first {@code COUNT(*)} must scan the whole file. The scan
     * harvests canonical-stripe statistics and reconciles them into the coordinator's source-stats cache;
     * a second identical {@code COUNT(*)} is then answered purely from those stats with the scan
     * short-circuited away. The query profile distinguishes the two outcomes positively:
     * <ul>
     *   <li><b>cold-harvest</b> — the scan ran ({@code splits_scanned > 0}) and harvested stripes into the
     *   coordinator cache; {@code external_warm_aggregates == 0} (no short-circuit). The harvest is proven
     *   end-to-end by the subsequent pass going warm. (The per-scan {@code stripesCommitted()} accessor on
     *   the operator status carries the same signal and is unit-tested in
     *   {@code AsyncExternalSourceOperatorStatusTests}, but on the COUNT short-circuit path the consumer
     *   reaches EOF before the producer's close-time stripe commit lands — the documented async hop also
     *   affecting {@code bytes_read} / {@code format_reader} — so it is not asserted here.)</li>
     *   <li><b>warm</b> — the aggregate was served from stripes ({@code external_warm_aggregates > 0})
     *   with ZERO scan ({@code splits_scanned == 0}, no {@code AsyncExternalSourceOperator} in the
     *   profile). The positive "served from stripes" signal — not inferred from latency.</li>
     * </ul>
     * The <b>miss</b> third state — a scan that ran but committed no usable stripes
     * ({@code stripesCommitted() == 0}) — is covered at the unit level in
     * {@code AsyncExternalSourceOperatorStatusTests}; it is exactly the {@code stripesCommitted() == 0}
     * arm, distinct from cold-harvest's positive count.
     */
    public void testCsvCountStarColdHarvestThenWarmServedFromStripes() throws Exception {
        int rowCount = 200;
        Path csvFile = writeCsvFile(rowCount);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(csvFile) + "\" | STATS c = COUNT(*)";

            // COLD-HARVEST: no cached stats yet, so the file is scanned to answer COUNT(*); the scan
            // harvests canonical stripes into the coordinator cache for the next query.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCountValue(response, rowCount);
                EsqlQueryProfile profile = response.getExecutionInfo().queryProfile();
                assertEquals("cold COUNT(*) scans the one CSV file", 1, profile.filesScanned());
                // The CSV file is far below the 64MB target split size and CSV is not range-aware, so it
                // produces exactly one whole-file split whose estimated size is the file length. Asserting
                // the exact split count also proves the scan stats are recorded once: if the top-level and
                // fragment discovery paths ever both counted this source, splitsScanned would read 2.
                assertEquals("cold COUNT(*) scans exactly one split", 1, profile.splitsScanned());
                assertEquals("cold COUNT(*) reads the whole CSV file", Files.size(csvFile), profile.bytesScanned());
                assertEquals("cold COUNT(*) does not short-circuit warm", 0, profile.externalWarmAggregates());
            }

            // WARM: the harvested stats were reconciled into the coordinator cache, so COUNT(*) is served
            // from stripes and no file is scanned. external_warm_aggregates carries the affirmative signal.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCountValue(response, rowCount);
                EsqlQueryProfile profile = response.getExecutionInfo().queryProfile();
                assertEquals("warm COUNT(*) scans no files", 0, profile.filesScanned());
                assertEquals("warm COUNT(*) scans no splits", 0, profile.splitsScanned());
                assertEquals("warm COUNT(*) scans no bytes", 0L, profile.bytesScanned());
                assertEquals("warm COUNT(*) must report exactly one aggregate served from stripes", 1, profile.externalWarmAggregates());
                assertThat(
                    "warm short-circuit runs no external-source operator",
                    findAsyncExternalSourceStatusOrNull(response),
                    nullValue()
                );
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    public void testFromDatasetProfileHasDatasetResolutionSpan() throws Exception {
        Path parquetFile = writeParquetFile(300, 100);
        try {
            assertAcked(
                client().execute(
                    PutDataSourceAction.INSTANCE,
                    new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "profile_src", "test", null, new HashMap<>())
                )
            );
            assertAcked(
                client().execute(
                    PutDatasetAction.INSTANCE,
                    new PutDatasetAction.Request(
                        TIMEOUT,
                        TIMEOUT,
                        "profile_ds",
                        "profile_src",
                        StoragePath.fileUri(parquetFile),
                        null,
                        new HashMap<>()
                    )
                )
            );

            var request = syncEsqlQueryRequest("FROM profile_ds | LIMIT 5");
            request.profile(true);

            try (var response = run(request, TIMEOUT)) {
                assertNotNull("profile must be present (request had profile=true)", response.profile());
                assertNotNull("execution info must be present", response.getExecutionInfo());

                TimeSpanMarker datasetMarker = response.getExecutionInfo().queryProfile().datasetResolution();
                assertThat("dataset_resolution marker must exist", datasetMarker, notNullValue());
                assertThat("dataset_resolution span must be recorded", datasetMarker.timeSpan(), notNullValue());

                AsyncExternalSourceOperator.Status status = findAsyncExternalSourceStatus(response);
                assertThat(status.processNanos(), greaterThan(0L));
                assertThat(status.splitsTotal(), greaterThanOrEqualTo(1));
                assertThat(status.currentSplit(), greaterThanOrEqualTo(1));
                assertThat(status.splitsProcessed(), greaterThanOrEqualTo(0));
                assertThat(status.bytesRead(), greaterThanOrEqualTo(0L));
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * End-to-end coverage for the {@code format_reader} sub-object on
     * {@link AsyncExternalSourceOperator.Status}. The query intentionally drains every row (no
     * {@code LIMIT}) so the producer reaches its terminal {@code DONE} / EOF callback before the
     * consumer reads the operator status — that callback is where the producer commits the latest
     * format-reader snapshot to the buffer. With a {@code LIMIT} short enough to short-circuit
     * before the producer drains, the consumer can observe {@code formatReader == Map.of()}
     * (the race the prior version of this test fell into and the reason it was deleted).
     * <p>
     * Drives a small fixture so the full drain is fast even without {@code LIMIT}.
     */
    public void testFormatReaderSnapshotPopulatedAfterFullDrain() throws Exception {
        Path parquetFile = writeParquetFile(50, 25);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(parquetFile) + "\"";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request, TIMEOUT)) {
                assertNotNull("profile must be present (request had profile=true)", response.profile());
                AsyncExternalSourceOperator.Status status = findAsyncExternalSourceStatus(response);
                FormatReaderStatus formatReader = status.formatReader();
                assertThat(
                    "format_reader snapshot must be populated after the producer drains the file",
                    formatReader,
                    instanceOf(ParquetReaderStatus.class)
                );
                ParquetReaderStatus parquetStatus = (ParquetReaderStatus) formatReader;
                assertThat(
                    "multi-row-group fixture should report at least one row group",
                    parquetStatus.rowGroupsInFile(),
                    greaterThanOrEqualTo(1L)
                );
                // read_nanos is wall-time and can read as zero on fast / containerized CI runners
                // (sub-microsecond synchronous reads + low-resolution clocks). Assert non-negative
                // rather than a strict positive — the deterministic shape signal lives in row_groups_in_file.
                assertThat("read_nanos must be non-negative", parquetStatus.readNanos(), greaterThanOrEqualTo(0L));
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    private static AsyncExternalSourceOperator.Status findAsyncExternalSourceStatus(EsqlQueryResponse response) {
        AsyncExternalSourceOperator.Status found = findAsyncExternalSourceStatusOrNull(response);
        assertThat("expected at least one AsyncExternalSourceOperator.Status in the driver profiles", found, notNullValue());
        return found;
    }

    /**
     * Same scan over the driver profiles as {@link #findAsyncExternalSourceStatus} but returns {@code null}
     * rather than asserting presence — used to assert the warm short-circuit ran NO external-source
     * operator (and therefore could not have scanned).
     */
    private static AsyncExternalSourceOperator.Status findAsyncExternalSourceStatusOrNull(EsqlQueryResponse response) {
        AsyncExternalSourceOperator.Status found = null;
        assertThat(response.profile(), notNullValue());
        for (var driver : response.profile().drivers()) {
            for (var op : driver.operators()) {
                if (op.status() instanceof AsyncExternalSourceOperator.Status s) {
                    found = s;
                }
            }
        }
        return found;
    }

    private static void assertCountValue(EsqlQueryResponse response, long expected) {
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expected));
    }

    private Path writeCsvFile(int rowCount) throws IOException {
        StringBuilder sb = new StringBuilder("id:integer,name:keyword,value:integer\n");
        for (int i = 0; i < rowCount; i++) {
            sb.append(i).append(",row_").append(i).append(',').append(i * 10).append('\n');
        }
        Path tempFile = createTempDir().resolve("count_star_scan_test.csv");
        Files.write(tempFile, sb.toString().getBytes(StandardCharsets.UTF_8));
        return tempFile;
    }

    private Path writeParquetFile(int rowCount, int rowGroupSize) throws IOException {
        return writeParquetFileTo(createTempDir().resolve("profile_test.parquet"), rowCount, rowGroupSize);
    }

    private static Path writeParquetFileTo(Path target, int rowCount, int rowGroupSize) throws IOException {
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

        Files.write(target, baos.toByteArray());
        return target;
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
