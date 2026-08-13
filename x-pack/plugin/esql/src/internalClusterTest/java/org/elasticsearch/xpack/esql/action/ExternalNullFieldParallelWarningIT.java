/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code error_mode:null_field} null-fills unparseable fields and must emit a client-visible {@code Warning} so
 * the drift is not silent. The warning must reach the client whichever read path handles the file.
 *
 * <p>Both read paths run under the async source on a background reader thread, so their reader-level
 * {@code SkipWarnings} are relayed through the source buffer and re-emitted on the driver thread whose
 * response headers reach the client. The parallel-parse path (above the 2 MiB threshold) additionally fans
 * the decode loop out across forked {@code esql_external_io} segment-parser threads; each segment's
 * {@code SkipWarnings} must route through that same relay, since a {@code ThreadContext} on a forked worker
 * is never merged back into the response.
 *
 * <p>Both tests read a single-column all-unparseable CSV with the column declared {@code long} under
 * {@code error_mode:null_field}: every value null-fills. The small twin stays on the serial path; the large
 * twin crosses the 2 MiB parallel gate. Each asserts the null-fill warnings reach the client.
 */
public class ExternalNullFieldParallelWarningIT extends AbstractExternalDataSourceIT {

    /** Just under the 2 MiB (2 x 1 MiB minimumSegmentSize) parallel gate: stays serial. */
    private static final int SMALL_ROWS = 50;
    /** Comfortably past the 2 MiB parallel gate so the file splits into several segments. */
    private static final int LARGE_ROWS = 300_000;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /** Baseline: a small (serial-path) all-unparseable declared-long CSV delivers the null-fill warnings. */
    public void testSerialPathDeliversNullFieldWarnings() throws Exception {
        assumeTrue("parsing_parallelism pragma is snapshot-only", Build.current().isSnapshot());
        List<String> warnings = runAndCollectWarnings("null_field_small", SMALL_ROWS, SMALL_ROWS);
        assertTrue(
            "serial path must deliver at least one per-field parse warning, got: " + warnings,
            warnings.stream().anyMatch(w -> w.contains("Failed to parse") && w.contains("[LONG]"))
        );
        assertTrue(
            "serial path must deliver the one-time policy summary, got: " + warnings,
            warnings.stream().anyMatch(w -> w.contains("encountered parse errors handled per policy"))
        );
    }

    /**
     * An all-unparseable declared-long CSV above the parallel gate null-fills the entire column and must still
     * deliver at least one per-field parse warning, so a client does not conclude the parse was clean.
     */
    public void testParallelPathDeliversNullFieldWarnings() throws Exception {
        assumeTrue("parsing_parallelism pragma is snapshot-only", Build.current().isSnapshot());
        List<String> warnings = runAndCollectWarnings("null_field_large", LARGE_ROWS, LARGE_ROWS);
        assertTrue(
            "parallel path must still deliver at least one per-field parse warning, got: " + warnings,
            warnings.stream().anyMatch(w -> w.contains("Failed to parse") && w.contains("[LONG]"))
        );
    }

    /**
     * Registers a single-column CSV of {@code rows} unparseable values with the column declared {@code long}
     * under {@code error_mode:null_field}, runs {@code STATS total=COUNT(*), present=COUNT(val)} on a random
     * coordinator with {@code parsing_parallelism=4}, asserts the data is correct (all values null-filled), and
     * returns that coordinator's accumulated response {@code Warning} headers.
     */
    private List<String> runAndCollectWarnings(String datasetName, int rows, long expectedTotal) throws Exception {
        Path file = createTempDir().resolve(datasetName + ".csv");
        StringBuilder sb = new StringBuilder("val\n");
        for (int i = 0; i < rows; i++) {
            sb.append("notanumber").append(i).append('\n');
        }
        Files.writeString(file, sb.toString());

        registerDataSource("null_field_ds", Map.of());
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("val", new DatasetFieldMapping("long", "val"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    datasetName,
                    "null_field_ds",
                    StoragePath.fileUri(file),
                    null,
                    new HashMap<>(Map.of("format", "csv", "error_mode", "null_field")),
                    mapping
                )
            )
        );

        String query = "FROM " + datasetName + " | STATS total = COUNT(*), present = COUNT(val)";
        EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(
            new QueryPragmas(Settings.builder().put(QueryPragmas.PARSING_PARALLELISM.getKey(), 4).build())
        );

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        CountDownLatch latch = new CountDownLatch(1);
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicReference<List<List<Object>>> values = new AtomicReference<>();
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            try {
                values.set(getValuesList(response));
                ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
                    .getThreadPool()
                    .getThreadContext();
                warnings.addAll(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()));
            } finally {
                latch.countDown();
            }
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue("query did not complete within timeout", latch.await(2, TimeUnit.MINUTES));
        if (failure.get() != null) {
            throw new AssertionError("null_field read must not fail, but did", failure.get());
        }
        assertThat("COUNT(*) must see every row", ((Number) values.get().get(0).get(0)).longValue(), equalTo(expectedTotal));
        assertThat(
            "every declared-long value is unparseable -> null-filled",
            ((Number) values.get().get(0).get(1)).longValue(),
            equalTo(0L)
        );
        return warnings;
    }
}
