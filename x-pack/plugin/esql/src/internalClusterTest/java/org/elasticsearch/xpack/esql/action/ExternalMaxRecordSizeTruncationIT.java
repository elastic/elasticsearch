/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * When the streaming-parallel segmentator hits the {@code max_record_size} cap on a stream-only
 * (gzip) input, the read must honor the {@code error_mode}: a strict policy still hard-fails, while a
 * non-strict policy truncates the read at the undelimitable record and returns the records parsed so
 * far (rather than failing the whole query). The non-strict truncation also surfaces a prominent,
 * client-visible partial-results {@code Warning} — the core requirement of #835, since a partial
 * {@code STATS COUNT(*)} is otherwise a silent under-count. That warning is recorded on a forked
 * parse-worker thread and re-emitted on the driver thread by {@code AsyncExternalSourceOperator} so it
 * propagates back through the normal response-header collection to the client; this test proves that
 * end-to-end by running the query through a chosen coordinator and reading that node's response
 * {@code Warning} headers (mirroring {@code ExternalCsvHivePartitionedIT} and {@code WarningsIT}).
 * Follow-up to the record-boundary livelock fix (capped grow loop). See elastic/esql-planning#835.
 */
public class ExternalMaxRecordSizeTruncationIT extends AbstractExternalDataSourceIT {

    private static final int LEADING_ROWS = 5;
    /** {@code max_record_size} pragma value; {@code 1mb} == {@link #MAX_RECORD_SIZE_BYTES} bytes. */
    private static final String MAX_RECORD_SIZE = "1mb";
    private static final long MAX_RECORD_SIZE_BYTES = 1024L * 1024L;
    /**
     * Size of the single oversized field. It is strictly larger than {@code max_record_size}, so the
     * segmentator can never accumulate a full record within the cap and raises {@code RECORD_TOO_LARGE}
     * — independent of the reader's {@code minimumSegmentSize} floor (asserted in {@link #assertFixtureSizing}).
     */
    private static final int GIANT_RECORD_BYTES = 4 * 1024 * 1024;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, GzipDataSourcePlugin.class);
    }

    /**
     * Pins the fixture's defining property so the test cannot silently change meaning if a constant or
     * the reader's segment floor is later retuned: a single record strictly larger than
     * {@code max_record_size} can never be delimited within the cap, so {@code RECORD_TOO_LARGE} fires
     * regardless of chunk/segment sizing. The {@code 2x} margin keeps that true even when a chunk
     * straddles the leading rows and the start of the giant field.
     */
    @Before
    public void assertFixtureSizing() {
        assertThat(
            "GIANT_RECORD_BYTES must exceed 2x max_record_size so the cap-hit is independent of segment sizing",
            (long) GIANT_RECORD_BYTES,
            greaterThan(2 * MAX_RECORD_SIZE_BYTES)
        );
    }

    /**
     * Default (strict) policy: a record exceeding {@code max_record_size} must fail the query fast with
     * a diagnosable error, as before this change.
     */
    public void testStrictPolicyHardFailsOnOversizedRecord() throws Exception {
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        Path file = writeGzipWithOversizedRecord();
        try {
            String dataset = registerDataset("strict_oversized", StoragePath.fileUri(file), Map.of("header_row", false));
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";
            // Pin allow_partial_results=false (cluster default is true) so the hard failure is thrown
            // rather than swallowed into a partial response.
            EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(pragmas(4, MAX_RECORD_SIZE)).allowPartialResults(false);
            Exception e = expectThrows(Exception.class, () -> run(request, TimeValue.timeValueMinutes(2)).close());
            String trace = ExceptionsHelper.stackTrace(e);
            assertTrue("strict policy must hard-fail on the cap-hit, got: " + trace, trace.contains("record exceeded max_record_size"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Non-strict policy ({@code error_mode: skip_row}): the read truncates at the oversized record and
     * returns the {@link #LEADING_ROWS} rows parsed before it, instead of failing the whole query. The
     * truncation is signalled to the client two ways: a prominent partial-results {@code Warning} (so the
     * under-counting {@code COUNT(*)} is not silent) and the structured {@code is_partial} response flag
     * (so the under-count is machine-detectable). The query runs through a chosen coordinator and we read
     * that node's accumulated response {@code Warning} headers — proving the warning recorded on the forked
     * parse-worker thread is re-emitted on the driver thread and propagates all the way to the client, not
     * just to a hand-bound test {@code ThreadContext}; the {@code is_partial} flag rides back from whichever
     * driver ran the external read (coordinator or data node) through {@code DriverCompletionInfo}.
     */
    public void testSkipRowPolicyReturnsPartialResultsAndWarnsClient() throws Exception {
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        Path file = writeGzipWithOversizedRecord();
        try {
            String dataset = registerDataset(
                "skip_row_oversized",
                StoragePath.fileUri(file),
                Map.of("header_row", false, "error_mode", "skip_row")
            );
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";
            EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(pragmas(4, MAX_RECORD_SIZE));

            DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
            CountDownLatch latch = new CountDownLatch(1);
            AtomicLong count = new AtomicLong(-1);
            AtomicBoolean partial = new AtomicBoolean(false);
            List<String> warnings = new CopyOnWriteArrayList<>();
            AtomicReference<Exception> failure = new AtomicReference<>();
            // ActionListener.wrap (not the run() helper) so we can read both the partial count and the
            // coordinator's response Warning headers; the transport client owns the response ref-count,
            // so we must not close it here (that would double-decRef — see ExternalCsvHivePartitionedIT).
            client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
                try {
                    List<List<Object>> values = getValuesList(response);
                    count.set(((Number) values.get(0).get(0)).longValue());
                    partial.set(response.isPartial());
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
            assertTrue("query did not complete within 2 minutes", latch.await(2, TimeUnit.MINUTES));
            if (failure.get() != null) {
                throw new AssertionError("non-strict read must not fail, but did", failure.get());
            }
            assertThat("non-strict read must return only the rows parsed before the cap-hit", count.get(), equalTo((long) LEADING_ROWS));
            assertTrue(
                "client must receive a prominent partial-results truncation Warning, got: " + warnings,
                warnings.stream().anyMatch(w -> w.contains("results are partial") && w.contains("truncated at byte"))
            );
            assertTrue("a truncated lenient read must flip the response is_partial flag", partial.get());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private QueryPragmas pragmas(int parsingParallelism, String maxRecordSize) {
        return new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.PARSING_PARALLELISM.getKey(), parsingParallelism)
                .put(QueryPragmas.MAX_RECORD_SIZE.getKey(), maxRecordSize)
                .build()
        );
    }

    /**
     * Writes a gzip TSV with {@link #LEADING_ROWS} clean rows, then a single {@link #GIANT_RECORD_BYTES}-byte
     * field terminated by a newline, then a couple of trailing rows. Because that one field is larger than
     * {@code max_record_size}, the segmentator cannot accumulate the whole record within the cap and trips
     * {@code RECORD_TOO_LARGE} (either the grow-loop pre-check or the forward-scan boundary check) before it
     * can reach the terminator — so the read truncates there and the trailing rows are never parsed.
     */
    private Path writeGzipWithOversizedRecord() throws Exception {
        Path file = createTempDir().resolve("oversized.tsv.gz");
        try (
            OutputStream os = Files.newOutputStream(file);
            OutputStream gz = new GZIPOutputStream(os);
            Writer w = new OutputStreamWriter(gz, StandardCharsets.UTF_8)
        ) {
            for (int i = 0; i < LEADING_ROWS; i++) {
                w.write("c0_" + i + "\tc1_" + i + "\tc2_" + i + "\n");
            }
            w.write("x".repeat(GIANT_RECORD_BYTES));
            w.write("\n");
            w.write("after0_0\tafter1_0\tafter2_0\n");
            w.write("after0_1\tafter1_1\tafter2_1\n");
        }
        return file;
    }
}
