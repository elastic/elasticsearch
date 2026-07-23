/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Concurrency coverage for the external warm-aggregate short-circuit. The per-coordinator schema cache
 * is the shared mutable state: concurrent cold scans of one file race on the stripe commit (guarded by
 * the per-path {@code KeyedLock}), and concurrent warm reads must all serve the same correct value.
 * These tests complement the deterministic unit coverage in
 * {@code ExternalSourceCacheServiceTests} (commit-lock no-lost-update, read-during-commit) and
 * {@code ExternalStatsCaptureTests} (parallel-harvest sink) by exercising the real coordinator cache
 * end to end, including a randomized soak that fuzzes the cold/warm/commit interleavings.
 */
public class ExternalConcurrentWarmAggregateIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    /**
     * Pins every query to one coordinator: the reconciled schema cache is per-coordinator, not
     * cluster-replicated, so concurrent cold scans and warm reads must hit the same node to race on the
     * same cache (matching {@code ExternalCsvAggregatePushdownIT}).
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testConcurrentWarmCountStarAllShortCircuit() throws Exception {
        int totalRows = 200;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("concurrent_warm", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";
            // Cold once to populate the coordinator cache.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
            }
            // Many concurrent warm reads of the same file: all must short-circuit (zero docs) and return
            // the same correct count. Concurrent reads of the warm entry are pure reads of the cache.
            runInParallel(24, i -> {
                try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                    assertCount(response, totalRows);
                    assertThat("warm read must short-circuit", response.documentsFound(), equalTo(0L));
                }
            });
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    public void testConcurrentColdScansSameFileConverge() throws Exception {
        int totalRows = 200;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("concurrent_warm", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";
            // N queries cold-scan the SAME uncached file at once: each contributes stripe stats to the
            // coordinator cache concurrently (the commit race the KeyedLock guards). Every response must
            // be the correct count regardless of how the commits interleave.
            runInParallel(8, i -> {
                try (var response = run(syncEsqlQueryRequest(query))) {
                    assertCount(response, totalRows);
                }
            });
            // After the storm the cache is consistent: a warm read short-circuits to the same count.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertThat(response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    public void testRandomizedConcurrentQuerySoak() throws Exception {
        // A handful of files of different sizes, a mix of warm-short-circuitable and full-scan queries,
        // fired in random order across several rounds and many threads — fuzzing the cold/warm/commit
        // interleavings for a wrong answer (a 0, a subset extremum, a torn count). Every result must
        // match the precomputed truth (value = id*10, so all rows non-null, MIN=0, MAX=(rows-1)*10).
        int fileCount = 4;
        List<Path> files = new ArrayList<>(fileCount);
        long[] rows = new long[fileCount];
        List<String> datasets = new ArrayList<>(fileCount);
        try {
            for (int f = 0; f < fileCount; f++) {
                rows[f] = 50 + between(0, 150);
                Path file = writeCsvFile((int) rows[f]);
                files.add(file);
                datasets.add(registerDataset("concurrent_soak_" + f, StoragePath.fileUri(file), Map.of()));
            }

            int rounds = 4;
            for (int round = 0; round < rounds; round++) {
                // Resolve all randomness on the test thread (the test's Random is not thread-safe), then
                // dispatch the pre-decided tasks to the worker threads in a shuffled order.
                int taskCount = 24;
                List<int[]> tasks = new ArrayList<>(taskCount); // {fileIndex, shape}
                for (int i = 0; i < taskCount; i++) {
                    tasks.add(new int[] { between(0, fileCount - 1), between(0, 3) });
                }
                Collections.shuffle(tasks, random());

                runInParallel(tasks.size(), i -> {
                    int[] t = tasks.get(i);
                    String dataset = datasets.get(t[0]);
                    long n = rows[t[0]];
                    switch (t[1]) {
                        case 0 -> {
                            try (var r = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"))) {
                                assertSingleLong(r, n);
                            }
                        }
                        case 1 -> {
                            try (var r = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(value)"))) {
                                assertSingleLong(r, n);
                            }
                        }
                        case 2 -> {
                            try (var r = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = MIN(value)"))) {
                                assertSingleLong(r, 0L);
                            }
                        }
                        case 3 -> {
                            try (var r = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = MAX(value)"))) {
                                assertSingleLong(r, (n - 1) * 10);
                            }
                        }
                        default -> throw new AssertionError("unreachable shape " + t[1]);
                    }
                });
            }
        } finally {
            for (Path p : files) {
                Files.deleteIfExists(p);
            }
        }
    }

    private static void assertCount(EsqlQueryResponse response, long expected) {
        assertSingleLong(response, expected);
    }

    private static void assertSingleLong(EsqlQueryResponse response, long expected) {
        List<? extends ColumnInfo> columns = response.columns();
        assertThat(columns.size(), equalTo(1));
        assertThat(columns.get(0).name(), equalTo("c"));
        List<List<Object>> values = getValuesList(response);
        assertThat(values.size(), equalTo(1));
        assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(expected));
    }

    private Path writeCsvFile(int rowCount) throws IOException {
        StringBuilder sb = new StringBuilder("id:integer,name:keyword,value:integer\n");
        for (int i = 0; i < rowCount; i++) {
            sb.append(i).append(",row_").append(i).append(',').append(i * 10).append('\n');
        }
        Path tempFile = createTempDir().resolve("concurrent_warm_test_" + rowCount + "_" + between(0, 1_000_000) + ".csv");
        Files.writeString(tempFile, sb.toString(), StandardCharsets.UTF_8);
        return tempFile;
    }
}
