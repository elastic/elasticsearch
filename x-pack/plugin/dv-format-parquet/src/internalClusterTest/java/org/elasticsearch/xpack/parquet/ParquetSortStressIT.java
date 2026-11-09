/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.parquet.docvalues.ParquetTestFiles;
import org.elasticsearch.xpack.parquet.mount.MountParquetAction;
import org.elasticsearch.xpack.parquet.mount.ParquetMountRequest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * The same rows as a Parquet mount and as an index of Elasticsearch, and the same sorted top N asked many times at once over both, to find
 * out whether a wrong result belongs to the Parquet format or to the way ES|QL splits a segment between its drivers. The rows have times
 * that are not in order and that tie, as the times of the ClickBench file do, a string as the second sort key, and a filter. It only
 * runs with {@code -Dtests.parquet.stress=true}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetSortStressIT extends ESIntegTestCase {

    private static final Logger LOGGER = LogManager.getLogger(ParquetSortStressIT.class);
    private static final String QUERY = "FROM %s | WHERE s != \"\" | SORT t, s | KEEP t, s | LIMIT 10";

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateParquetDocValues.class, EncryptionPlugin.class, EsqlPlugin.class);
    }

    private static List<List<Object>> esql(String query, Settings pragmas) {
        EsqlQueryRequest request = new EsqlQueryRequest().query(query);
        if (pragmas.isEmpty() == false) {
            request.pragmas(new QueryPragmas(pragmas)).acceptedPragmaRisks(true);
        }
        try (EsqlQueryResponse response = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(2, TimeUnit.MINUTES)) {
            List<List<Object>> rows = new ArrayList<>();
            Iterator<Iterator<Object>> iterator = response.values();
            while (iterator.hasNext()) {
                List<Object> values = new ArrayList<>();
                iterator.next().forEachRemaining(v -> values.add(v instanceof Integer i ? (Object) i.longValue() : v));
                rows.add(values);
            }
            return rows;
        }
    }

    /** How many of {@code clients * rounds} executions of the query give rows other than {@code expected}. */
    private static int wrongResults(String index, Settings pragmas, List<List<Object>> expected, int clients, int rounds) throws Exception {
        AtomicInteger wrong = new AtomicInteger();
        String query = String.format(java.util.Locale.ROOT, QUERY, index);
        for (int round = 0; round < rounds; round++) {
            List<Thread> threads = new ArrayList<>();
            for (int client = 0; client < clients; client++) {
                Thread thread = new Thread(() -> {
                    List<List<Object>> rows = esql(query, pragmas);
                    if (expected.equals(rows) == false) {
                        wrong.incrementAndGet();
                        LOGGER.info("--> [{}] WRONG {} EXPECTED {}", index, rows, expected);
                    }
                });
                threads.add(thread);
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
        }
        return wrong.get();
    }

    public void testSortedTopNOverAMountAndOverAnIndex() throws Exception {
        assumeTrue("set tests.parquet.stress to run", Booleans.parseBoolean(System.getProperty("tests.parquet.stress", "false")));
        internalCluster().startNode();
        int rows = Integer.parseInt(System.getProperty("tests.parquet.stress.rows", "240000"));
        int rowGroupRows = Integer.parseInt(System.getProperty("tests.parquet.stress.rowgroup", "24000"));
        long[] t = new long[rows];
        String[] s = new String[rows];
        String[] words = new String[40];
        for (int w = 0; w < words.length; w++) {
            words[w] = "w" + w;
        }
        // the times are wide everywhere, and the smallest ones are a few rows scattered over the file, as in the ClickBench file
        for (int i = 0; i < rows; i++) {
            t[i] = 1_000 + randomLongBetween(0, 1_000_000);
            s[i] = randomIntBetween(0, 9) == 0 ? "" : words[randomIntBetween(0, words.length - 1)];
        }
        int scattered = Integer.parseInt(System.getProperty("tests.parquet.stress.scattered", "40"));
        for (int k = 0; k < scattered; k++) {
            t[randomIntBetween(0, rows - 1)] = randomIntBetween(0, 1);
        }
        // the ten smallest rows by t and then s among those with a string, found with one pass
        List<long[]> best = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            if (s[i].isEmpty() == false && (best.size() < 10 || t[i] <= best.get(9)[0])) {
                best.add(new long[] { t[i], i });
                best.sort(Comparator.<long[]>comparingLong(a -> a[0]).thenComparing(a -> s[(int) a[1]]));
                if (best.size() > 10) {
                    best.remove(10);
                }
            }
        }
        List<List<Object>> expected = new ArrayList<>();
        for (long[] row : best) {
            expected.add(List.of(row[0], s[(int) row[1]]));
        }

        // the mount
        Path location = randomRepoPath();
        Path file = location.resolve("stress/part.parquet");
        Files.createDirectories(file.getParent());
        Files.write(file, ParquetTestFiles.writeTimesAndStrings(t, s, rowGroupRows));
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "lake")
                .setType("fs")
                .setSettings(Settings.builder().put("location", location).put("readonly", true))
        );
        client().execute(
            MountParquetAction.INSTANCE,
            new ParquetMountRequest(
                Map.of(
                    "repository",
                    "lake",
                    "paths",
                    List.of("stress/part.parquet"),
                    "index",
                    "mounted",
                    "index_settings",
                    Map.of("index.number_of_replicas", 0)
                )
            )
        ).actionGet(2, TimeUnit.MINUTES);

        boolean withPlain = Booleans.parseBoolean(System.getProperty("tests.parquet.stress.plain", "true"));
        if (withPlain) {
            // the same rows in a columnar index of Elasticsearch, in one segment
            assertAcked(
                prepareCreate("plain").setSettings(
                    Settings.builder().put("index.mode", "columnar").put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
                ).setMapping("""
                    {"properties":{"t":{"type":"long","index":false},"s":{"type":"keyword","index":false,"multi_value":false}}}""")
            );
            for (int start = 0; start < rows; start += 5_000) {
                BulkRequestBuilder bulk = client().prepareBulk();
                for (int i = start; i < Math.min(rows, start + 5_000); i++) {
                    bulk.add(prepareIndex("plain").setSource("t", t[i], "s", s[i]));
                }
                assertFalse(bulk.get().hasFailures());
            }
            refresh("plain");
            assertEquals(0, indicesAdmin().prepareForceMerge("plain").setMaxNumSegments(1).get().getFailedShards());
            ensureGreen("plain");

        }
        ensureGreen("mounted");
        int clients = Integer.parseInt(System.getProperty("tests.parquet.stress.clients", "8"));
        int rounds = Integer.parseInt(System.getProperty("tests.parquet.stress.rounds", "60"));
        Settings pragmas = Settings.builder()
            .put("min_docs_per_slice", Integer.parseInt(System.getProperty("tests.parquet.stress.minslice", "3000")))
            .put("task_concurrency", Integer.parseInt(System.getProperty("tests.parquet.stress.concurrency", "8")))
            .build();
        assertEquals("the query itself", expected, esql(String.format(java.util.Locale.ROOT, QUERY, "mounted"), pragmas));
        EsqlQueryRequest profiled = new EsqlQueryRequest().query(String.format(java.util.Locale.ROOT, QUERY, "mounted")).profile(true);
        profiled.pragmas(new QueryPragmas(pragmas)).acceptedPragmaRisks(true);
        try (EsqlQueryResponse response = client().execute(EsqlQueryAction.INSTANCE, profiled).actionGet(2, TimeUnit.MINUTES)) {
            String profile = response.profile().toString();
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("\"processed_slices\":(\\d+).*?\"total_slices\":(\\d+)")
                .matcher(profile);
            while (matcher.find()) {
                LOGGER.info("--> driver processed [{}] of [{}] slices", matcher.group(1), matcher.group(2));
            }
        }
        int wrongPlain = withPlain ? wrongResults("plain", pragmas, expected, clients, rounds) : 0;
        int wrongMounted = wrongResults("mounted", pragmas, expected, clients, rounds);
        LOGGER.info("--> wrong results of [{}] executions: index [{}], mount [{}]", clients * rounds, wrongPlain, wrongMounted);
        assertEquals("wrong results: index=" + wrongPlain + " mount=" + wrongMounted, 0, wrongPlain + wrongMounted);
    }
}
