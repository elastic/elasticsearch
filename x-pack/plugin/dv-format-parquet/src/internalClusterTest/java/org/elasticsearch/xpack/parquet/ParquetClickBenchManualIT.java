/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.parquet.docvalues.ClickBenchResultCheck;
import org.elasticsearch.xpack.parquet.docvalues.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.docvalues.ParquetBlobCache;
import org.elasticsearch.xpack.parquet.mount.MountParquetAction;
import org.elasticsearch.xpack.parquet.mount.ParquetMountRequest;
import org.elasticsearch.xpack.parquet.mount.PreviewParquetAction;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * The acceptance target of the plan (A0) for ES|QL: the ClickBench {@code hits.parquet} file, about 100 million rows and 14 GB, mounted
 * from a read-only {@code fs} repository, queried with ES|QL versions of ClickBench queries. The expected values were computed with
 * pyarrow from the same file. It only runs when the file is given:
 * {@code -Dtests.jvm.argline=-Dtests.parquet.hits=/path/to/hits.parquet -Dtests.heap.size=4G}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetClickBenchManualIT extends ESIntegTestCase {

    private static final Logger LOGGER = LogManager.getLogger(ParquetClickBenchManualIT.class);
    private static final long ROWS = 99_997_497L;

    private Path hits() {
        String file = System.getProperty("tests.parquet.hits");
        assumeTrue("set tests.parquet.hits to the path of hits.parquet", file != null && file.isEmpty() == false);
        return PathUtils.get(file);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected boolean forceSingleDataPath() {
        // the shared cache is not allowed on a node with several data paths
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateParquetDocValues.class,
            DataStreamsPlugin.class,
            EncryptionPlugin.class,
            EsqlPlugin.class,
            S3RepositoryPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        String file = System.getProperty("tests.parquet.hits");
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // The test cluster gives every node a random number of 1 to 4 processors, which sizes the esql_worker pool and so decides how many
        // drivers run at once. The timings of a manual run must use the cores of the machine (or -Dtests.parquet.processors=<n>).
        settings.put(
            EsExecutors.NODE_PROCESSORS_SETTING.getKey(),
            Integer.parseInt(System.getProperty("tests.parquet.processors", Integer.toString(Runtime.getRuntime().availableProcessors())))
        );
        // ClickBench Q42 reads the rows after the first 10,000, which ES|QL only returns when the result may be larger
        settings.put("esql.query.result_truncation_max_size", 20_000);
        if (Booleans.parseBoolean(System.getProperty("tests.parquet.noQueryCache", "false"))) {
            // to find out whether a wrong result depends on the cached result of a filter
            settings.put("indices.queries.cache.size", "0b");
        }
        String cache = System.getProperty("tests.parquet.cache", "");
        if (cache.isEmpty() == false) {
            // the node-local cache of byte ranges, which needs a size, because the shared cache is off on a node without the right role
            settings.put(ParquetBlobCache.CACHE_SIZE.getKey(), cache);
            String regionSize = System.getProperty("tests.parquet.regionSize", "");
            if (regionSize.isEmpty() == false) {
                // the region and the range size of the cache: a column chunk of 1 to 3 MB fits better in a small region
                settings.put(ParquetBlobCache.CACHE_REGION_SIZE.getKey(), regionSize);
            }
        }
        if (s3()) {
            // the anonymous setting of repository-s3 exists only on this prototype branch; the bucket is public
            settings.put("s3.client.default.anonymous", true).put("s3.client.default.region", "eu-central-1");
        }
        if (file != null && file.isEmpty() == false) {
            // the repository location must be below path.repo
            settings.put(Environment.PATH_REPO_SETTING.getKey(), PathUtils.get(file).getParent().toString());
        }
        return settings.build();
    }

    /**
     * Registers the read-only repository {@code clickbench}: the directory of the file as an {@code fs} repository, or with
     * {@code -Dtests.parquet.s3=true} the public bucket of ClickBench as an {@code s3} repository (the client of the node is anonymous,
     * see {@link #nodeSettings}), so that the same tests run against S3. In that mode {@code tests.parquet.hits} only has to end in
     * {@code hits.parquet}, and the file is read from the bucket.
     */
    private static void registerRepository(Path hits) {
        Settings.Builder repository = Settings.builder().put("readonly", true);
        if (s3()) {
            repository.put("bucket", "clickhouse-public-datasets").put("base_path", "hits_compatible");
        } else {
            repository.put("location", hits.getParent());
        }
        assertAcked(
            ESIntegTestCase.client()
                .admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "clickbench")
                .setType(s3() ? "s3" : "fs")
                .setSettings(repository)
        );
    }

    /** What the cache of the node has written and missed so far, after a run of a query. */
    private void logCache(String name, int run) {
        if (cacheEnabled()) {
            var stats = internalCluster().getInstance(ParquetBlobCache.class).stats();
            LOGGER.info(
                "--> [{}] run [{}] cache: written [{}] MB, writes [{}], misses [{}], evictions [{}]",
                name,
                run,
                stats.writeBytes() >> 20,
                stats.writeCount(),
                stats.missCount(),
                stats.evictCount()
            );
        }
    }

    private static boolean cacheEnabled() {
        return System.getProperty("tests.parquet.cache", "").isEmpty() == false;
    }

    private static boolean s3() {
        return Booleans.parseBoolean(System.getProperty("tests.parquet.s3", "false"));
    }

    private static List<List<Object>> esql(String query) {
        return esqlWithColumns(query).rows();
    }

    /** The names of the columns of a response, and its rows. */
    private record Result(List<String> columns, List<List<Object>> rows) {}

    /** The request, with the pragmas of {@code -Dtests.parquet.pragmas=name=value,name=value} to find out what a result depends on. */
    private static EsqlQueryRequest request(String query) {
        EsqlQueryRequest request = new EsqlQueryRequest().query(query);
        if (Booleans.parseBoolean(System.getProperty("tests.parquet.noPartial", "false"))) {
            // a driver that fails then fails the query, which shows why it failed
            request.allowPartialResults(false);
        }
        String pragmas = System.getProperty("tests.parquet.pragmas", "");
        if (pragmas.isEmpty() == false) {
            Settings.Builder settings = Settings.builder();
            for (String pragma : pragmas.split(",")) {
                String[] parts = pragma.split("=");
                settings.put(parts[0], parts[1]);
            }
            request.pragmas(new QueryPragmas(settings.build())).acceptedPragmaRisks(true);
        }
        return request;
    }

    private static Result esqlWithColumns(String query) {
        long start = System.nanoTime();
        try (EsqlQueryResponse response = client().execute(EsqlQueryAction.INSTANCE, request(query)).actionGet(15, TimeUnit.MINUTES)) {
            List<List<Object>> rows = new ArrayList<>();
            Iterator<Iterator<Object>> iterator = response.values();
            while (iterator.hasNext()) {
                List<Object> values = new ArrayList<>();
                iterator.next().forEachRemaining(values::add);
                rows.add(values);
            }
            if (response.isPartial()) {
                LOGGER.info("--> PARTIALRESULT [{}] execution info: {}", query, response.getExecutionInfo());
            }
            LOGGER.info(
                "--> [{}] gives {} in [{}] ms",
                query,
                rows.size() > 10 ? rows.subList(0, 10) + "..." : rows,
                (System.nanoTime() - start) / 1_000_000
            );
            return new Result(response.columns().stream().map(ColumnInfoImpl::name).toList(), rows);
        }
    }

    public void testClickBenchQueriesOverTheMountedFile() throws Exception {
        Path hits = hits();
        internalCluster().startNode();
        registerRepository(hits);
        long start = System.nanoTime();
        assertAcked(
            prepareCreate("hits").setSettings(
                Settings.builder()
                    .put("index.mode", "columnar")
                    .put(IndexSettings.ID_FIELD_INDEXED.getKey(), false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put(ParquetBackedSettings.REPOSITORY.getKey(), "clickbench")
                    .putList(ParquetBackedSettings.PIECES.getKey(), hits.getFileName().toString() + "#0+226")
                    .put(
                        ParquetBackedSettings.FIELDS.getKey(),
                        "adv:AdvEngineID,width:ResolutionWidth,date:EventDate,counter:CounterID,phrase:SearchPhrase,url:URL"
                    )
            ).setMapping("""
                {"properties":{
                  "adv":{"type":"integer","index":false},
                  "width":{"type":"integer","index":false},
                  "date":{"type":"integer","index":false},
                  "counter":{"type":"integer","index":false},
                  "phrase":{"type":"keyword","index":false,"multi_value":false},
                  "url":{"type":"keyword","index":false,"multi_value":false}}}""")
        );
        ensureGreen("hits");
        LOGGER.info("--> mounted the file in [{}] ms", (System.nanoTime() - start) / 1_000_000);

        // ClickBench Q1: SELECT COUNT(*)
        assertEquals(List.of(List.of(ROWS)), esql("FROM hits | STATS c = COUNT(*)"));
        // Q2: SELECT COUNT(*) WHERE AdvEngineID <> 0
        assertEquals(List.of(List.of(630_500L)), esql("FROM hits | WHERE adv != 0 | STATS c = COUNT(*)"));
        // Q3: SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)
        List<List<Object>> q3 = esql("FROM hits | STATS s = SUM(adv), c = COUNT(*), a = AVG(width)");
        assertEquals(7_280_088L, ((Number) q3.get(0).get(0)).longValue());
        assertEquals(ROWS, ((Number) q3.get(0).get(1)).longValue());
        assertEquals(1513.4879349030107, ((Number) q3.get(0).get(2)).doubleValue(), 1e-6);
        // Q7: SELECT MIN(EventDate), MAX(EventDate)
        assertEquals(List.of(List.of(15_888, 15_917)), esql("FROM hits | STATS lo = MIN(date), hi = MAX(date)"));
        // Q8: SELECT AdvEngineID, COUNT(*) WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC
        List<List<Object>> q8 = esql("FROM hits | WHERE adv != 0 | STATS c = COUNT(*) BY adv | SORT c DESC | LIMIT 5");
        assertEquals(List.of(404_602L, 2), q8.get(0));
        assertEquals(List.of(113_167L, 27), q8.get(1));
        // a string column: rows with a search phrase
        assertEquals(List.of(List.of(13_172_392L)), esql("FROM hits | WHERE phrase != \"\" | STATS c = COUNT(*)"));
        // a selective filter on a number
        assertEquals(List.of(List.of(738_172L)), esql("FROM hits | WHERE counter == 62 | STATS c = COUNT(*)"));
        // Q21: SELECT COUNT(*) WHERE URL LIKE '%google%', which scans 8.5 GB of URLs
        assertEquals(List.of(List.of(15_911L)), esql("FROM hits | WHERE url LIKE \"*google*\" | STATS c = COUNT(*)"));
        assertEquals(List.of(List.of(1_038L)), esql("FROM hits | WHERE url LIKE \"*google*\" AND phrase != \"\" | STATS c = COUNT(*)"));
        // Q13: the most frequent search phrases, which groups 13 million rows into 6 million groups
        List<List<Object>> phrases = esql(
            "FROM hits | WHERE phrase != \"\" | STATS c = COUNT(*) BY phrase | SORT c DESC, phrase | LIMIT 3"
        );
        assertEquals(List.of(70_263L, "\u043a\u0430\u0440\u0435\u043b\u043a\u0438"), phrases.get(0));
        assertEquals(34_675L, ((Number) phrases.get(1).get(0)).longValue());
        assertEquals(24_580L, ((Number) phrases.get(2).get(0)).longValue());
    }

    /**
     * The same file through the mount API, with every column that the mount can map, {@code EventTime} as {@code @timestamp} and
     * {@code EventDate} as a date. The expected values come from {@code constants/expected_mount.py} in the {@code clickbench} directory
     * next to the sources of this test.
     */
    /** Starts a node, registers the directory of the file as a repository and mounts the file as the data stream {@code hits}. */
    private void mountThroughApi(Path hits) throws Exception {
        internalCluster().startNode();
        registerRepository(hits);
        Map<String, Object> body = Map.of(
            "repository",
            "clickbench",
            "paths",
            List.of(hits.getFileName().toString()),
            "data_stream",
            "hits",
            "timestamp_field",
            "EventTime",
            "fields",
            Map.of(
                "EventTime",
                Map.of("type", "date", "conversion", "seconds_to_millis"),
                "EventDate",
                Map.of("type", "date", "conversion", "days_to_millis")
            ),
            "index_settings",
            Map.of("index.number_of_replicas", 0)
        );
        long start = System.nanoTime();
        Map<String, Object> preview = client().execute(PreviewParquetAction.INSTANCE, new ParquetMountRequest(body))
            .actionGet(5, TimeUnit.MINUTES)
            .body();
        LOGGER.info(
            "--> preview in [{}] ms: rows [{}], unmapped [{}], indices [{}]",
            (System.nanoTime() - start) / 1_000_000,
            preview.get("row_count"),
            preview.get("unmapped"),
            preview.get("indices")
        );
        assertEquals(ROWS, preview.get("row_count"));

        start = System.nanoTime();
        client().execute(MountParquetAction.INSTANCE, new ParquetMountRequest(body)).actionGet(5, TimeUnit.MINUTES);
        ensureGreen("hits");
        LOGGER.info("--> mounted through the API in [{}] ms", (System.nanoTime() - start) / 1_000_000);
    }

    public void testMountApiOverTheWholeFile() throws Exception {
        mountThroughApi(hits());
        // a second pass shows what the cache of the node (-Dtests.parquet.cache=<size>) saves
        int passes = Integer.parseInt(System.getProperty("tests.parquet.passes", "1"));
        for (int pass = 0; pass < passes; pass++) {
            LOGGER.info("--> pass [{}]", pass);
            assertTheMount();
            if (cacheEnabled()) {
                LOGGER.info("--> the cache after pass [{}]: {}", pass, internalCluster().getInstance(ParquetBlobCache.class).stats());
            }
        }
    }

    private static void assertTheMount() {
        assertEquals(List.of(List.of(ROWS)), esql("FROM hits | STATS c = COUNT(*)"));
        assertEquals(
            List.of(List.of("2013-07-01T20:00:00.000Z", "2013-07-31T19:59:59.000Z")),
            esql("FROM hits | STATS lo = MIN(@timestamp), hi = MAX(@timestamp) | EVAL lo = TO_STRING(lo), hi = TO_STRING(hi)")
        );
        assertEquals(
            List.of(List.of(738_172L)),
            esql(
                "FROM hits | WHERE CounterID == 62 AND EventDate >= \"2013-07-01T00:00:00Z\" AND EventDate <= \"2013-07-31T00:00:00Z\" "
                    + "| STATS c = COUNT(*)"
            )
        );
        // a range that holds a part of the days, so that the filter cannot be the whole file
        assertEquals(
            List.of(List.of(12_621_831L)),
            esql("FROM hits | WHERE EventDate >= \"2013-07-14T00:00:00Z\" AND EventDate <= \"2013-07-16T00:00:00Z\" | STATS c = COUNT(*)")
        );
        assertEquals(
            List.of(List.of(2_100_428L)),
            esql("FROM hits | WHERE @timestamp >= \"2013-07-14T00:00:00Z\" AND @timestamp < \"2013-07-15T00:00:00Z\" | STATS c = COUNT(*)")
        );
    }

    /**
     * Runs the queries of a JSON file over the mounted file and compares each result with the rows that DuckDB computed from the same
     * file, so that one run shows every query that differs. {@code clickbench_expected.py} in the {@code clickbench} directory next to the
     * sources of this test (see its README.md) writes the file, which is given with {@code -Dtests.parquet.queries=/path/queries.json}.
     */
    @SuppressWarnings("unchecked")
    public void testQueriesFromFileGiveWhatDuckDbComputes() throws Exception {
        String queries = System.getProperty("tests.parquet.queries");
        assumeTrue("set tests.parquet.queries to the path of the queries file", queries != null && queries.isEmpty() == false);
        // the file holds a JSON array, which the helper reads as the value of a field
        String content = "{\"cases\":" + Files.readString(PathUtils.get(queries), StandardCharsets.UTF_8) + "}";
        List<Object> cases = (List<Object>) XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false).get("cases");
        mountThroughApi(hits());
        // the queries to run, as name prefixes separated by commas, or all of them
        String only = System.getProperty("tests.parquet.queries.only", "");
        List<String> filter = only.isEmpty() ? List.of() : List.of(only.split(","));
        int repeat = Integer.parseInt(System.getProperty("tests.parquet.queries.repeat", "1"));
        List<String> failures = new ArrayList<>();
        for (Object entry : cases) {
            Map<String, Object> testCase = (Map<String, Object>) entry;
            String name = (String) testCase.get("name");
            if (filter.isEmpty() == false && filter.stream().noneMatch(name::startsWith)) {
                continue;
            }
            Object tolerance = testCase.get("tolerance");
            // an offset asks for the rows after the first ones, which the query returns because ES|QL has no OFFSET
            int offset = ((Number) testCase.get("offset")).intValue();
            List<List<Object>> expected = (List<List<Object>>) testCase.get("rows");
            try {
                for (int run = 0; run < repeat; run++) {
                    List<List<Object>> actual = esql((String) testCase.get("esql"));
                    String difference = difference(expected, actual.subList(Math.min(offset, actual.size()), actual.size()), tolerance);
                    LOGGER.info("--> [{}] run [{}] {}", name, run, difference == null ? "equals DuckDB" : "DIFFERS: " + difference);
                    if (difference != null) {
                        failures.add(name + " run " + run + ": " + difference);
                    }
                }
            } catch (Exception e) {
                LOGGER.info("--> [{}] FAILED: {}", name, e.toString());
                failures.add(name + ": " + e);
            }
        }
        assertEquals("queries that differ: " + failures, List.of(), failures);
    }

    /** The first difference between the expected rows and the rows of the response, or {@code null} if they are equal. */
    private static String difference(List<List<Object>> expected, List<List<Object>> actual, Object tolerance) {
        if (expected.size() != actual.size()) {
            return "expected [" + expected.size() + "] rows but got [" + actual.size() + "]";
        }
        for (int row = 0; row < expected.size(); row++) {
            List<Object> e = expected.get(row);
            List<Object> a = actual.get(row);
            if (e.size() != a.size()) {
                return "row [" + row + "] expected [" + e.size() + "] columns but got [" + a.size() + "]";
            }
            for (int column = 0; column < e.size(); column++) {
                double columnTolerance = tolerance instanceof List<?> list
                    ? ((Number) list.get(column)).doubleValue()
                    : ((Number) tolerance).doubleValue();
                if (same(e.get(column), a.get(column), columnTolerance) == false) {
                    return "row [" + row + "] column [" + column + "] expected [" + e.get(column) + "] but got [" + a.get(column) + "]";
                }
            }
        }
        return null;
    }

    private static boolean same(Object expected, Object actual, double tolerance) {
        if (expected instanceof Number e && actual instanceof Number a) {
            boolean integral = (e instanceof Double || e instanceof Float) == false && (a instanceof Double || a instanceof Float) == false;
            // a query with a tolerance is approximate (COUNT_DISTINCT), so its whole numbers may differ too
            if (integral && tolerance <= 1e-6) {
                return e.longValue() == a.longValue();
            }
            return Math.abs(e.doubleValue() - a.doubleValue()) <= tolerance * Math.max(1.0, Math.abs(e.doubleValue()));
        }
        return Objects.equals(expected, actual);
    }

    /**
     * Mounts the file as the plain index {@code hits}, with the date columns that the Rally track maps as dates, and runs the ES|QL
     * queries of the Rally track unchanged ({@code -Dtests.parquet.internal=} the file that {@code clickbench/internal_expected.py} writes
     * from {@code rally-internal-tracks/clickbench/operations/02_all_queries.json}). Each result is checked against DuckDB with
     * {@link ClickBenchResultCheck}, which accepts every valid choice among ties. {@code tests.parquet.queries.only} and
     * {@code tests.parquet.queries.repeat} select and repeat queries.
     */
    @SuppressWarnings("unchecked")
    public void testRallyTrackQueriesGiveWhatDuckDbComputes() throws Exception {
        String file = System.getProperty("tests.parquet.internal");
        assumeTrue("set tests.parquet.internal to the path of the expected results", file != null && file.isEmpty() == false);
        String content = "{\"cases\":" + Files.readString(PathUtils.get(file), StandardCharsets.UTF_8) + "}";
        List<Object> cases = (List<Object>) XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false).get("cases");
        Path hits = hits();
        internalCluster().startNode();
        registerRepository(hits);
        Map<String, Object> mountBody = Map.of(
            "repository",
            "clickbench",
            "paths",
            List.of(hits.getFileName().toString()),
            "index",
            "hits",
            "fields",
            Map.of(
                "EventTime",
                Map.of("type", "date", "conversion", "seconds_to_millis"),
                "ClientEventTime",
                Map.of("type", "date", "conversion", "seconds_to_millis"),
                "LocalEventTime",
                Map.of("type", "date", "conversion", "seconds_to_millis"),
                "EventDate",
                Map.of("type", "date", "conversion", "days_to_millis")
            ),
            "index_settings",
            Map.of("index.number_of_replicas", 0)
        );
        client().execute(MountParquetAction.INSTANCE, new ParquetMountRequest(mountBody)).actionGet(5, TimeUnit.MINUTES);
        ensureGreen("hits");

        String only = System.getProperty("tests.parquet.queries.only", "");
        List<String> filter = only.isEmpty() ? List.of() : List.of(only.split(","));
        int repeat = Integer.parseInt(System.getProperty("tests.parquet.queries.repeat", "1"));
        List<String> failures = new ArrayList<>();
        for (Object entry : cases) {
            Map<String, Object> expected = (Map<String, Object>) entry;
            String name = (String) expected.get("name");
            if (filter.isEmpty() == false && filter.stream().noneMatch(name::startsWith)) {
                continue;
            }
            if (Booleans.parseBoolean(System.getProperty("tests.parquet.profile", "false"))) {
                // the first execution warms the cache and the file, and only the second is logged
                for (int execution = 0; execution < 2; execution++) {
                    try (
                        EsqlQueryResponse response = client().execute(
                            EsqlQueryAction.INSTANCE,
                            new EsqlQueryRequest().query((String) expected.get("esql")).profile(true)
                        ).actionGet(5, TimeUnit.MINUTES)
                    ) {
                        if (execution == 1) {
                            LOGGER.info("--> PROFILE [{}] {}", name, response.profile());
                        }
                    }
                }
            }
            for (int run = 0; run < repeat; run++) {
                // several clients at once change which driver gets which slice of the shard
                int clients = Integer.parseInt(System.getProperty("tests.parquet.queries.parallel", "1"));
                if (clients > 1) {
                    List<Thread> threads = new ArrayList<>();
                    final int currentRun = run;
                    for (int client = 0; client < clients; client++) {
                        Thread thread = new Thread(() -> {
                            try {
                                Result result = esqlWithColumns((String) expected.get("esql"));
                                String problem = ClickBenchResultCheck.check(expected, result.columns(), result.rows());
                                if (problem != null) {
                                    LOGGER.info(
                                        "--> [{}] run [{}] PARALLEL INVALID: {} ACTUAL {}",
                                        name,
                                        currentRun,
                                        problem,
                                        result.rows()
                                    );
                                    synchronized (failures) {
                                        failures.add(name + " run " + currentRun + ": " + problem);
                                    }
                                }
                            } catch (Exception e) {
                                synchronized (failures) {
                                    failures.add(name + " run " + currentRun + ": " + e);
                                }
                            }
                        });
                        threads.add(thread);
                        thread.start();
                    }
                    for (Thread thread : threads) {
                        thread.join();
                    }
                    continue;
                }
                try {
                    long start = System.nanoTime();
                    Result result = esqlWithColumns((String) expected.get("esql"));
                    String problem = ClickBenchResultCheck.check(expected, result.columns(), result.rows());
                    LOGGER.info(
                        "--> [{}] run [{}] {} in [{}] ms",
                        name,
                        run,
                        problem == null ? "VALID" : "INVALID: " + problem,
                        (System.nanoTime() - start) / 1_000_000
                    );
                    logCache(name, run);
                    if (problem != null) {
                        LOGGER.info(
                            "--> [{}] run [{}] ACTUAL ROWS {} EXPECTED ROWS {}",
                            name,
                            run,
                            result.rows().size() > 12 ? result.rows().subList(0, 12) : result.rows(),
                            ((List<Object>) expected.get("rows")).size() > 12
                                ? ((List<Object>) expected.get("rows")).subList(0, 12)
                                : expected.get("rows")
                        );
                        failures.add(name + " run " + run + ": " + problem);
                    }
                } catch (Exception e) {
                    LOGGER.info("--> [{}] run [{}] FAILED: {}", name, run, e.toString());
                    failures.add(name + " run " + run + ": " + e);
                }
            }
        }
        assertEquals("queries with a result that is not valid: " + failures, List.of(), failures);
    }
}
