/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.HttpEntity;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Types;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.Mode;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.RequestObjectBuilder;
import org.elasticsearch.xpack.esql.telemetry.TookMetrics;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.CsvAssert.assertData;
import static org.elasticsearch.xpack.esql.CsvAssert.assertMetadata;
import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.createInferenceEndpoints;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.deleteInferenceEndpoints;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.loadDataSetIntoEs;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.COMPLETION;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.KNN_FUNCTION_V5;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.RERANK;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.SEMANTIC_TEXT_FIELD_CAPS;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.SOURCE_FIELD_MAPPING;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.TEXT_EMBEDDING_FUNCTION;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.assertNotPartial;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;

// This test can run very long in serverless configurations
@TimeoutSuite(millis = 30 * TimeUnits.MINUTE)
public abstract class EsqlSpecTestCase extends ESRestTestCase {

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    private static final Logger LOGGER = LogManager.getLogger(EsqlSpecTestCase.class);
    private final String fileName;
    private final String groupName;
    private final String testName;
    private final Integer lineNumber;
    protected final CsvTestCase testCase;
    protected final String instructions;
    protected final Mode mode;
    protected static Boolean supportsTook;

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec");
        assertTrue("Not enough specs found " + urls, urls.size() > 0);
        return SpecReader.readScriptSpec(urls, specParser());
    }

    protected EsqlSpecTestCase(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        this.fileName = fileName;
        this.groupName = groupName;
        this.testName = testName;
        this.lineNumber = lineNumber;
        this.testCase = testCase;
        this.instructions = instructions;
        this.mode = randomFrom(Mode.values());
    }

    private static class Protected {
        private volatile boolean completed = false;
        private volatile boolean started = false;
        private volatile Throwable failure = null;

        private void protectedBlock(Callable<Void> callable) {
            if (completed) {
                return;
            }
            // In case tests get run in parallel, we ensure only one setup is run, and other tests wait for this
            synchronized (this) {
                if (completed) {
                    return;
                }
                if (started) {
                    // Should only happen if a previous test setup failed, possibly with partial setup, let's fail fast the current test
                    if (failure != null) {
                        fail(failure, "Previous test setup failed: " + failure.getMessage());
                    }
                    fail("Previous test setup failed with unknown error");
                }
                started = true;
                try {
                    callable.call();
                    completed = true;
                } catch (Throwable t) {
                    failure = t;
                    fail(failure, "Current test setup failed: " + failure.getMessage());
                }
            }
        }

        private synchronized void reset() {
            completed = false;
            started = false;
            failure = null;
        }
    }

    private static final Protected INGEST = new Protected();
    protected static boolean testClustersOk = true;

    @Before
    public void setup() {
        assumeTrue("test clusters were broken", testClustersOk);
        INGEST.protectedBlock(() -> {
            // Inference endpoints must be created before ingesting any datasets that rely on them (mapping of inference_id)
            if (supportsInferenceTestService()) {
                createInferenceEndpoints(adminClient());
            }
            loadDataSetIntoEs(
                client(),
                supportsIndexModeLookup(),
                supportsSourceFieldMapping(),
                supportsInferenceTestService(),
                false,
                supportsExponentialHistograms()
            );
            return null;
        });
    }

    @AfterClass
    public static void wipeTestData() throws IOException {
        if (testClustersOk == false) {
            return;
        }
        try {
            adminClient().performRequest(new Request("DELETE", "/*"));
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
        INGEST.reset();
        deleteInferenceEndpoints(adminClient());
    }

    public boolean logResults() {
        return false;
    }

    public final void test() throws Throwable {
        try {
            shouldSkipTest(testName);
            doTest();
        } catch (Exception e) {
            ensureTestClustersAreOk(e);
            throw reworkException(e);
        }
    }

    protected void ensureTestClustersAreOk(Exception failure) {
        try {
            ensureHealth(client(), "", (request) -> {
                request.addParameter("wait_for_status", "yellow");
                request.addParameter("level", "shards");
            });
        } catch (Exception inner) {
            testClustersOk = false;
            failure.addSuppressed(inner);
        }
    }

    protected void shouldSkipTest(String testName) throws IOException {
        assumeTrue("test clusters were broken", testClustersOk);
        if (requiresInferenceEndpoint()) {
            assumeTrue("Inference test service needs to be supported", supportsInferenceTestService());
        }
        checkCapabilities(adminClient(), testFeatureService, testName, testCase);
        assumeTrue("Test " + testName + " is not enabled", isEnabled(testName, instructions, Version.CURRENT));
        if (supportsSourceFieldMapping() == false) {
            assumeFalse("source mapping tests are muted", testCase.requiredCapabilities.contains(SOURCE_FIELD_MAPPING.capabilityName()));
        }
    }

    protected static void checkCapabilities(
        RestClient client,
        TestFeatureService testFeatureService,
        String testName,
        CsvTestCase testCase
    ) {
        if (hasCapabilities(client, testCase.requiredCapabilities)) {
            return;
        }

        var features = new EsqlFeatures().getFeatures().stream().map(NodeFeature::id).collect(Collectors.toSet());

        for (String feature : testCase.requiredCapabilities) {
            var esqlFeature = "esql." + feature;
            assumeTrue("Requested capability " + feature + " is an ESQL cluster feature", features.contains(esqlFeature));
            assumeTrue("Test " + testName + " requires " + feature, testFeatureService.clusterHasFeature(esqlFeature));
        }
    }

    protected boolean supportsInferenceTestService() {
        return true;
    }

    protected boolean requiresInferenceEndpoint() {
        return Stream.of(
            SEMANTIC_TEXT_FIELD_CAPS.capabilityName(),
            RERANK.capabilityName(),
            COMPLETION.capabilityName(),
            KNN_FUNCTION_V5.capabilityName(),
            TEXT_EMBEDDING_FUNCTION.capabilityName()
        ).anyMatch(testCase.requiredCapabilities::contains);
    }

    protected boolean supportsIndexModeLookup() throws IOException {
        return true;
    }

    protected boolean supportsSourceFieldMapping() throws IOException {
        return true;
    }

    protected boolean supportsExponentialHistograms() {
        return RestEsqlTestCase.hasCapabilities(
            client(),
            List.of(EsqlCapabilities.Cap.EXPONENTIAL_HISTOGRAM_PRE_TECH_PREVIEW_V1.capabilityName())
        );
    }

    protected void doTest() throws Throwable {
        doTest(testCase.query);
    }

    protected final void doTest(String query) throws Throwable {
        RequestObjectBuilder builder = new RequestObjectBuilder(randomFrom(XContentType.values()));

        if (query.toUpperCase(Locale.ROOT).contains("LOOKUP_\uD83D\uDC14")) {
            builder.tables(tables());
        }

        boolean checkTook = supportsTook() && rarely();

        Map<?, ?> prevTooks = checkTook ? tooks() : null;
        Map<String, Object> answer = RestEsqlTestCase.runEsql(
            builder.query(query),
            testCase.assertWarnings(deduplicateExactWarnings()),
            profileLogger,
            mode
        );

        assertNotPartial(answer);

        var expectedColumnsWithValues = loadCsvSpecValues(testCase.expectedResults);

        var metadata = answer.get("columns");
        assertNotNull(metadata);
        @SuppressWarnings("unchecked")
        var actualColumns = (List<Map<String, String>>) metadata;

        Logger logger = logResults() ? LOGGER : null;
        var values = answer.get("values");
        assertNotNull(values);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualValues = (List<List<Object>>) values;

        assertResults(expectedColumnsWithValues, actualColumns, actualValues, testCase.ignoreOrder, logger);

        if (checkTook) {
            LOGGER.info("checking took incremented from {}", prevTooks);
            long took = ((Number) answer.get("took")).longValue();
            int prevTookHisto = ((Number) prevTooks.remove(tookKey(took))).intValue();
            assertMap(tooks(), matchesMap(prevTooks).entry(tookKey(took), prevTookHisto + 1));
        }
    }

    private Map<?, ?> tooks() throws IOException {
        Request request = new Request("GET", "/_xpack/usage");
        HttpEntity entity = client().performRequest(request).getEntity();
        Map<?, ?> usage = XContentHelper.convertToMap(XContentType.JSON.xContent(), entity.getContent(), false);
        Map<?, ?> esql = (Map<?, ?>) usage.get("esql");
        return (Map<?, ?>) esql.get("took");
    }

    /**
     * Should warnings be de-duplicated before checking for exact matches. Defaults
     * to {@code false}, but in some environments we emit duplicate warnings. We'd prefer
     * not to emit duplicate warnings but for now it isn't worth fighting with. So! In
     * those environments we override this to deduplicate.
     * <p>
     *     Note: This only applies to warnings declared as {@code warning:}. Those
     *     declared as {@code warningRegex:} are always a list of
     *     <strong>allowed</strong> warnings. {@code warningRegex:} matches 0 or more
     *     warnings. There is no need to deduplicate because there's no expectation
     *     of an exact match.
     * </p>
     *
     */
    protected boolean deduplicateExactWarnings() {
        return false;
    }

    protected void assertResults(
        ExpectedResults expected,
        List<Map<String, String>> actualColumns,
        List<List<Object>> actualValues,
        boolean ignoreOrder,
        Logger logger
    ) {
        assertMetadata(expected, actualColumns, logger);
        assertData(expected, actualValues, testCase.ignoreOrder, logger, this::valueMapper);
    }

    private Object valueMapper(CsvTestUtils.Type type, Object value) {
        if (value == null) {
            return "null";
        }
        if (type == CsvTestUtils.Type.GEO_POINT || type == CsvTestUtils.Type.CARTESIAN_POINT) {
            // Point tests are failing in clustered integration tests because of tiny precision differences at very small scales
            if (value instanceof String wkt) {
                try {
                    Geometry geometry = WellKnownText.fromWKT(GeometryValidator.NOOP, false, wkt);
                    if (geometry instanceof Point point) {
                        return normalizedPoint(type, point.getX(), point.getY());
                    }
                } catch (Throwable ignored) {}
            }
        }
        if (type == CsvTestUtils.Type.DOUBLE && enableRoundingDoubleValuesOnAsserting()) {
            if (value instanceof List<?> vs) {
                List<Object> values = new ArrayList<>();
                for (Object v : vs) {
                    values.add(valueMapper(type, v));
                }
                return values;
            } else if (value instanceof Double d) {
                return new BigDecimal(d).round(new MathContext(7, RoundingMode.HALF_DOWN)).doubleValue();
            } else if (value instanceof String s) {
                return new BigDecimal(s).round(new MathContext(7, RoundingMode.HALF_DOWN)).doubleValue();
            }
        }
        if (type == CsvTestUtils.Type.TEXT || type == CsvTestUtils.Type.KEYWORD || type == CsvTestUtils.Type.SEMANTIC_TEXT) {
            if (value instanceof String s) {
                value = s.replaceAll("\\\\n", "\n");
            }
        }
        if (type == CsvTestUtils.Type.EXPONENTIAL_HISTOGRAM) {
            if (value instanceof Map<?, ?> map) {
                return ExponentialHistogramXContent.parseForTesting(Types.<Map<String, Object>>forciblyCast(map));
            }
            if (value instanceof String json) {
                try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
                    return ExponentialHistogramXContent.parseForTesting(parser);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return value.toString();
    }

    /**
     * Rounds double values when asserting double values returned in queries.
     * By default, no rounding is performed.
     */
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return false;
    }

    private static String normalizedPoint(CsvTestUtils.Type type, double x, double y) {
        if (type == CsvTestUtils.Type.GEO_POINT) {
            return normalizedGeoPoint(x, y);
        }
        return String.format(Locale.ROOT, "POINT (%f %f)", (float) x, (float) y);
    }

    private static String normalizedGeoPoint(double x, double y) {
        x = decodeLongitude(encodeLongitude(x));
        y = decodeLatitude(encodeLatitude(y));
        return String.format(Locale.ROOT, "POINT (%f %f)", x, y);
    }

    private Throwable reworkException(Throwable th) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(getClass().getName(), groupName + "." + testName, fileName, lineNumber);

        th.setStackTrace(redone);
        return th;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @After
    public void assertRequestBreakerEmptyAfterTests() throws Exception {
        if (testClustersOk) {
            assertRequestBreakerEmpty();
        }
    }

    public static void assertRequestBreakerEmpty() throws Exception {
        assertBusy(() -> {
            HttpEntity entity = adminClient().performRequest(new Request("GET", "/_nodes/stats?metric=breaker")).getEntity();
            Map<?, ?> stats = XContentHelper.convertToMap(XContentType.JSON.xContent(), entity.getContent(), false);
            Map<?, ?> nodes = (Map<?, ?>) stats.get("nodes");

            MapMatcher breakersEmpty = matchesMap().extraOk().entry("estimated_size_in_bytes", 0).entry("estimated_size", "0b");

            MapMatcher nodesMatcher = matchesMap();
            for (Object name : nodes.keySet()) {
                nodesMatcher = nodesMatcher.entry(
                    name,
                    matchesMap().extraOk().entry("breakers", matchesMap().extraOk().entry("request", breakersEmpty))
                );
            }
            assertMap("circuit breakers not reset to 0", stats, matchesMap().extraOk().entry("nodes", nodesMatcher));
        });
    }

    /**
     * "tables" parameter sent if there is a LOOKUP in the request. If you
     * add to this, you must also add to {@link EsqlTestUtils#tables};
     */
    private Map<String, Map<String, RestEsqlTestCase.TypeAndValues>> tables() {
        Map<String, Map<String, RestEsqlTestCase.TypeAndValues>> tables = new TreeMap<>();
        tables.put(
            "int_number_names",
            EsqlTestUtils.table(
                Map.entry("int", new RestEsqlTestCase.TypeAndValues("integer", IntStream.range(0, 10).boxed().toList())),
                Map.entry(
                    "name",
                    new RestEsqlTestCase.TypeAndValues("keyword", IntStream.range(0, 10).mapToObj(EsqlTestUtils::numberName).toList())
                )
            )
        );
        tables.put(
            "long_number_names",
            EsqlTestUtils.table(
                Map.entry("long", new RestEsqlTestCase.TypeAndValues("long", LongStream.range(0, 10).boxed().toList())),
                Map.entry(
                    "name",
                    new RestEsqlTestCase.TypeAndValues("keyword", IntStream.range(0, 10).mapToObj(EsqlTestUtils::numberName).toList())
                )
            )
        );
        tables.put(
            "double_number_names",
            EsqlTestUtils.table(
                Map.entry("double", new RestEsqlTestCase.TypeAndValues("double", List.of(2.03, 2.08))),
                Map.entry("name", new RestEsqlTestCase.TypeAndValues("keyword", List.of("two point zero three", "two point zero eight")))
            )
        );
        tables.put(
            "double_number_names_with_null",
            EsqlTestUtils.table(
                Map.entry("double", new RestEsqlTestCase.TypeAndValues("double", List.of(2.03, 2.08, 0.0))),
                Map.entry(
                    "name",
                    new RestEsqlTestCase.TypeAndValues("keyword", Arrays.asList("two point zero three", "two point zero eight", null))
                )
            )
        );
        tables.put(
            "big",
            EsqlTestUtils.table(
                Map.entry("aa", new RestEsqlTestCase.TypeAndValues("keyword", List.of("foo", "bar", "baz", "foo"))),
                Map.entry("ab", new RestEsqlTestCase.TypeAndValues("keyword", List.of("zoo", "zop", "zoi", "foo"))),
                Map.entry("na", new RestEsqlTestCase.TypeAndValues("integer", List.of(1, 10, 100, 2))),
                Map.entry("nb", new RestEsqlTestCase.TypeAndValues("integer", List.of(-1, -10, -100, -2)))
            )
        );
        return tables;
    }

    protected boolean supportsTook() throws IOException {
        if (supportsTook == null) {
            supportsTook = hasCapabilities(adminClient(), List.of("usage_contains_took"));
        }
        return supportsTook;
    }

    private String tookKey(long took) {
        if (took < 10) {
            return "lt_10ms";
        }
        if (took < 100) {
            return "lt_100ms";
        }
        if (took < TookMetrics.ONE_SECOND) {
            return "lt_1s";
        }
        if (took < TookMetrics.TEN_SECONDS) {
            return "lt_10s";
        }
        if (took < TookMetrics.ONE_MINUTE) {
            return "lt_1m";
        }
        if (took < TookMetrics.TEN_MINUTES) {
            return "lt_10m";
        }
        if (took < TookMetrics.ONE_DAY) {
            return "lt_1d";
        }
        return "gt_1d";
    }
}
