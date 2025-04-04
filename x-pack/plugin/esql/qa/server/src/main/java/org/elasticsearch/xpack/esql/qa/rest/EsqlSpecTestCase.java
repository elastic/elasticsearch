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
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.RequestObjectBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

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
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.availableDatasetsForEs;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.clusterHasInferenceEndpoint;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.clusterHasRerankInferenceEndpoint;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.clusterHasRerankInferenceEndpoint;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.createInferenceEndpoint;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.createRerankInferenceEndpoint;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.createRerankInferenceEndpoint;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.deleteInferenceEndpoint;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.deleteRerankInferenceEndpoint;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.loadDataSetIntoEs;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.cap;

// This test can run very long in serverless configurations
@TimeoutSuite(millis = 30 * TimeUnits.MINUTE)
public abstract class EsqlSpecTestCase extends ESRestTestCase {

    // To avoid referencing the main module, we replicate EsqlFeatures.ASYNC_QUERY.id() here
    protected static final String ASYNC_QUERY_FEATURE_ID = "esql.async_query";

    private static final Logger LOGGER = LogManager.getLogger(EsqlSpecTestCase.class);
    private final String fileName;
    private final String groupName;
    private final String testName;
    private final Integer lineNumber;
    protected final CsvTestCase testCase;
    protected final String instructions;
    protected final Mode mode;

    public enum Mode {
        SYNC,
        ASYNC
    }

    @ParametersFactory(argumentFormatting = "%2$s.%3$s %7$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec");
        assertTrue("Not enough specs found " + urls, urls.size() > 0);
        List<Object[]> specs = SpecReader.readScriptSpec(urls, specParser());

        int len = specs.get(0).length;
        List<Object[]> testcases = new ArrayList<>();
        for (var spec : specs) {
            for (Mode mode : Mode.values()) {
                Object[] obj = new Object[len + 1];
                System.arraycopy(spec, 0, obj, 0, len);
                obj[len] = mode;
                testcases.add(obj);
            }
        }
        return testcases;
    }

    protected EsqlSpecTestCase(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        Mode mode
    ) {
        this.fileName = fileName;
        this.groupName = groupName;
        this.testName = testName;
        this.lineNumber = lineNumber;
        this.testCase = testCase;
        this.instructions = instructions;
        this.mode = mode;
    }

    @Before
    public void setup() throws IOException {
        if (supportsInferenceTestService() && clusterHasInferenceEndpoint(client()) == false) {
            createInferenceEndpoint(client());
        }

        if (supportsInferenceTestService() && clusterHasRerankInferenceEndpoint(client()) == false) {
            createRerankInferenceEndpoint(client());
        }

        if (indexExists(availableDatasetsForEs(client(), supportsIndexModeLookup()).iterator().next().indexName()) == false) {
            loadDataSetIntoEs(client(), supportsIndexModeLookup());
        }
    }

    protected boolean supportsAsync() {
        return clusterHasFeature(ASYNC_QUERY_FEATURE_ID); // the Async API was introduced in 8.13.0
    }

    @AfterClass
    public static void wipeTestData() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "/*"));
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }

        deleteInferenceEndpoint(client());
        deleteRerankInferenceEndpoint(client());
    }

    public boolean logResults() {
        return false;
    }

    public final void test() throws Throwable {
        try {
            shouldSkipTest(testName);
            doTest();
        } catch (Exception e) {
            throw reworkException(e);
        }
    }

    protected void shouldSkipTest(String testName) throws IOException {
        if (testCase.requiredCapabilities.contains("semantic_text_field_caps")) {
            assumeTrue("Inference test service needs to be supported for semantic_text", supportsInferenceTestService());
        }
        checkCapabilities(adminClient(), testFeatureService, testName, testCase);
        assumeTrue("Test " + testName + " is not enabled", isEnabled(testName, instructions, Version.CURRENT));
        if (testCase.requiredCapabilities.contains(cap(EsqlFeatures.METRICS_SYNTAX))) {
            assumeTrue("Skip time-series tests in mixed clusters until the development stabilizes", supportTimeSeriesCommand());
        }
    }

    /**
     * Skip time-series tests in mixed clusters until the development stabilizes
     */
    protected boolean supportTimeSeriesCommand() {
        return true;
    }

    protected static void checkCapabilities(RestClient client, TestFeatureService testFeatureService, String testName, CsvTestCase testCase)
        throws IOException {
        if (hasCapabilities(client, testCase.requiredCapabilities)) {
            return;
        }

        var features = Stream.concat(
            new EsqlFeatures().getFeatures().stream(),
            new EsqlFeatures().getHistoricalFeatures().keySet().stream()
        ).map(NodeFeature::id).collect(Collectors.toSet());

        for (String feature : testCase.requiredCapabilities) {
            var esqlFeature = "esql." + feature;
            assumeTrue("Requested capability " + feature + " is an ESQL cluster feature", features.contains(esqlFeature));
            assumeTrue("Test " + testName + " requires " + feature, testFeatureService.clusterHasFeature(esqlFeature));
        }
    }

    protected static boolean hasCapabilities(List<String> requiredCapabilities) throws IOException {
        return hasCapabilities(adminClient(), requiredCapabilities);
    }

    public static boolean hasCapabilities(RestClient client, List<String> requiredCapabilities) throws IOException {
        if (requiredCapabilities.isEmpty()) {
            return true;
        }
        try {
            if (clusterHasCapability(client, "POST", "/_query", List.of(), requiredCapabilities).orElse(false)) {
                return true;
            }
            LOGGER.info("capabilities API returned false, we might be in a mixed version cluster so falling back to cluster features");
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() / 100 == 4) {
                /*
                 * The node we're testing against is too old for the capabilities
                 * API which means it has to be pretty old. Very old capabilities
                 * are ALSO present in the features API, so we can check them instead.
                 *
                 * It's kind of weird that we check for *any* 400, but that's required
                 * because old versions of Elasticsearch return 400, not the expected
                 * 404.
                 */
                LOGGER.info("capabilities API failed, falling back to cluster features");
            } else {
                throw e;
            }
        }
        return false;
    }

    protected boolean supportsInferenceTestService() {
        return true;
    }

    protected boolean supportsIndexModeLookup() throws IOException {
        return true;
    }

    protected final void doTest() throws Throwable {
        RequestObjectBuilder builder = new RequestObjectBuilder(randomFrom(XContentType.values()));

        if (testCase.query.toUpperCase(Locale.ROOT).contains("LOOKUP_\uD83D\uDC14")) {
            builder.tables(tables());
        }

        Map<String, Object> answer = runEsql(builder.query(testCase.query), testCase.assertWarnings(deduplicateExactWarnings()));

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

    private Map<String, Object> runEsql(RequestObjectBuilder requestObject, AssertWarnings assertWarnings) throws IOException {
        if (mode == Mode.ASYNC) {
            assert supportsAsync();
            return RestEsqlTestCase.runEsqlAsync(requestObject, assertWarnings);
        } else {
            return RestEsqlTestCase.runEsqlSync(requestObject, assertWarnings);
        }
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
                return new BigDecimal(d).round(new MathContext(7, RoundingMode.DOWN)).doubleValue();
            } else if (value instanceof String s) {
                return new BigDecimal(s).round(new MathContext(7, RoundingMode.DOWN)).doubleValue();
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

    @Before
    @After
    public void assertRequestBreakerEmptyAfterTests() throws Exception {
        assertRequestBreakerEmpty();
    }

    public static void assertRequestBreakerEmpty() throws Exception {
        assertBusy(() -> {
            HttpEntity entity = adminClient().performRequest(new Request("GET", "/_nodes/stats")).getEntity();
            Map<?, ?> stats = XContentHelper.convertToMap(XContentType.JSON.xContent(), entity.getContent(), false);
            Map<?, ?> nodes = (Map<?, ?>) stats.get("nodes");
            for (Object n : nodes.values()) {
                Map<?, ?> node = (Map<?, ?>) n;
                Map<?, ?> breakers = (Map<?, ?>) node.get("breakers");
                Map<?, ?> request = (Map<?, ?>) breakers.get("request");
                assertMap(
                    "circuit breakers not reset to 0",
                    request,
                    matchesMap().extraOk().entry("estimated_size_in_bytes", 0).entry("estimated_size", "0b")
                );
            }
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
}
