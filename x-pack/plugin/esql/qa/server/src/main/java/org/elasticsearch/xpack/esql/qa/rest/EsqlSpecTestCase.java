/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.RequestObjectBuilder;
import org.elasticsearch.xpack.ql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.ql.SpecReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.CsvAssert.assertData;
import static org.elasticsearch.xpack.esql.CsvAssert.assertMetadata;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.loadDataSetIntoEs;
import static org.elasticsearch.xpack.ql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.ql.TestUtils.classpathResources;

public abstract class EsqlSpecTestCase extends ESRestTestCase {

    private static final Logger LOGGER = LogManager.getLogger(EsqlSpecTestCase.class);
    private final String fileName;
    private final String groupName;
    private final String testName;
    private final Integer lineNumber;
    protected final CsvTestCase testCase;
    protected final Mode mode;

    public enum Mode {
        SYNC,
        ASYNC
    }

    @ParametersFactory(argumentFormatting = "%2$s.%3$s")
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

    protected EsqlSpecTestCase(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase, Mode mode) {
        this.fileName = fileName;
        this.groupName = groupName;
        this.testName = testName;
        this.lineNumber = lineNumber;
        this.testCase = testCase;
        this.mode = mode;
    }

    protected boolean supportsAsync() {
        return Version.CURRENT.onOrAfter(Version.V_8_13_0); // the Async API was introduced in 8.13.0
    }

    @AfterClass
    // TODO: This should become of a classrule for consistency.
    public static void wipeTestData() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "/*"));
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
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

    protected void shouldSkipTest(String testName) {
        assumeTrue("Test " + testName + " is not enabled", isEnabled(testName, Version.CURRENT));
    }

    protected final void doTest() throws Throwable {
        RequestObjectBuilder builder = new RequestObjectBuilder(randomFrom(XContentType.values()));
        Map<String, Object> answer = runEsql(builder.query(testCase.query), testCase.expectedWarnings(false));
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

    private Map<String, Object> runEsql(RequestObjectBuilder requestObject, List<String> expectedWarnings) throws IOException {
        if (mode == Mode.ASYNC) {
            assert supportsAsync();
            return RestEsqlTestCase.runEsqlAsync(requestObject, expectedWarnings);
        } else {
            return RestEsqlTestCase.runEsqlSync(requestObject, expectedWarnings);
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
        assertData(expected, actualValues, testCase.ignoreOrder, logger, EsqlSpecTestCase::valueMapper);
    }

    private static Object valueMapper(CsvTestUtils.Type type, Object value) {
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
        return value.toString();
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
                assertMap(request, matchesMap().extraOk().entry("estimated_size_in_bytes", 0).entry("estimated_size", "0b"));
            }
        });
    }

    public static class ClusterWithCsvData implements TestRule {
        private static final long CLIENT_TIMEOUT = 40L;

        ElasticsearchCluster cluster;
        Settings clientAuthSettings;
        RestClient client;

        public ClusterWithCsvData(ElasticsearchCluster cluster, Settings clientAuthSettings) {
            this.cluster = cluster;
            this.clientAuthSettings = clientAuthSettings;
        }

        public ElasticsearchCluster cluster() {
            return cluster;
        }

        @Override
        public Statement apply(Statement base, Description description) {
            return cluster.apply(startClient(loadCsvData(base)), description);
        }

        private Statement startClient(Statement base) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    try {
                        client = doStartClient();
                        base.evaluate();
                    } finally {
                        IOUtils.close(client);
                    }
                }
            };
        }

        private Statement loadCsvData(Statement base) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    loadDataSetIntoEs(client);
                    base.evaluate();
                }
            };
        }

        private RestClient doStartClient() throws IOException {
            String address = cluster.getHttpAddress(0);
            int portSeparator = address.lastIndexOf(':');
            if (portSeparator < 0) {
                throw new IllegalArgumentException("Illegal cluster url [" + address + "]");
            }
            String host = address.substring(0, portSeparator);
            int port = Integer.parseInt(address.substring(portSeparator + 1));
            HttpHost[] remoteHttpHosts = new HttpHost[] { new HttpHost(host, port) };

            return clientBuilder(clientAuthSettings, remoteHttpHosts);
        }

        protected static TimeValue timeout() {
            return TimeValue.timeValueSeconds(CLIENT_TIMEOUT);
        }

        private static RestClient clientBuilder(Settings settings, HttpHost[] hosts) throws IOException {
            RestClientBuilder builder = RestClient.builder(hosts);
            doConfigureClient(builder, settings);

            int timeout = Math.toIntExact(timeout().millis());
            builder.setRequestConfigCallback(
                requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(timeout)
                    .setConnectionRequestTimeout(timeout)
                    .setSocketTimeout(timeout)
            );
            builder.setStrictDeprecationMode(true);
            return builder.build();
        }
    }
}
