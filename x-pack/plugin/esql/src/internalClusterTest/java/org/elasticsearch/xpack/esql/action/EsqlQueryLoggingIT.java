/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.activity.QueryCaptureContext;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ActivityLoggingUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.querylog.EsqlCaptureContext;
import org.elasticsearch.xpack.esql.querylog.EsqlLogProducer;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_FILTER;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_INDICES;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_PARAMS;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_QUERY;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_RESULT_COUNT;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_SHARDS;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageFailure;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageSuccess;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageData;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageField;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class EsqlQueryLoggingIT extends AbstractEsqlIntegTestCase {
    private static final String SET_SETTINGS = "SET time_zone = \"Europe/Paris\"; SET unmapped_fields = \"NULLIFY\"; ";
    static AccumulatingMockAppender appender;
    static Logger queryLog = LogManager.getLogger(QueryLogging.QUERY_LOGGER_NAME);
    static Level origQueryLogLevel = queryLog.getLevel();

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new AccumulatingMockAppender("trace_appender");
        appender.start();
        Loggers.addAppender(queryLog, appender);

        Loggers.setLevel(queryLog, Level.TRACE);
    }

    @AfterClass
    public static void cleanup() {
        Loggers.removeAppender(queryLog, appender);
        appender.stop();
        Loggers.setLevel(queryLog, origQueryLogLevel);
    }

    @Before
    public void enableLog() {
        ActivityLoggingUtils.enableLoggers();
        appender.reset();
    }

    @After
    public void disableLog() {
        ActivityLoggingUtils.disableLoggers();
    }

    public void testLogging() throws Exception {
        int numDocs1 = setupIndex("index-1", "192.168.0.1");
        int numDocs2 = setupIndex("index-2", "10.0.0.1");

        assertQuery("FROM index-* | LIMIT 0", 0);
        assertQuery("FROM index-* | EVAL ip = to_ip(host) | STATS s = COUNT(*) by ip | KEEP ip | LIMIT 100", 2);
        assertQuery(SET_SETTINGS + "FROM index-* | LIMIT 100", numDocs1 + numDocs2);
        assertFailedQuery(
            SET_SETTINGS + "FROM index-* | EVAL a = count(*) | LIMIT 100",
            "aggregate function [count(*)] not allowed outside STATS command",
            VerificationException.class
        );
    }

    public void testAnalyzeOnlyIsNotLogged() throws Exception {
        setupIndex("index-analyze-only", "192.168.0.1");
        var parser = new EsqlParser(new EsqlConfig(new EsqlFunctionRegistry()));
        var inferenceSettings = new InferenceSettings(clusterService().state().metadata().settings());

        String query = "FROM index-analyze-only | LIMIT 1";
        try (var ignored = run(PreparedEsqlQueryRequest.analyzeOnly(parser.parse(query, new QueryParams(), inferenceSettings), query))) {
            assertNull(appender.getLastEventAndReset());
        }
        String failing = "FROM index-analyze-only | KEEP missing";
        expectThrows(
            VerificationException.class,
            () -> run(PreparedEsqlQueryRequest.analyzeOnly(parser.parse(failing, new QueryParams(), inferenceSettings), failing))
        );
        assertNull(appender.getLastEventAndReset());
    }

    public void testLoggingParseFailure() {
        String query = SET_SETTINGS + "FROM";
        expectThrows(ParsingException.class, () -> run(query));
        var message = getMessageData(appender.getLastEventAndReset());
        assertMessageFailure(message, EsqlCaptureContext.TYPE, query, ParsingException.class, null);
        assertThat(message.keySet().stream().filter(k -> k.startsWith(EsqlLogProducer.SETTINGS_PREFIX)).toList(), empty());
    }

    public void testLoggingFilter() throws Exception {
        int numDocs = setupIndex("index-filter", "192.168.0.1");
        QueryBuilder filter = new TermQueryBuilder("host", "192.168.0.1");
        String query = SET_SETTINGS + "FROM index-filter | LIMIT 100";
        EsqlQueryRequest request = syncEsqlQueryRequest(query).filter(filter);
        assertQuery(request, query, numDocs, filter, "index-filter");
    }

    @SuppressWarnings("unchecked")
    public void testLoggingPositionalParams() throws Exception {
        int numDocs = setupIndex("index-1", "192.168.0.1");
        String query = SET_SETTINGS + "FROM index-1 | WHERE host == ? AND value < ? AND value != ? | LIMIT 100";
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.params(
            new QueryParams(List.of(paramAsConstant(null, "192.168.0.1"), paramAsConstant(null, 1000), paramAsConstant(null, 1000)))
        );
        try (var resp = run(request)) {
            var event = appender.getLastEventAndReset();
            var message = getMessageData(event);
            assertMessageSuccess(message, EsqlCaptureContext.TYPE, query);
            assertSettingsLogged(message, query);
            assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(Long.toString(numDocs)));
            var params = (Map<String, Object>) getMessageField(event, QUERY_FIELD_PARAMS);
            assertNotNull(params);
            assertThat(params, hasKey(QueryLogging.QUERY_FIELD_PARAM_POSITIONAL));
            var posParams = (List<Object>) params.get(QueryLogging.QUERY_FIELD_PARAM_POSITIONAL);
            assertThat(posParams, hasSize(3));
            assertThat(posParams.get(0), equalTo("192.168.0.1"));
            assertThat(posParams.get(1), equalTo("1000"));
            assertThat(posParams.get(2), equalTo("1000"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testLoggingNamedParams() {
        int numDocs = setupIndex("index-1", "192.168.0.1");
        String query = SET_SETTINGS + "FROM index-1 | WHERE host == ?host_name AND value < ?max_value | LIMIT 100";
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.params(new QueryParams(List.of(paramAsConstant("host_name", "192.168.0.1"), paramAsConstant("max_value", 1000))));
        try (var resp = run(request)) {
            var event = appender.getLastEventAndReset();
            var message = getMessageData(event);
            assertMessageSuccess(message, EsqlCaptureContext.TYPE, query);
            assertSettingsLogged(message, query);
            assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(Long.toString(numDocs)));
            var params = (Map<String, Object>) getMessageField(event, QUERY_FIELD_PARAMS);
            assertNotNull(params);
            assertThat(params.get("host_name"), equalTo("192.168.0.1"));
            assertThat(params.get("max_value"), equalTo("1000"));
        }
    }

    /**
     * Each resolved view's own settings are logged under one field keyed by view name, apart from the caller's, which
     * stay what the caller set. Every resolved view is logged, with the default for each setting its definition does not
     * SET, and a query that resolves no view has no such field.
     */
    @SuppressWarnings("unchecked")
    public void testLoggingViewSettings() throws Exception {
        int numDocs = setupIndex("logged-view-source", "192.168.0.1");
        createView("nullifying_view", "SET unmapped_fields=\"nullify\"; SET wildcards_match_views=true; FROM logged-view-source");
        createView("plain_view", "FROM logged-view-source");
        try {
            String query = SET_SETTINGS + "FROM nullifying_view, plain_view | LIMIT 100";
            try (var resp = run(query)) {
                var event = appender.getLastEventAndReset();
                var message = getMessageData(event);
                assertMessageSuccess(message, EsqlCaptureContext.TYPE, query);
                assertSettingsLogged(message, query);
                assertThat(message.get(EsqlLogProducer.SETTINGS_PREFIX + "wildcards_match_views"), equalTo("false"));
                assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(Long.toString(2L * numDocs)));
                var viewSettings = (Map<String, Object>) getMessageField(event, EsqlLogProducer.VIEW_SETTINGS_FIELD);
                assertThat(
                    viewSettings,
                    equalTo(
                        Map.of(
                            "nullifying_view",
                            Map.of("unmapped_fields", "NULLIFY", "wildcards_match_views", "true", "wildcards_match_datasets", "false"),
                            "plain_view",
                            PLAIN_VIEW_SETTINGS
                        )
                    )
                );
            }

            try (var resp = run("FROM plain_view | LIMIT 100")) {
                var event = appender.getLastEventAndReset();
                var viewSettings = (Map<String, Object>) getMessageField(event, EsqlLogProducer.VIEW_SETTINGS_FIELD);
                assertThat(viewSettings, equalTo(Map.of("plain_view", PLAIN_VIEW_SETTINGS)));
            }

            try (var resp = run("FROM logged-view-source | LIMIT 100")) {
                var event = appender.getLastEventAndReset();
                assertNull(getMessageField(event, EsqlLogProducer.VIEW_SETTINGS_FIELD));
            }
        } finally {
            client().execute(
                DeleteViewAction.INSTANCE,
                new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { "nullifying_view", "plain_view" })
            ).actionGet(30, TimeUnit.SECONDS);
        }
    }

    private static final Map<String, String> PLAIN_VIEW_SETTINGS = Map.of(
        "unmapped_fields",
        "DEFAULT",
        "wildcards_match_views",
        "false",
        "wildcards_match_datasets",
        "false"
    );

    private void createView(String viewName, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(viewName, query, null, false))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }

    private static void assertSettingsLogged(Map<String, String> message, String query) {
        boolean set = query.startsWith(SET_SETTINGS);
        assertThat(message.get(EsqlLogProducer.SETTINGS_PREFIX + "time_zone"), equalTo(set ? "Europe/Paris" : "Z"));
        assertThat(message.get(EsqlLogProducer.SETTINGS_PREFIX + "unmapped_fields"), equalTo(set ? "NULLIFY" : "DEFAULT"));
    }

    private void assertQuery(String query, long hits) {
        assertQuery(syncEsqlQueryRequest(query), query, hits, null, "index-*");
    }

    private void assertQuery(EsqlQueryRequest request, String query, long hits, QueryBuilder filter, String expectedIndices) {
        try (var resp = run(request)) {
            var message = getMessageData(appender.getLastEventAndReset());
            // When the request was randomly promoted to a PreparedEsqlQueryRequest the logged
            // query will be the plan representation rather than the original query string.
            var loggedQuery = message.get(QUERY_FIELD_QUERY);
            assertMessageSuccess(message, EsqlCaptureContext.TYPE, query);
            assertSettingsLogged(message, query);
            if (hits > 0) {
                // Zero hits may mean no shards were used, so we can't assert that
                assertThat(Integer.valueOf(message.get(QUERY_FIELD_SHARDS + "successful")), greaterThanOrEqualTo(1));
            }
            assertThat(Integer.valueOf(message.getOrDefault(QUERY_FIELD_SHARDS + "skipped", "0")), greaterThanOrEqualTo(0));
            assertThat(message.getOrDefault(QUERY_FIELD_SHARDS + "failed", "0"), equalTo("0"));

            // Create empty EsqlQueryProfile just to get the markers
            EsqlQueryProfile profile = new EsqlQueryProfile();

            for (var marker : profile.timeSpanMarkers()) {
                String tookKey = EsqlLogProducer.PROFILE_PREFIX + marker.name() + ".took";
                assertTrue("Expected profile field present: " + tookKey, message.containsKey(tookKey));
            }
            // Query-level rollup counters surfaced from the response root. Present on every success
            // path; for Lucene-only queries (this IT) the external-source-specific counters
            // (rows_emitted / bytes_read / read_nanos) are zero because no operator overrode the
            // corresponding Operator.Status defaults.
            for (String key : new String[] {
                "documents_found",
                "values_loaded",
                "rows_emitted",
                "bytes_read",
                "read_nanos",
                "read_cpu_nanos",
                "cpu_nanos",
                "driver_nanos" }) {
                String fullKey = EsqlLogProducer.PROFILE_PREFIX + key;
                assertTrue("Expected rollup field present: " + fullKey, message.containsKey(fullKey));
            }
            assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(Long.toString(hits)));
            if (filter != null) {
                assertThat(message.get(QUERY_FIELD_FILTER), equalTo(QueryCaptureContext.filterToLogString(filter).get()));
            } else {
                assertNull(message.get(QUERY_FIELD_FILTER));
            }
            assertThat(message.get(QUERY_FIELD_INDICES), equalTo(expectedIndices));
        }
    }

    private void assertFailedQuery(String query, String expectedMessage, Class<? extends Throwable> expectedException) {
        expectThrows(VerificationException.class, () -> run(query));
        var message = getMessageData(appender.getLastEventAndReset());
        assertMessageFailure(message, EsqlCaptureContext.TYPE, query, expectedException, expectedMessage);
        assertSettingsLogged(message, query);
    }

    private int setupIndex(String name, String prefix) {
        int numDocs = randomIntBetween(1, 15);
        int numShards = internalCluster().numDataNodes() + 2;
        assertAcked(
            client().admin().indices().prepareCreate(name).setMapping("host", "type=keyword").setSettings(indexSettings(numShards, 0))
        );
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(name).setSource("host", prefix, "value", i).get();
        }
        client().admin().indices().prepareRefresh(name).get();
        ensureGreen(name);
        return numDocs;
    }

    /**
     * When the request succeeds with partial results (some shards fail), the activity log records
     * shards.successful and shards.failed from EsqlCaptureContext.shardInfo().
     */
    public void testLoggingPartialShardFailure() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        setupIndex("esql_partial_test", "1.1.1.1");
        internalCluster().stopRandomDataNode();
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForStatus(ClusterHealthStatus.RED).get();
        assertBusy(() -> {
            var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            assertFalse(
                "expected some unassigned shards",
                RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.UNASSIGNED).isEmpty()
            );
        });

        var query = SET_SETTINGS + "FROM esql_partial_test | LIMIT 100";
        EsqlQueryRequest request = syncEsqlQueryRequest(query).allowPartialResults(true);
        try (var resp = run(request)) {
            EsqlExecutionInfo.Cluster local = resp.getExecutionInfo().getCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.getFailedShards(), greaterThanOrEqualTo(1));
            assertThat(local.getSuccessfulShards(), greaterThanOrEqualTo(1));
        }
        var event = appender.getLastEventAndReset();
        assertNotNull(event);
        var message = getMessageData(event);
        assertMessageSuccess(message, EsqlCaptureContext.TYPE, query);
        assertSettingsLogged(message, query);
        assertThat(Integer.valueOf(message.get(QUERY_FIELD_SHARDS + "successful")), greaterThanOrEqualTo(1));
        assertThat(Integer.valueOf(message.getOrDefault(QUERY_FIELD_SHARDS + "skipped", "0")), equalTo(0));
        assertThat(Integer.valueOf(message.get(QUERY_FIELD_SHARDS + "failed")), greaterThanOrEqualTo(1));
        assertThat(message.get(QUERY_FIELD_INDICES), equalTo("esql_partial_test"));
    }

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client().execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testStreamingLogging() throws Exception {
        int numDocs1 = setupIndex("stream-index-1", "192.168.0.1");
        int numDocs2 = setupIndex("stream-index-2", "10.0.0.1");

        assertStreamQuery("FROM stream-index-* | LIMIT 0", 0);
        assertStreamQuery(SET_SETTINGS + "FROM stream-index-* | EVAL ip = to_ip(host) | STATS s = COUNT(*) by ip | KEEP ip | LIMIT 100", 2);
        assertStreamQuery(SET_SETTINGS + "FROM stream-index-* | LIMIT 100", numDocs1 + numDocs2);
    }

    public void testStreamingLoggingFilter() throws Exception {
        int numDocs = setupIndex("stream-filter-index", "192.168.0.1");
        QueryBuilder filter = new TermQueryBuilder("host", "192.168.0.1");
        EsqlQueryRequest source = syncEsqlQueryRequest("FROM stream-filter-index | LIMIT 100").filter(filter);
        assertStreamQuery(source, "FROM stream-filter-index | LIMIT 100", numDocs, filter, "stream-filter-index");
    }

    @SuppressWarnings("unchecked")
    public void testStreamingLoggingPositionalParams() throws Exception {
        int numDocs = setupIndex("stream-params-index", "192.168.0.1");
        String query = "FROM stream-params-index | WHERE host == ? AND value < ? AND value != ? | LIMIT 100";
        EsqlQueryRequest source = syncEsqlQueryRequest(query);
        source.params(
            new QueryParams(List.of(paramAsConstant(null, "192.168.0.1"), paramAsConstant(null, 1000), paramAsConstant(null, 1000)))
        );
        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        StreamQueryTestUtils.executeStreamRequest(client(), source, subscriber);

        var event = appender.getLastEventAndReset();
        var message = getMessageData(event);
        assertMessageSuccess(message, EsqlCaptureContext.TYPE, query);
        assertSettingsLogged(message, query);
        assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(Long.toString(numDocs)));
        var params = (Map<String, Object>) getMessageField(event, QUERY_FIELD_PARAMS);
        assertNotNull(params);
        assertThat(params, hasKey(QueryLogging.QUERY_FIELD_PARAM_POSITIONAL));
        var posParams = (List<Object>) params.get(QueryLogging.QUERY_FIELD_PARAM_POSITIONAL);
        assertThat(posParams, hasSize(3));
        assertThat(posParams.get(0), equalTo("192.168.0.1"));
        assertThat(posParams.get(1), equalTo("1000"));
        assertThat(posParams.get(2), equalTo("1000"));
    }

    @SuppressWarnings("unchecked")
    public void testStreamingLoggingNamedParams() throws Exception {
        int numDocs = setupIndex("stream-named-index", "192.168.0.1");
        String query = "FROM stream-named-index | WHERE host == ?host_name AND value < ?max_value | LIMIT 100";
        EsqlQueryRequest source = syncEsqlQueryRequest(query);
        source.params(new QueryParams(List.of(paramAsConstant("host_name", "192.168.0.1"), paramAsConstant("max_value", 1000))));

        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        StreamQueryTestUtils.executeStreamRequest(client(), source, subscriber);

        var event = appender.getLastEventAndReset();
        var message = getMessageData(event);
        assertMessageSuccess(message, EsqlCaptureContext.TYPE, query);
        assertSettingsLogged(message, query);
        assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(Long.toString(numDocs)));
        var params = (Map<String, Object>) getMessageField(event, QUERY_FIELD_PARAMS);
        assertNotNull(params);
        assertThat(params.get("host_name"), equalTo("192.168.0.1"));
        assertThat(params.get("max_value"), equalTo("1000"));
    }

    public void testStreamingLoggingFailure() throws Exception {
        setupIndex("stream-fail-index", "192.168.0.1");
        String query = SET_SETTINGS + "FROM stream-fail-index | EVAL a = count(*) | LIMIT 100";

        EsqlQueryRequest source = syncEsqlQueryRequest(query);
        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        expectThrows(Exception.class, () -> StreamQueryTestUtils.executeStreamRequest(client(), source, subscriber));

        var event = appender.getLastEventAndReset();
        assertNotNull("expected a query-log failure event", event);
        var message = getMessageData(event);
        assertMessageFailure(message, EsqlCaptureContext.TYPE, query, VerificationException.class, "count(*)");
        assertSettingsLogged(message, query);
    }

    private void assertStreamQuery(String query, long expectedHits) throws Exception {
        assertStreamQuery(syncEsqlQueryRequest(query), query, expectedHits, null, null);
    }

    private void assertStreamQuery(EsqlQueryRequest source, String query, long expectedHits, QueryBuilder filter, String expectedIndices)
        throws Exception {
        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        StreamQueryTestUtils.executeStreamRequest(client(), source, subscriber);

        var event = appender.getLastEventAndReset();
        assertNotNull("expected a query-log event for the streaming query", event);
        var message = getMessageData(event);
        assertMessageSuccess(message, EsqlCaptureContext.TYPE, query);
        assertSettingsLogged(message, query);
        assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(Long.toString(expectedHits)));
        if (expectedHits > 0) {
            assertThat(subscriber.rowCount.get(), greaterThan(0));
            assertThat(Long.parseLong(message.get(QUERY_FIELD_RESULT_COUNT)), equalTo((long) subscriber.rowCount.get()));
        }
        if (expectedIndices != null) {
            assertThat(message.get(QUERY_FIELD_INDICES), equalTo(expectedIndices));
        } else {
            assertNotNull(message.get(QUERY_FIELD_INDICES));
        }
        if (filter != null) {
            assertThat(message.get(QUERY_FIELD_FILTER), equalTo(QueryCaptureContext.filterToLogString(filter).get()));
        } else {
            assertNull(message.get(QUERY_FIELD_FILTER));
        }

        for (String key : new String[] {
            "documents_found",
            "values_loaded",
            "rows_emitted",
            "bytes_read",
            "read_nanos",
            "cpu_nanos",
            "driver_nanos" }) {
            String fullKey = EsqlLogProducer.PROFILE_PREFIX + key;
            assertTrue("Expected rollup field present: " + fullKey, message.containsKey(fullKey));
        }
    }
}
