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
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.activity.QueryLoggerContext;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ActivityLoggingUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.querylog.EsqlLogContext;
import org.elasticsearch.xpack.esql.querylog.EsqlLogProducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;

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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class EsqlQueryLoggingIT extends AbstractEsqlIntegTestCase {
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
        assertQuery("FROM index-* | LIMIT 100", numDocs1 + numDocs2);
        assertFailedQuery(
            "FROM index-* | EVAL a = count(*) | LIMIT 100",
            "aggregate function [count(*)] not allowed outside STATS command",
            VerificationException.class
        );
    }

    public void testLoggingFilter() throws Exception {
        int numDocs = setupIndex("index-filter", "192.168.0.1");
        QueryBuilder filter = new TermQueryBuilder("host", "192.168.0.1");
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM index-filter | LIMIT 100").filter(filter);
        assertQuery(request, "FROM index-filter | LIMIT 100", numDocs, filter, "index-filter");
    }

    @SuppressWarnings("unchecked")
    public void testLoggingPositionalParams() throws Exception {
        int numDocs = setupIndex("index-1", "192.168.0.1");
        String query = "FROM index-1 | WHERE host == ? AND value < ? AND value != ? | LIMIT 100";
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.params(
            new QueryParams(List.of(paramAsConstant(null, "192.168.0.1"), paramAsConstant(null, 1000), paramAsConstant(null, 1000)))
        );
        try (var resp = run(request)) {
            var event = appender.getLastEventAndReset();
            var message = getMessageData(event);
            assertMessageSuccess(message, EsqlLogContext.TYPE, query);
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
        String query = "FROM index-1 | WHERE host == ?host_name AND value < ?max_value | LIMIT 100";
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.params(new QueryParams(List.of(paramAsConstant("host_name", "192.168.0.1"), paramAsConstant("max_value", 1000))));
        try (var resp = run(request)) {
            var event = appender.getLastEventAndReset();
            var message = getMessageData(event);
            assertMessageSuccess(message, EsqlLogContext.TYPE, query);
            assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(Long.toString(numDocs)));
            var params = (Map<String, Object>) getMessageField(event, QUERY_FIELD_PARAMS);
            assertNotNull(params);
            assertThat(params.get("host_name"), equalTo("192.168.0.1"));
            assertThat(params.get("max_value"), equalTo("1000"));
        }
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
            assertMessageSuccess(message, EsqlLogContext.TYPE, query);
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
                "cpu_nanos" }) {
                String fullKey = EsqlLogProducer.PROFILE_PREFIX + key;
                assertTrue("Expected rollup field present: " + fullKey, message.containsKey(fullKey));
            }
            assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(Long.toString(hits)));
            if (filter != null) {
                assertThat(message.get(QUERY_FIELD_FILTER), equalTo(QueryLoggerContext.filterToLogString(filter).get()));
            } else {
                assertNull(message.get(QUERY_FIELD_FILTER));
            }
            assertThat(message.get(QUERY_FIELD_INDICES), equalTo(expectedIndices));
        }
    }

    private void assertFailedQuery(String query, String expectedMessage, Class<? extends Throwable> expectedException) {
        expectThrows(VerificationException.class, () -> run(query));
        var message = getMessageData(appender.getLastEventAndReset());
        assertMessageFailure(message, EsqlLogContext.TYPE, query, expectedException, expectedMessage);
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
     * shards.successful and shards.failed from EsqlLogContext.shardInfo().
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

        var query = "FROM esql_partial_test | LIMIT 100";
        EsqlQueryRequest request = syncEsqlQueryRequest(query).allowPartialResults(true);
        try (var resp = run(request)) {
            EsqlExecutionInfo.Cluster local = resp.getExecutionInfo().getCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.getFailedShards(), greaterThanOrEqualTo(1));
            assertThat(local.getSuccessfulShards(), greaterThanOrEqualTo(1));
        }
        var event = appender.getLastEventAndReset();
        assertNotNull(event);
        var message = getMessageData(event);
        assertMessageSuccess(message, EsqlLogContext.TYPE, query);
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

}
