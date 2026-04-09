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
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.test.ActivityLoggingUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.querylog.EsqlLogContext;
import org.elasticsearch.xpack.esql.querylog.EsqlLogProducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_INDICES;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_QUERY;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_RESULT_COUNT;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_SHARDS;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageFailure;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageSuccess;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageData;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.action.PreparedEsqlQueryRequest.PREPARED_QUERY_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertTrue;

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

    private void assertQuery(String query, long hits) {
        try (var resp = run(query)) {
            var message = getMessageData(appender.getLastEventAndReset());
            // When the request was randomly promoted to a PreparedEsqlQueryRequest the logged
            // query will be the plan representation rather than the original query string.
            var loggedQuery = message.get(QUERY_FIELD_QUERY);
            var queryForAssert = loggedQuery != null && loggedQuery.startsWith(PREPARED_QUERY_PREFIX) ? PREPARED_QUERY_PREFIX : query;
            assertMessageSuccess(message, EsqlLogContext.TYPE, queryForAssert);
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
            assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(Long.toString(hits)));
            assertThat(message.get(QUERY_FIELD_INDICES), equalTo("index-*"));
        }
    }

    private void assertFailedQuery(String query, String expectedMessage, Class<? extends Throwable> expectedException) {
        expectThrows(VerificationException.class, () -> run(query));
        var message = getMessageData(appender.getLastEventAndReset());
        var loggedQuery = message.get(QUERY_FIELD_QUERY);
        var queryForAssert = loggedQuery != null && loggedQuery.startsWith(PREPARED_QUERY_PREFIX) ? PREPARED_QUERY_PREFIX : query;
        assertMessageFailure(message, EsqlLogContext.TYPE, queryForAssert, expectedException, expectedMessage);
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

        EsqlQueryRequest request = syncEsqlQueryRequest("FROM esql_partial_test | LIMIT 100").allowPartialResults(true);
        try (var resp = run(request)) {
            EsqlExecutionInfo.Cluster local = resp.getExecutionInfo().getCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.getFailedShards(), greaterThanOrEqualTo(1));
            assertThat(local.getSuccessfulShards(), greaterThanOrEqualTo(1));
        }
        var event = appender.getLastEventAndReset();
        assertNotNull(event);
        var message = getMessageData(event);
        var loggedQuery = message.get(QUERY_FIELD_QUERY);
        var queryForAssert = loggedQuery != null && loggedQuery.startsWith(PREPARED_QUERY_PREFIX)
            ? PREPARED_QUERY_PREFIX
            : "FROM esql_partial_test | LIMIT 100";
        assertMessageSuccess(message, EsqlLogContext.TYPE, queryForAssert);
        assertThat(Integer.valueOf(message.get(QUERY_FIELD_SHARDS + "successful")), greaterThanOrEqualTo(1));
        assertThat(Integer.valueOf(message.getOrDefault(QUERY_FIELD_SHARDS + "skipped", "0")), equalTo(0));
        assertThat(Integer.valueOf(message.get(QUERY_FIELD_SHARDS + "failed")), greaterThanOrEqualTo(1));
        assertThat(message.get(QUERY_FIELD_INDICES), equalTo("esql_partial_test"));
    }

}
