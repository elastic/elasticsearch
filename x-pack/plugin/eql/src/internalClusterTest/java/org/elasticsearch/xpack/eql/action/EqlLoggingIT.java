/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ActivityLoggingUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.eql.logging.EqlLogContext;
import org.elasticsearch.xpack.eql.plugin.EqlAsyncGetResultAction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.logging.activity.QueryLogging.ES_QUERY_FIELDS_PREFIX;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_INDICES;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_RESULT_COUNT;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_SHARDS;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageFailure;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageSuccess;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageData;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.eql.action.EqlSearchResponseIntegTestHelpers.decRef;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class EqlLoggingIT extends AbstractEqlBlockingIntegTestCase {
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
    public void restoreLog() {
        ActivityLoggingUtils.disableLoggers();
    }

    public void testEqlLogging() throws Exception {
        prepareIndex();
        boolean success = randomBoolean();
        String query = success ? "my_event where i==1" : "my_event where i==42";
        EqlSearchRequest request = new EqlSearchRequest().indices("test")
            .query(query)
            .eventCategoryField("event_type")
            .waitForCompletionTimeout(TimeValue.THIRTY_SECONDS);

        EqlSearchResponse response = client().execute(EqlSearchAction.INSTANCE, request).get();
        try {
            assertThat(response.isRunning(), is(false));
            assertThat(response.isPartial(), is(false));
            var message = getMessageData(appender.getLastEventAndReset());
            assertMessageSuccess(message, EqlLogContext.TYPE, query);
            assertThat(message.get(QUERY_FIELD_INDICES), equalTo("test"));
            assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(success ? "1" : "0"));
        } finally {
            decRef(response);
        }
    }

    public void testEqlFailureLogging() throws Exception {
        String query = "my_event where i==1";
        EqlSearchRequest request = new EqlSearchRequest().indices("test")
            .query(query)
            .eventCategoryField("event_type")
            .waitForCompletionTimeout(TimeValue.THIRTY_SECONDS);

        expectThrows(Exception.class, () -> client().execute(EqlSearchAction.INSTANCE, request).get());
        assertThat(appender.events.size(), equalTo(1));
        var message = getMessageData(appender.getLastEventAndReset());
        assertMessageFailure(message, EqlLogContext.TYPE, query, IndexNotFoundException.class, "Unknown index [test]");
        assertThat(message.get(QUERY_FIELD_INDICES), equalTo("test"));
        assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo("0"));
    }

    /**
     * When the request succeeds with partial results (some shards fail), the activity log records
     * shards.failed from EqlLogContext.shardInfo() (EQL only logs failed shard count).
     */
    public void testEqlLoggingPartialShardFailure() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        int numberOfShards = cluster().numDataNodes() + 2;
        prepareIndex(numberOfShards);
        internalCluster().stopRandomDataNode();
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForStatus(ClusterHealthStatus.RED).get();
        assertBusy(() -> {
            var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            assertFalse(
                "expected some unassigned shards",
                RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.UNASSIGNED).isEmpty()
            );
        });

        EqlSearchRequest request = new EqlSearchRequest().indices("test")
            .query("my_event where i >= 0")
            .eventCategoryField("event_type")
            .allowPartialSearchResults(true)
            .waitForCompletionTimeout(TimeValue.THIRTY_SECONDS);
        EqlSearchResponse response = client().execute(EqlSearchAction.INSTANCE, request).get();
        try {
            assertThat(response.isRunning(), is(false));
            assertThat(response.shardFailures().length, greaterThanOrEqualTo(1));

            var event = appender.getLastEventAndReset();
            assertNotNull(event);
            var message = getMessageData(event);
            assertMessageSuccess(message, "eql", "my_event where i >= 0");
            assertThat(message.get(QUERY_FIELD_INDICES), equalTo("test"));
            assertThat(Integer.valueOf(message.get(QUERY_FIELD_SHARDS + "failed")), greaterThanOrEqualTo(1));
        } finally {
            decRef(response);
        }
    }

    /**
     * When an async EQL request runs longer than the wait_for_completion_timeout, the activity log
     * is written with the correct (non-zero) result count when the query completes in the background.
     */
    public void testAsyncLogging() throws Exception {
        prepareIndex();

        String query = "my_event where i==1";
        EqlSearchRequest request = new EqlSearchRequest().indices("test")
            .query(query)
            .eventCategoryField("event_type")
            .waitForCompletionTimeout(TimeValue.timeValueMillis(1));

        List<SearchBlockPlugin> plugins = initBlockFactory(true, false);

        EqlSearchResponse initialResponse = client().execute(EqlSearchAction.INSTANCE, request).get();
        try {
            assertThat(initialResponse.isRunning(), is(true));
            assertThat(initialResponse.isPartial(), is(true));

            awaitForBlockedSearches(plugins, "test");

            GetAsyncResultRequest getResultsRequest = new GetAsyncResultRequest(initialResponse.id()).setKeepAlive(
                TimeValue.timeValueMinutes(10)
            ).setWaitForCompletionTimeout(TimeValue.THIRTY_SECONDS);
            ActionFuture<EqlSearchResponse> future = client().execute(EqlAsyncGetResultAction.INSTANCE, getResultsRequest);
            disableBlocks(plugins);

            EqlSearchResponse response = future.get();
            try {
                assertThat(response.isRunning(), is(false));
                assertThat(response.isPartial(), is(false));
                assertThat(response.hits().events().size(), equalTo(1));

                var message = appender.events.stream()
                    .map(ActivityLoggingUtils::getMessageData)
                    .filter(m -> EqlLogContext.TYPE.equals(m.get(ES_QUERY_FIELDS_PREFIX + "type")))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("expected EQL log event not found"));
                assertMessageSuccess(message, EqlLogContext.TYPE, query);
                assertThat(message.get(QUERY_FIELD_INDICES), equalTo("test"));
                assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo("1"));

                AcknowledgedResponse deleteResponse = client().execute(
                    TransportDeleteAsyncResultAction.TYPE,
                    new DeleteAsyncResultRequest(response.id())
                ).actionGet();
                assertThat(deleteResponse.isAcknowledged(), equalTo(true));
            } finally {
                decRef(response);
            }
        } finally {
            decRef(initialResponse);
        }
    }

    private void prepareIndex() throws Exception {
        prepareIndex(1);
    }

    private void prepareIndex(int numberOfShards) throws Exception {
        var createRequest = indicesAdmin().prepareCreate("test")
            .setMapping("val", "type=integer", "event_type", "type=keyword", "@timestamp", "type=date", "i", "type=integer");
        if (numberOfShards > 1) {
            createRequest.setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()
            );
        }
        assertAcked(createRequest);

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            int fieldValue = randomIntBetween(0, 10);
            builders.add(
                prepareIndex("test").setSource(
                    jsonBuilder().startObject()
                        .field("val", fieldValue)
                        .field("event_type", "my_event")
                        .field("@timestamp", "2020-04-09T12:35:48Z")
                        .field("i", i)
                        .endObject()
                )
            );
        }
        indexRandom(true, builders);
    }
}
