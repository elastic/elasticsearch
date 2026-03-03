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
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ActivityLoggingUtils;
import org.elasticsearch.xpack.eql.logging.EqlLogProducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_INDICES;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_RESULT_COUNT;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_SHARDS;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageFailure;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageSuccess;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageData;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EqlLoggingIT extends AbstractEqlIntegTestCase {
    static AccumulatingMockAppender appender;
    static Logger queryLog = LogManager.getLogger(EqlLogProducer.QUERY_LOGGER_NAME);
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
        assertThat(response.isRunning(), is(false));
        assertThat(response.isPartial(), is(false));
        var message = getMessageData(appender.getLastEventAndReset());
        assertMessageSuccess(message, "eql", query);
        assertThat(message.get(QUERY_FIELD_INDICES), equalTo("test"));
        assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo(success ? "1" : "0"));
        // EQL only logs shards.failed
        assertThat(message.get(QUERY_FIELD_SHARDS + "failed"), equalTo("0"));
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
        assertMessageFailure(message, "eql", query, IndexNotFoundException.class, "Unknown index [test]");
        assertThat(message.get(QUERY_FIELD_INDICES), equalTo("test"));
        assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo("0"));
    }

    /**
     * When the request succeeds with partial results (some shards fail), the activity log records
     * shards.failed from EqlLogContext.shardInfo() (EQL only logs failed shard count).
     */
    public void testEqlLoggingPartialShardFailure() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareIndex(2);

        internalCluster().stopRandomDataNode();

        EqlSearchRequest request = new EqlSearchRequest().indices("test")
            .query("my_event where i >= 0")
            .eventCategoryField("event_type")
            .allowPartialSearchResults(true)
            .waitForCompletionTimeout(TimeValue.THIRTY_SECONDS);
        EqlSearchResponse response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.isRunning(), is(false));
        assertThat(response.shardFailures().length, equalTo(1));

        var event = appender.getLastEventAndReset();
        assertNotNull(event);
        var message = getMessageData(event);
        assertMessageSuccess(message, "eql", "my_event where i >= 0");
        assertThat(message.get(QUERY_FIELD_INDICES), equalTo("test"));
        assertThat(message.get(QUERY_FIELD_SHARDS + "failed"), equalTo("1"));
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
