/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ActivityLoggingUtils;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.sql.analysis.analyzer.VerificationException;
import org.elasticsearch.xpack.sql.logging.SqlLogContext;
import org.elasticsearch.xpack.sql.plugin.SqlAsyncGetResultsAction;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlVersions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.logging.activity.QueryLogging.ES_QUERY_FIELDS_PREFIX;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_RESULT_COUNT;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageFailure;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageSuccess;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageData;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class SqlLoggingIT extends AbstractSqlBlockingIntegTestCase {
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

    public void testSqlLogging() {
        prepareIndex();

        boolean dataBeforeCount = randomBoolean();
        String columns = dataBeforeCount ? "data, count" : "count, data";
        String query = "SELECT " + columns + " FROM test ORDER BY count";
        SqlQueryResponse response = new SqlQueryRequestBuilder(client()).query(query)
            .mode(Mode.JDBC)
            .version(SqlVersions.SERVER_COMPAT_VERSION.toString())
            .get();
        assertThat(response.size(), equalTo(2L));
        assertThat(response.columns(), hasSize(2));
        assertThat(appender.events.size(), equalTo(2));

        var searchMessage = getMessageData(appender.events.get(0));
        var sqlMessage = getMessageData(appender.events.get(1));
        assertMessageSuccess(sqlMessage, SqlLogContext.TYPE, query);
        assertThat(sqlMessage.get(QUERY_FIELD_RESULT_COUNT), equalTo("2"));
    }

    public void testSqlFailureLogging() {
        String query = "SELECT data, count FROM test ORDER BY count";
        expectThrows(VerificationException.class, () -> new SqlQueryRequestBuilder(client()).query(query).get());
        assertThat(appender.events.size(), equalTo(1));
        var message = getMessageData(appender.getLastEventAndReset());
        assertMessageFailure(message, SqlLogContext.TYPE, query, VerificationException.class, "Unknown index [test]");
        assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo("0"));
    }

    /**
     * When an async SQL request runs longer than the wait_for_completion_timeout, the activity log
     * is written with the correct (non-zero) result count when the query completes in the background.
     */
    public void testAsyncLogging() throws Exception {
        prepareIndex();

        String query = "SELECT data, count FROM test ORDER BY count";
        SqlQueryRequestBuilder builder = new SqlQueryRequestBuilder(client()).query(query)
            .waitForCompletionTimeout(TimeValue.timeValueMillis(1));

        List<SearchBlockPlugin> plugins = initBlockFactory(true, false);

        SqlQueryResponse initialResponse = client().execute(SqlQueryAction.INSTANCE, builder.request()).get();
        assertThat(initialResponse.isRunning(), is(true));
        assertThat(initialResponse.isPartial(), is(true));

        awaitForBlockedSearches(plugins, "test");

        GetAsyncResultRequest getResultsRequest = new GetAsyncResultRequest(initialResponse.id()).setKeepAlive(
            TimeValue.timeValueMinutes(10)
        ).setWaitForCompletionTimeout(TimeValue.THIRTY_SECONDS);
        ActionFuture<SqlQueryResponse> future = client().execute(SqlAsyncGetResultsAction.INSTANCE, getResultsRequest);
        disableBlocks(plugins);

        SqlQueryResponse response = future.get();
        assertThat(response.isRunning(), is(false));
        assertThat(response.isPartial(), is(false));
        assertThat(response.rows().size(), equalTo(2));

        Map<String, String> message = appender.events.stream()
            .map(ActivityLoggingUtils::getMessageData)
            .filter(m -> SqlLogContext.TYPE.equals(m.get(ES_QUERY_FIELDS_PREFIX + "type")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected SQL log event not found"));
        assertMessageSuccess(message, SqlLogContext.TYPE, query);
        assertThat(message.get(QUERY_FIELD_RESULT_COUNT), equalTo("2"));

        AcknowledgedResponse deleteResponse = client().execute(
            TransportDeleteAsyncResultAction.TYPE,
            new DeleteAsyncResultRequest(response.id())
        ).actionGet();
        assertThat(deleteResponse.isAcknowledged(), equalTo(true));
    }

    private void prepareIndex() {
        assertAcked(indicesAdmin().prepareCreate("test").get());
        client().prepareBulk()
            .add(new IndexRequest("test").id("1").source("data", "bar", "count", 42))
            .add(new IndexRequest("test").id("2").source("data", "baz", "count", 43))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow("test");
    }
}
