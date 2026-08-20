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
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.querylog.EsqlLogContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.logging.activity.QueryLogger.QUERY_LOGGER_ENABLED;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_INDICES;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_REMOTES;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_REMOTE_COUNT;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageSuccess;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageData;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CrossClusterLoggingIT extends AbstractCrossClusterTestCase {
    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, randomBoolean(), REMOTE_CLUSTER_2, randomBoolean());
    }

    static AccumulatingMockAppender appender;
    static Logger queryLog = LogManager.getLogger(QueryLogging.QUERY_LOGGER_NAME);
    static Level origQueryLogLevel = queryLog.getLevel();

    @BeforeClass
    public static void initAppender() throws IllegalAccessException {
        appender = new AccumulatingMockAppender("ccs_logging_appender");
        appender.start();
        Loggers.addAppender(queryLog, appender);
        Loggers.setLevel(queryLog, Level.TRACE);
    }

    @AfterClass
    public static void cleanupAppender() {
        Loggers.removeAppender(queryLog, appender);
        appender.stop();
        Loggers.setLevel(queryLog, origQueryLogLevel);
    }

    @Before
    public void enableActivityLogger() {
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Settings.builder().put(QUERY_LOGGER_ENABLED.getKey(), true))
                .get()
        );
        appender.reset();
    }

    @After
    public void disableActivityLogger() {
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Settings.builder().put(QUERY_LOGGER_ENABLED.getKey(), (String) null))
                .get()
        );
    }

    public void testLocalQueryLogging() throws IOException {
        setupClusters(2);
        try (EsqlQueryResponse resp = runQuery("from logs-* | stats sum (v)", randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
        }
        var event = appender.getLastEventAndReset();
        assertNotNull(event);
        Map<String, String> message = getMessageData(event);
        assertMessageSuccess(message, EsqlLogContext.TYPE, "from logs-*");
        assertNull(message.get(QUERY_FIELD_REMOTE_COUNT));
        assertNull(message.get(QUERY_FIELD_REMOTES));
        assertThat(message.get(QUERY_FIELD_INDICES), equalTo("logs-*"));
    }

    public void testRemoteQueryLogging() throws IOException {
        setupClusters(3);
        try (EsqlQueryResponse resp = runQuery("from logs-*,*:logs-* | stats sum (v)", randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
        }
        var event = appender.getLastEventAndReset();
        assertNotNull(event);
        Map<String, String> message = getMessageData(event);
        assertMessageSuccess(message, EsqlLogContext.TYPE, "from logs-*");
        assertThat(message.get(QUERY_FIELD_REMOTE_COUNT), equalTo("2"));
        assertThat(message.get(QUERY_FIELD_REMOTES), containsString(REMOTE_CLUSTER_1));
        assertThat(message.get(QUERY_FIELD_REMOTES), containsString(REMOTE_CLUSTER_2));
        assertThat(
            message.get(QUERY_FIELD_INDICES).split(","),
            arrayContainingInAnyOrder("logs-*", REMOTE_CLUSTER_1 + ":logs-*", REMOTE_CLUSTER_2 + ":logs-*")
        );
    }

    /**
     * Streaming remote query: the {@code elasticsearch.querylog} event for a {@code /_query/stream}
     * cross-cluster query must carry {@code remote_count}, {@code remotes}, and remote-qualified
     * {@code indices} — confirming that hardcoding {@code IncludeExecutionMetadata.NEVER} in the
     * streaming action gates only response rendering, not {@code clusterInfo} tracking.
     */
    public void testStreamingRemoteQueryLogging() throws Exception {
        setupClusters(3);
        String query = "from logs-*,*:logs-* | stats sum (v)";
        EsqlQueryRequest source = syncEsqlQueryRequest(query);
        source.pageSize(between(1, 10));

        AtomicReference<Throwable> startError = new AtomicReference<>();
        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        ActionFuture<ActionResponse.Empty> future = client(LOCAL_CLUSTER).execute(
            EsqlStreamQueryAction.INSTANCE,
            EsqlStreamQueryRequest.from(
                source,
                ActionListener.wrap(start -> start.publisher().subscribe(subscriber), startError::set),
                false
            )
        );
        future.actionGet(TimeValue.timeValueSeconds(30));

        if (startError.get() != null) {
            throw new AssertionError("stream start failed", startError.get());
        }
        subscriber.rethrowIfFailed();

        var event = appender.getLastEventAndReset();
        assertNotNull("expected a query-log event for /_query/stream CCS query", event);
        Map<String, String> message = getMessageData(event);
        assertMessageSuccess(message, EsqlLogContext.TYPE, query);
        assertThat(message.get(QUERY_FIELD_REMOTE_COUNT), equalTo("2"));
        assertThat(message.get(QUERY_FIELD_REMOTES), containsString(REMOTE_CLUSTER_1));
        assertThat(message.get(QUERY_FIELD_REMOTES), containsString(REMOTE_CLUSTER_2));
        assertThat(
            message.get(QUERY_FIELD_INDICES).split(","),
            arrayContainingInAnyOrder("logs-*", REMOTE_CLUSTER_1 + ":logs-*", REMOTE_CLUSTER_2 + ":logs-*")
        );
    }
}
