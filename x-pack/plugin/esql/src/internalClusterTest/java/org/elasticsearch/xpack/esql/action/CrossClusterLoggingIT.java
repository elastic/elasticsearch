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
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.querylog.EsqlLogContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.logging.activity.ActivityLogger.ACTIVITY_LOGGER_ENABLED;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_IS_CCS;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_REMOTE_COUNT;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageSuccess;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageData;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
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
                .setPersistentSettings(Settings.builder().put(ACTIVITY_LOGGER_ENABLED.getKey(), true))
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
                .setPersistentSettings(Settings.builder().put(ACTIVITY_LOGGER_ENABLED.getKey(), (String) null))
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
        assertNull(message.get(QUERY_FIELD_IS_CCS));
        assertNull(message.get(QUERY_FIELD_REMOTE_COUNT));
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
        assertThat(message.get(QUERY_FIELD_IS_CCS), equalTo("true"));
        assertThat(message.get(QUERY_FIELD_REMOTE_COUNT), equalTo("2"));
    }
}
