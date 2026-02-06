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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ActivityLoggingUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.VerificationException;
import org.elasticsearch.xpack.sql.logging.SqlLogProducer;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlVersions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.elasticsearch.common.logging.activity.ActivityLogProducer.ES_FIELDS_PREFIX;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageFailure;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageSuccess;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageData;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SqlLoggingIT extends AbstractSqlIntegTestCase {
    static AccumulatingMockAppender appender;
    static Logger queryLog = LogManager.getLogger(SqlLogProducer.LOGGER_NAME);
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
        assertAcked(indicesAdmin().prepareCreate("test").get());
        client().prepareBulk()
            .add(new IndexRequest("test").id("1").source("data", "bar", "count", 42))
            .add(new IndexRequest("test").id("2").source("data", "baz", "count", 43))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow("test");

        boolean dataBeforeCount = randomBoolean();
        String columns = dataBeforeCount ? "data, count" : "count, data";
        String query = "SELECT " + columns + " FROM test ORDER BY count";
        SqlQueryResponse response = new SqlQueryRequestBuilder(client()).query(query)
            .mode(Mode.JDBC)
            .version(SqlVersions.SERVER_COMPAT_VERSION.toString())
            .get();
        assertThat(response.size(), equalTo(2L));
        assertThat(response.columns(), hasSize(2));
        assertThat(appender.events.size(), equalTo(1));
        var message = getMessageData(appender.getLastEventAndReset());
        assertMessageSuccess(message, "sql", query);
        assertThat(message.get(ES_FIELDS_PREFIX + "rows"), equalTo("2"));
    }

    public void testSqlFailureLogging() {
        String query = "SELECT data, count FROM test ORDER BY count";
        expectThrows(VerificationException.class, () -> new SqlQueryRequestBuilder(client()).query(query).get());
        assertThat(appender.events.size(), equalTo(1));
        var message = getMessageData(appender.getLastEventAndReset());
        assertMessageFailure(message, "sql", query, VerificationException.class, "Unknown index [test]");
        assertThat(message.get(ES_FIELDS_PREFIX + "rows"), equalTo("0"));
    }
}
