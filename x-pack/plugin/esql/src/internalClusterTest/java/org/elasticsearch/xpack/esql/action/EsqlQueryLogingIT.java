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
import org.elasticsearch.test.ActivityLoggingUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.querylog.EsqlLogProducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageFailure;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageSuccess;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageData;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class EsqlQueryLogingIT extends AbstractEsqlIntegTestCase {
    static AccumulatingMockAppender appender;
    static Logger queryLog = LogManager.getLogger(EsqlLogProducer.LOGGER_NAME);
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

    public void testLogging() throws Exception {
        int numDocs1 = randomIntBetween(1, 15);
        assertAcked(client().admin().indices().prepareCreate("index-1").setMapping("host", "type=keyword"));
        for (int i = 0; i < numDocs1; i++) {
            client().prepareIndex("index-1").setSource("host", "192." + i).get();
        }
        int numDocs2 = randomIntBetween(1, 15);
        assertAcked(client().admin().indices().prepareCreate("index-2").setMapping("host", "type=keyword"));
        for (int i = 0; i < numDocs2; i++) {
            client().prepareIndex("index-2").setSource("host", "10." + i).get();
        }

        client().admin().indices().prepareRefresh("index-1", "index-2").get();

        assertQuery("FROM index-* | EVAL ip = to_ip(host) | STATS s = COUNT(*) by ip | KEEP ip | LIMIT 100");
        assertFailedQuery(
            "FROM index-* | EVAL a = count(*) | LIMIT 100",
            "aggregate function [count(*)] not allowed outside STATS command",
            VerificationException.class
        );
    }

    private void assertQuery(String query) {
        try (var resp = run(query)) {
            var message = getMessageData(appender.getLastEventAndReset());
            assertMessageSuccess(message, "esql", query);
        }
    }

    private void assertFailedQuery(String query, String expectedMessage, Class<? extends Throwable> expectedException) {
        expectThrows(VerificationException.class, () -> run(query));
        var message = getMessageData(appender.getLastEventAndReset());
        assertMessageFailure(message, "esql", query, expectedException, expectedMessage);
    }
}
