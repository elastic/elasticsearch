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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.MockAppender;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.querylog.EsqlQueryLog;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_ERROR_MESSAGE;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_ERROR_TYPE;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_PLANNING_TOOK;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_PLANNING_TOOK_MILLIS;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_QUERY;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_SUCCESS;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_TOOK;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_TOOK_MILLIS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class EsqlQueryLogIT extends AbstractEsqlIntegTestCase {
    static MockAppender appender;
    static Logger queryLog = LogManager.getLogger(EsqlQueryLog.LOGGER_NAME);
    static Level origQueryLogLevel = queryLog.getLevel();

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("trace_appender");
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

    public void testSetLevel() throws Exception {
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

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        client().admin().indices().prepareRefresh("index-1", "index-2").get();

        Map<Level, String> levels = Map.of(
            Level.WARN,
            EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_WARN_SETTING.getKey(),
            Level.INFO,
            EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_INFO_SETTING.getKey(),
            Level.DEBUG,
            EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING.getKey(),
            Level.TRACE,
            EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING.getKey()
        );
        testAllLevels(
            levels,
            coordinator,
            0,
            "FROM index-* | EVAL ip = to_ip(host) | STATS s = COUNT(*) by ip | KEEP ip | LIMIT 100",
            null,
            null
        );
        for (int i = 0; i < 10; i++) {
            testAllLevels(
                levels,
                coordinator,
                randomIntBetween(0, 500),
                "FROM index-* | EVAL ip = to_ip(host) | STATS s = COUNT(*) by ip | KEEP ip | LIMIT 100",
                null,
                null
            );
        }
        testAllLevels(
            levels,
            coordinator,
            600_000,
            "FROM index-* | EVAL ip = to_ip(host) | STATS s = COUNT(*) by ip | KEEP ip | LIMIT 100",
            null,
            null
        );

        testAllLevels(
            levels,
            coordinator,
            0,
            "FROM index-* | EVAL a = count(*) | LIMIT 100",
            "aggregate function [count(*)] not allowed outside STATS command",
            VerificationException.class.getName()
        );
        for (int i = 0; i < 10; i++) {
            testAllLevels(
                levels,
                coordinator,
                randomIntBetween(0, 500),
                "FROM index-* | EVAL a = count(*) | LIMIT 100",
                "aggregate function [count(*)] not allowed outside STATS command",
                VerificationException.class.getName()
            );
        }
        testAllLevels(
            levels,
            coordinator,
            600_000,
            "FROM index-* | EVAL a = count(*) | LIMIT 100",
            "aggregate function [count(*)] not allowed outside STATS command",
            VerificationException.class.getName()
        );
    }

    private static void testAllLevels(
        Map<Level, String> levels,
        DiscoveryNode coordinator,
        long timeoutMillis,
        String query,
        String expectedErrorMsg,
        String expectedException
    ) throws InterruptedException, ExecutionException {
        for (Map.Entry<Level, String> logLevel : levels.entrySet()) {

            client().execute(
                ClusterUpdateSettingsAction.INSTANCE,
                new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                    Settings.builder().put(logLevel.getValue(), timeoutMillis + "ms")
                )
            ).get();

            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
            request.query(query);
            request.pragmas(randomPragmas());
            CountDownLatch latch = new CountDownLatch(1);
            client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.running(() -> {
                try {
                    if (appender.lastEvent() == null) {
                        if (timeoutMillis == 0) {
                            fail("Expected a slow log with timeout set to zero");
                        }
                        return;
                    }
                    var msg = (ESLogMessage) appender.lastMessage();
                    long took = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_TOOK));
                    long tookMillisExpected = took / 1_000_000;
                    long tookMillis = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_TOOK_MILLIS));
                    assertThat(took, greaterThan(0L));
                    assertThat(tookMillis, greaterThanOrEqualTo(timeoutMillis));
                    assertThat(tookMillis, is(tookMillisExpected));

                    if (expectedException == null) {
                        long planningTook = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_PLANNING_TOOK));
                        long planningTookMillisExpected = planningTook / 1_000_000;
                        long planningTookMillis = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_PLANNING_TOOK_MILLIS));
                        assertThat(planningTook, greaterThanOrEqualTo(0L));
                        assertThat(planningTookMillis, is(planningTookMillisExpected));
                        assertThat(took, greaterThan(planningTook));
                    }

                    assertThat(msg.get(ELASTICSEARCH_QUERYLOG_QUERY), is(query));
                    assertThat(appender.getLastEventAndReset().getLevel(), equalTo(logLevel.getKey()));

                    boolean success = Boolean.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_SUCCESS));
                    assertThat(success, is(expectedException == null));
                    if (expectedErrorMsg == null) {
                        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_ERROR_MESSAGE), is(nullValue()));
                    } else {
                        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_ERROR_MESSAGE), containsString(expectedErrorMsg));
                    }
                    if (expectedException == null) {
                        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_ERROR_TYPE), is(nullValue()));
                    } else {
                        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_ERROR_TYPE), is(expectedException));
                    }
                } finally {
                    latch.countDown();
                }
            }));

            safeAwait(latch);

            assertEquals("All requests must respond", 0, latch.getCount());

            client().execute(
                ClusterUpdateSettingsAction.INSTANCE,
                new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                    Settings.builder().putNull(logLevel.getValue())
                )
            ).get();
        }
    }
}
