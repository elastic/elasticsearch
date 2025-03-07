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
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.slowlog.EsqlSlowLog;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class EsqlSlowLogIT extends AbstractEsqlIntegTestCase {
    static MockAppender appender;
    static Logger queryLog = LogManager.getLogger(EsqlSlowLog.SLOWLOG_PREFIX + ".query");
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
        final String node1;
        if (randomBoolean()) {
            internalCluster().ensureAtLeastNumDataNodes(2);
            node1 = randomDataNode().getName();
        } else {
            node1 = randomDataNode().getName();
        }

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
            EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(),
            Level.INFO,
            EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING.getKey(),
            Level.DEBUG,
            EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING.getKey(),
            Level.TRACE,
            EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING.getKey()
        );
        testAllLevels(levels, coordinator, 0);
        for (int i = 0; i < 10; i++) {
            testAllLevels(levels, coordinator, randomIntBetween(0, 10_000));
        }
    }

    private static void testAllLevels(Map<Level, String> levels, DiscoveryNode coordinator, long timeoutMillis) throws InterruptedException,
        ExecutionException {
        for (Map.Entry<Level, String> logLevel : levels.entrySet()) {

            client().execute(
                ClusterUpdateSettingsAction.INSTANCE,
                new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                    Settings.builder().put(logLevel.getValue(), timeoutMillis + "ms")
                )
            ).get();

            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
            var querySource = "FROM index-* | EVAL ip = to_ip(host) | STATS s = COUNT(*) by ip | KEEP ip | LIMIT 100";
            request.query(querySource);
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
                    long took = Long.valueOf(msg.get("elasticsearch.slowlog.took"));
                    long tookMillisExpected = took / 1_000_000;
                    long tookMillis = Long.valueOf(msg.get("elasticsearch.slowlog.took_millis"));
                    assertThat(took, greaterThan(0L));
                    assertThat(tookMillis, greaterThanOrEqualTo(timeoutMillis));
                    assertThat(tookMillis, is(tookMillisExpected));

                    long planningTook = Long.valueOf(msg.get("elasticsearch.slowlog.planning.took"));
                    long planningTookMillisExpected = planningTook / 1_000_000;
                    long planningTookMillis = Long.valueOf(msg.get("elasticsearch.slowlog.planning.took_millis"));
                    assertThat(planningTook, greaterThanOrEqualTo(0L));
                    assertThat(planningTookMillis, greaterThanOrEqualTo(timeoutMillis));
                    assertThat(planningTookMillis, is(planningTookMillisExpected));

                    assertThat(took, greaterThan(planningTook));

                    assertThat(msg.get("elasticsearch.slowlog.query"), is(querySource));
                    assertThat(appender.getLastEventAndReset().getLevel(), equalTo(logLevel.getKey()));
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

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }

}
