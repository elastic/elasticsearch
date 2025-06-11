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
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class EsqlResponseListenerTests extends ESTestCase {
    private final String LOCAL_CLUSTER_ALIAS = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

    private static MockAppender appender;
    static Logger logger = LogManager.getLogger(EsqlResponseListener.class);

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("testAppender");
        appender.start();
        Configurator.setLevel(logger, Level.DEBUG);
        Loggers.addAppender(logger, appender);
    }

    @After
    public void clear() {
        appender.events.clear();
    }

    @AfterClass
    public static void cleanup() {
        appender.stop();
        Loggers.removeAppender(logger, appender);
    }

    public void testLogPartialFailures() {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(false);
        executionInfo.swapCluster(
            LOCAL_CLUSTER_ALIAS,
            (k, v) -> new EsqlExecutionInfo.Cluster(
                LOCAL_CLUSTER_ALIAS,
                "idx",
                false,
                EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                10,
                10,
                3,
                0,
                List.of(
                    new ShardSearchFailure(new Exception("dummy"), target(LOCAL_CLUSTER_ALIAS, 0)),
                    new ShardSearchFailure(new Exception("error"), target(LOCAL_CLUSTER_ALIAS, 1))
                ),
                new TimeValue(4444L)
            )
        );
        EsqlResponseListener.logPartialFailures("/_query", Map.of(), executionInfo);

        assertThat(appender.events, hasSize(2));
        LogEvent logEvent = appender.events.get(0);
        assertThat(logEvent.getLevel(), equalTo(Level.WARN));
        assertThat(logEvent.getMessage().getFormattedMessage(), equalTo("partial failure at path: /_query, params: {}"));
        assertThat(logEvent.getThrown().getCause().getMessage(), equalTo("dummy"));
        logEvent = appender.events.get(1);
        assertThat(logEvent.getLevel(), equalTo(Level.WARN));
        assertThat(logEvent.getMessage().getFormattedMessage(), equalTo("partial failure at path: /_query, params: {}"));
        assertThat(logEvent.getThrown().getCause().getMessage(), equalTo("error"));
    }

    public void testLogPartialFailuresRemote() {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(false);
        executionInfo.swapCluster(
            "remote_cluster",
            (k, v) -> new EsqlExecutionInfo.Cluster(
                "remote_cluster",
                "idx",
                false,
                EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                10,
                10,
                3,
                0,
                List.of(new ShardSearchFailure(new Exception("dummy"), target("remote_cluster", 0))),
                new TimeValue(4444L)
            )
        );
        EsqlResponseListener.logPartialFailures("/_query", Map.of(), executionInfo);

        assertThat(appender.events, hasSize(1));
        LogEvent logEvent = appender.events.get(0);
        assertThat(logEvent.getLevel(), equalTo(Level.WARN));
        assertThat(
            logEvent.getMessage().getFormattedMessage(),
            equalTo("partial failure at path: /_query, params: {}, cluster: remote_cluster")
        );
        assertThat(logEvent.getThrown().getCause().getMessage(), equalTo("dummy"));
    }

    private SearchShardTarget target(String clusterAlias, int shardId) {
        return new SearchShardTarget("node", new ShardId("idx", "uuid", shardId), clusterAlias);
    }

    private static class MockAppender extends AbstractAppender {
        public final List<LogEvent> events = new ArrayList<>();

        MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null, false);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }
    }
}
