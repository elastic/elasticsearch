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
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.MockAppender;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.querylog.EsqlQueryLog;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_QUERY;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_SUCCESS;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_TOOK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests ESQL query logging with cross-cluster search when remote clusters are unavailable.
 * This tests the fix for <a href="https://github.com/elastic/elasticsearch/issues/142915">issue #142915</a>.
 */
public class CrossClusterQueryLogUnavailableRemotesIT extends AbstractCrossClusterTestCase {

    static MockAppender appender;
    static Logger queryLog = LogManager.getLogger(EsqlQueryLog.LOGGER_NAME);
    static Level origQueryLogLevel = queryLog.getLevel();

    @BeforeClass
    public static void initQueryLogging() throws IllegalAccessException {
        appender = new MockAppender("ccs_querylog_appender");
        appender.start();
        Loggers.addAppender(queryLog, appender);
        Loggers.setLevel(queryLog, Level.TRACE);
    }

    @AfterClass
    public static void cleanupQueryLogging() {
        Loggers.removeAppender(queryLog, appender);
        appender.stop();
        Loggers.setLevel(queryLog, origQueryLogLevel);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING.getKey(), "0ms").build();
    }

    /**
     * Tests that query logging works correctly when all remote clusters are unavailable
     * with skip_unavailable=true. This reproduces issue #142915 where NPE occurred due to
     * TimeSpanMarkers not being properly stopped before logging.
     */
    public void testQueryLoggingWithAllRemotesUnavailable() throws Exception {
        int numClusters = 2;
        setupClusters(numClusters);
        setSkipUnavailable(REMOTE_CLUSTER_1, true);

        try {
            cluster(REMOTE_CLUSTER_1).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            boolean responseExpectMeta = includeCCSMetadata.v2();

            String query = "FROM " + REMOTE_CLUSTER_1 + ":logs-* | STATS sum(v)";
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.isPartial(), equalTo(true));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));

                assertClusterMetadataInResponse(resp, responseExpectMeta, 1);

                assertQueryLogged(query);
            }
        } finally {
            clearSkipUnavailable(numClusters);
        }
    }

    /**
     * Tests query logging when one remote is unavailable but local cluster and another remote succeed.
     */
    public void testQueryLoggingWithPartialRemoteFailure() throws Exception {
        int numClusters = 3;
        Map<String, Object> testClusterInfo = setupClusters(numClusters);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.num_shards");
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        try {
            cluster(REMOTE_CLUSTER_1).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            boolean responseExpectMeta = includeCCSMetadata.v2();

            String query = "FROM logs-*,*:logs-* | STATS sum(v)";
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(1));
                assertThat(values.get(0), equalTo(List.of(330L)));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.isPartial(), equalTo(true));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, LOCAL_CLUSTER)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertClusterInfoSuccess(remote2Cluster, remote2NumShards);

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertClusterInfoSuccess(localCluster, localNumShards);

                assertClusterMetadataInResponse(resp, responseExpectMeta, numClusters);

                assertQueryLogged(query);
            }
        } finally {
            clearSkipUnavailable(numClusters);
        }
    }

    /**
     * Tests query logging when both remotes are unavailable but local cluster succeeds.
     */
    public void testQueryLoggingWithAllRemotesUnavailableLocalSucceeds() throws Exception {
        int numClusters = 3;
        Map<String, Object> testClusterInfo = setupClusters(numClusters);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        try {
            cluster(REMOTE_CLUSTER_1).close();
            cluster(REMOTE_CLUSTER_2).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            boolean responseExpectMeta = includeCCSMetadata.v2();

            String query = "FROM logs-*,*:logs-* | STATS sum(v)";
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(1));
                assertThat(values.get(0), equalTo(List.of(45L)));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.isPartial(), equalTo(true));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertClusterInfoSuccess(localCluster, localNumShards);

                assertClusterMetadataInResponse(resp, responseExpectMeta, numClusters);

                assertQueryLogged(query);
            }
        } finally {
            clearSkipUnavailable(numClusters);
        }
    }

    private void assertQueryLogged(String expectedQuery) {
        assertThat("Query should have been logged", appender.lastEvent(), is(notNullValue()));
        var msg = (ESLogMessage) appender.lastMessage();
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_QUERY), is(expectedQuery));
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_SUCCESS), is("true"));
        assertThat(Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_TOOK)), greaterThanOrEqualTo(0L));
        appender.getLastEventAndReset();
    }
}
