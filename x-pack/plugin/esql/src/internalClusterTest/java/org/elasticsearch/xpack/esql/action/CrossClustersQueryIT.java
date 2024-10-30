/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CrossClustersQueryIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster-a";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, randomBoolean());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        return plugins;
    }

    public static class InternalExchangePlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.timeSetting(
                    ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING,
                    TimeValue.timeValueSeconds(30),
                    Setting.Property.NodeScope
                )
            );
        }
    }

    public void testSuccessfulPathways() {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();
        try (EsqlQueryResponse resp = runQuery("from logs-*,*:logs-* | stats sum (v)", requestIncludeMeta)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(330L)));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));

            // ensure that the _clusters metadata is present only if requested
            assertClusterMetadataInResponse(resp, responseExpectMeta);
        }

        try (EsqlQueryResponse resp = runQuery("from logs-*,*:logs-* | stats count(*) by tag | sort tag | keep tag", requestIncludeMeta)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertThat(values.get(0), equalTo(List.of("local")));
            assertThat(values.get(1), equalTo(List.of("remote")));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));

            // ensure that the _clusters metadata is present only if requested
            assertClusterMetadataInResponse(resp, responseExpectMeta);
        }
    }

    public void testSearchesWhereMissingIndicesAreSpecified() {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        // since a valid local index was specified, the invalid index on cluster-a does not throw an exception,
        // but instead is simply ignored - ensure this is captured in the EsqlExecutionInfo
        try (EsqlQueryResponse resp = runQuery("from logs-*,cluster-a:no_such_index | stats sum (v)", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(45L)));

            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("no_such_index"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));  // 0 since no matching index, thus no shards to search
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));
        }

        // since the remote cluster has a valid index expression, the missing local index is ignored
        // make this is captured in the EsqlExecutionInfo
        try (
            EsqlQueryResponse resp = runQuery(
                "from no_such_index,*:logs-* | stats count(*) by tag | sort tag | keep tag",
                requestIncludeMeta
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of("remote")));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("no_such_index"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(localCluster.getTotalShards(), equalTo(0));
            assertThat(localCluster.getSuccessfulShards(), equalTo(0));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));
        }

        // when multiple invalid indices are specified on the remote cluster, both should be ignored and present
        // in the index expression of the EsqlExecutionInfo and with an indication that zero shards were searched
        try (
            EsqlQueryResponse resp = runQuery(
                "FROM no_such_index*,*:no_such_index1,*:no_such_index2,logs-1 | STATS COUNT(*) by tag | SORT tag | KEEP tag",
                requestIncludeMeta
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of("local")));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("no_such_index1,no_such_index2"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("no_such_index*,logs-1"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));
        }

        // wildcard on remote cluster that matches nothing - should be present in EsqlExecutionInfo marked as SKIPPED, no shards searched
        try (EsqlQueryResponse resp = runQuery("from cluster-a:no_such_index*,logs-* | stats sum (v)", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(45L)));

            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("no_such_index*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));
        }
    }

    public void testSearchesWhereNonExistentClusterIsSpecifiedWithWildcards() {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        // a query which matches no remote cluster is not a cross cluster search
        try (EsqlQueryResponse resp = runQuery("from logs-*,x*:no_such_index* | stats sum (v)", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(45L)));

            assertNotNull(executionInfo);
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER)));
            assertThat(executionInfo.isCrossClusterSearch(), is(false));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            // since this not a CCS, only the overall took time in the EsqlExecutionInfo matters
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        }

        // cluster-foo* matches nothing and so should not be present in the EsqlExecutionInfo
        try (
            EsqlQueryResponse resp = runQuery(
                "from logs-*,no_such_index*,cluster-a:no_such_index*,cluster-foo*:* | stats sum (v)",
                requestIncludeMeta
            )
        ) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(45L)));

            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("no_such_index*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-*,no_such_index*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));
        }
    }

    /**
     * Searches with LIMIT 0 are used by Kibana to get a list of columns. After the initial planning
     * (which involves cross-cluster field-caps calls), it is a coordinator only operation at query time
     * which uses a different pathway compared to queries that require data node (and remote data node) operations
     * at query time.
     */
    public void testCCSExecutionOnSearchesWithLimit0() {
        setupTwoClusters();

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        // Ensure non-cross cluster queries have overall took time
        try (EsqlQueryResponse resp = runQuery("FROM logs* | LIMIT 0", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(false));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        }

        // ensure cross-cluster searches have overall took time and correct per-cluster details in EsqlExecutionInfo
        try (EsqlQueryResponse resp = runQuery("FROM logs*,cluster-a:* | LIMIT 0", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));
        }

        try (EsqlQueryResponse resp = runQuery("FROM logs*,cluster-a:nomatch* | LIMIT 0", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("nomatch*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(localCluster.getTotalShards(), equalTo(0));
            assertThat(localCluster.getSuccessfulShards(), equalTo(0));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));
        }

        try (EsqlQueryResponse resp = runQuery("FROM nomatch*,cluster-a:* | LIMIT 0", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("nomatch*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));
        }
    }

    public void testMetadataIndex() {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs*,*:logs* METADATA _index | stats sum(v) by _index | sort _index",
                requestIncludeMeta
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0), equalTo(List.of(285L, "cluster-a:logs-2")));
            assertThat(values.get(1), equalTo(List.of(45L, "logs-1")));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));
        }
    }

    public void testProfile() {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        assumeTrue("pragmas only enabled on snapshot builds", Build.current().isSnapshot());
        // uses shard partitioning as segments can be merged during these queries
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.DATA_PARTITIONING.getKey(), DataPartitioning.SHARD).build());
        // Use single replicas for the target indices, to make sure we hit the same set of target nodes
        client(LOCAL_CLUSTER).admin()
            .indices()
            .prepareUpdateSettings("logs-1")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put("index.routing.rebalance.enable", "none"))
            .get();
        waitForNoInitializingShards(client(LOCAL_CLUSTER), TimeValue.timeValueSeconds(30), "logs-1");
        client(REMOTE_CLUSTER).admin()
            .indices()
            .prepareUpdateSettings("logs-2")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put("index.routing.rebalance.enable", "none"))
            .get();
        waitForNoInitializingShards(client(REMOTE_CLUSTER), TimeValue.timeValueSeconds(30), "logs-2");
        final int localOnlyProfiles;
        {
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
            request.query("FROM logs* | stats sum(v)");
            request.pragmas(pragmas);
            request.profile(true);
            try (EsqlQueryResponse resp = runQuery(request)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(45L)));
                assertNotNull(resp.profile());
                List<DriverProfile> drivers = resp.profile().drivers();
                assertThat(drivers.size(), greaterThanOrEqualTo(2)); // one coordinator and at least one data
                localOnlyProfiles = drivers.size();

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
                assertNull(remoteCluster);
                assertThat(executionInfo.isCrossClusterSearch(), is(false));
                assertThat(executionInfo.includeCCSMetadata(), is(false));
                // since this not a CCS, only the overall took time in the EsqlExecutionInfo matters
                assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
            }
        }
        final int remoteOnlyProfiles;
        {
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
            request.query("FROM *:logs* | stats sum(v)");
            request.pragmas(pragmas);
            request.profile(true);
            try (EsqlQueryResponse resp = runQuery(request)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(285L)));
                assertNotNull(resp.profile());
                List<DriverProfile> drivers = resp.profile().drivers();
                assertThat(drivers.size(), greaterThanOrEqualTo(3)); // two coordinators and at least one data
                remoteOnlyProfiles = drivers.size();

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), is(false));
                assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
                assertThat(remoteCluster.getIndexExpression(), equalTo("logs*"));
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
                assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
                assertThat(remoteCluster.getSkippedShards(), equalTo(0));
                assertThat(remoteCluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertNull(localCluster);
            }
        }
        final int allProfiles;
        {
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
            request.query("FROM logs*,*:logs* | stats total = sum(v)");
            request.pragmas(pragmas);
            request.profile(true);
            try (EsqlQueryResponse resp = runQuery(request)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(330L)));
                assertNotNull(resp.profile());
                List<DriverProfile> drivers = resp.profile().drivers();
                assertThat(drivers.size(), greaterThanOrEqualTo(4)); // two coordinators and at least two data
                allProfiles = drivers.size();

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), is(false));
                assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
                assertThat(remoteCluster.getIndexExpression(), equalTo("logs*"));
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
                assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
                assertThat(remoteCluster.getSkippedShards(), equalTo(0));
                assertThat(remoteCluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
                assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
                assertThat(localCluster.getSkippedShards(), equalTo(0));
                assertThat(localCluster.getFailedShards(), equalTo(0));
            }
        }
        assertThat(allProfiles, equalTo(localOnlyProfiles + remoteOnlyProfiles - 1));
    }

    public void testWarnings() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query("FROM logs*,*:logs* | EVAL ip = to_ip(id) | STATS total = sum(v) by ip | LIMIT 10");
        InternalTestCluster cluster = cluster(LOCAL_CLUSTER);
        String node = randomFrom(cluster.getNodeNames());
        CountDownLatch latch = new CountDownLatch(1);
        cluster.client(node).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(resp -> {
            TransportService ts = cluster.getInstance(TransportService.class, node);
            Map<String, List<String>> responseHeaders = ts.getThreadPool().getThreadContext().getResponseHeaders();
            List<String> warnings = responseHeaders.getOrDefault("Warning", List.of())
                .stream()
                .filter(w -> w.contains("is not an IP string literal"))
                .toList();
            assertThat(warnings.size(), greaterThanOrEqualTo(20));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0).get(0), equalTo(330L));
            assertNull(values.get(0).get(1));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.includeCCSMetadata(), is(false));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));

            latch.countDown();
        }, e -> {
            latch.countDown();
            throw new AssertionError(e);
        }));
        assertTrue(latch.await(30, TimeUnit.SECONDS));
    }

    private static void assertClusterMetadataInResponse(EsqlQueryResponse resp, boolean responseExpectMeta) {
        try {
            final Map<String, Object> esqlResponseAsMap = XContentTestUtils.convertToMap(resp);
            final Object clusters = esqlResponseAsMap.get("_clusters");
            if (responseExpectMeta) {
                assertNotNull(clusters);
                // test a few entries to ensure it looks correct (other tests do a full analysis of the metadata in the response)
                @SuppressWarnings("unchecked")
                Map<String, Object> inner = (Map<String, Object>) clusters;
                assertTrue(inner.containsKey("total"));
                assertTrue(inner.containsKey("details"));
            } else {
                assertNull(clusters);
            }
        } catch (IOException e) {
            fail("Could not convert ESQL response to Map: " + e);
        }
    }

    protected EsqlQueryResponse runQuery(String query, Boolean ccsMetadataInResponse) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        if (ccsMetadataInResponse != null) {
            request.includeCCSMetadata(ccsMetadataInResponse);
        }
        return runQuery(request);
    }

    protected EsqlQueryResponse runQuery(EsqlQueryRequest request) {
        return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
    }

    /**
     * v1: value to send to runQuery (can be null; null means use default value)
     * v2: whether to expect CCS Metadata in the response (cannot be null)
     * @return
     */
    public static Tuple<Boolean, Boolean> randomIncludeCCSMetadata() {
        return switch (randomIntBetween(1, 3)) {
            case 1 -> new Tuple<>(Boolean.TRUE, Boolean.TRUE);
            case 2 -> new Tuple<>(Boolean.FALSE, Boolean.FALSE);
            case 3 -> new Tuple<>(null, Boolean.FALSE);
            default -> throw new AssertionError("should not get here");
        };
    }

    void waitForNoInitializingShards(Client client, TimeValue timeout, String... indices) {
        ClusterHealthResponse resp = client.admin()
            .cluster()
            .prepareHealth(TEST_REQUEST_TIMEOUT, indices)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .setTimeout(timeout)
            .get();
        assertFalse(Strings.toString(resp, true, true), resp.isTimedOut());
    }

    Map<String, Object> setupTwoClusters() {
        String localIndex = "logs-1";
        int numShardsLocal = randomIntBetween(1, 5);
        populateLocalIndices(localIndex, numShardsLocal);

        String remoteIndex = "logs-2";
        int numShardsRemote = randomIntBetween(1, 5);
        populateRemoteIndices(remoteIndex, numShardsRemote);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", localIndex);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", remoteIndex);
        return clusterInfo;
    }

    void populateLocalIndices(String indexName, int numShards) {
        Client localClient = client(LOCAL_CLUSTER);
        assertAcked(
            localClient.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping("id", "type=keyword", "tag", "type=keyword", "v", "type=long")
        );
        for (int i = 0; i < 10; i++) {
            localClient.prepareIndex(indexName).setSource("id", "local-" + i, "tag", "local", "v", i).get();
        }
        localClient.admin().indices().prepareRefresh(indexName).get();
    }

    void populateRemoteIndices(String indexName, int numShards) {
        Client remoteClient = client(REMOTE_CLUSTER);
        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping("id", "type=keyword", "tag", "type=keyword", "v", "type=long")
        );
        for (int i = 0; i < 10; i++) {
            remoteClient.prepareIndex(indexName).setSource("id", "remote-" + i, "tag", "remote", "v", i * i).get();
        }
        remoteClient.admin().indices().prepareRefresh(indexName).get();
    }
}
