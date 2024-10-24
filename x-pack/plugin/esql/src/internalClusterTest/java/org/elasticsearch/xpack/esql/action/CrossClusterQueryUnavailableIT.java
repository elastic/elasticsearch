/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CrossClusterQueryUnavailableIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster-a";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPlugin.class);
        plugins.add(org.elasticsearch.xpack.esql.action.CrossClustersQueryIT.InternalExchangePlugin.class);
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

    public void testCCSAgainstDisconnectedRemoteWithSkipUnavailableTrue() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        setSkipUnavailable(true);

        try {
            // close the remote cluster so that it is unavailable
            cluster(REMOTE_CLUSTER).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            boolean responseExpectMeta = includeCCSMetadata.v2();

            try (EsqlQueryResponse resp = runQuery("from logs-*,*:logs-* | stats sum (v)", requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(1));
                assertThat(values.get(0), equalTo(List.of(45L)));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER, LOCAL_CLUSTER)));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
                assertThat(remoteCluster.getIndexExpression(), equalTo("logs-*"));
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

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta);
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testRemoteOnlyCCSAgainstDisconnectedRemoteWithSkipUnavailableTrue() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        setSkipUnavailable(true);

        try {
            // close the remote cluster so that it is unavailable
            cluster(REMOTE_CLUSTER).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            boolean responseExpectMeta = includeCCSMetadata.v2();

            try (EsqlQueryResponse resp = runQuery("FROM " + REMOTE_CLUSTER + ":logs-* | STATS sum (v)", requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER)));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER);
                assertThat(remoteCluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remoteCluster.getTotalShards(), equalTo(0));
                assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
                assertThat(remoteCluster.getSkippedShards(), equalTo(0));
                assertThat(remoteCluster.getFailedShards(), equalTo(0));

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta);
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testCCSAgainstDisconnectedRemoteWithSkipUnavailableFalse() throws Exception {
        setupTwoClusters();
        setSkipUnavailable(false);

        try {
            // close the remote cluster so that it is unavailable
            cluster(REMOTE_CLUSTER).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();

            final Exception exception = expectThrows(
                Exception.class,
                () -> runQuery("FROM logs-*,*:logs-* | STATS sum (v)", requestIncludeMeta)
            );
            assertThat(ExceptionsHelper.isRemoteUnavailableException(exception), is(true));
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testRemoteOnlyCCSAgainstDisconnectedRemoteWithSkipUnavailableFalse() throws Exception {
        setupTwoClusters();
        setSkipUnavailable(false);

        try {
            // close the remote cluster so that it is unavailable
            cluster(REMOTE_CLUSTER).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();

            final Exception exception = expectThrows(Exception.class, () -> runQuery("FROM *:logs-* | STATS sum (v)", requestIncludeMeta));
            assertThat(ExceptionsHelper.isRemoteUnavailableException(exception), is(true));
        } finally {
            clearSkipUnavailable();
        }
    }

    private void setSkipUnavailable(boolean skip) {
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().put("cluster.remote." + REMOTE_CLUSTER + ".skip_unavailable", skip).build())
            .get();
    }

    private void clearSkipUnavailable() {
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().putNull("cluster.remote." + REMOTE_CLUSTER + ".skip_unavailable").build())
            .get();
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

    void waitForRemoteClusterRed(Client client) {
        ClusterHealthResponse resp = client.admin()
            .cluster()
            .prepareHealth(TEST_REQUEST_TIMEOUT, "*")
            .setWaitForStatus(ClusterHealthStatus.RED)
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

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);
        clusterInfo.put("remote.skip_unavailable", skipUnavailable);

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
