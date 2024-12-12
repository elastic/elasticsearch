/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
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

public class CrossClusterQueryUnavailableRemotesIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER_1 = "cluster-a";
    private static final String REMOTE_CLUSTER_2 = "cluster-b";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
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
        int numClusters = 3;
        Map<String, Object> testClusterInfo = setupClusters(numClusters);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.num_shards");
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            try (EsqlQueryResponse resp = runQuery("FROM logs-*,*:logs-* | STATS sum (v)", requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(1));
                assertThat(values.get(0), equalTo(List.of(330L)));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, LOCAL_CLUSTER)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remote1Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote1Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote1Cluster.getTotalShards(), equalTo(0));
                assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote1Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(remote2Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote2Cluster.getTotalShards(), equalTo(remote2NumShards));
                assertThat(remote2Cluster.getSuccessfulShards(), equalTo(remote2NumShards));
                assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote2Cluster.getFailedShards(), equalTo(0));

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

            // scenario where there are no indices to match because
            // 1) the local cluster indexExpression and REMOTE_CLUSTER_2 indexExpression match no indices
            // 2) the REMOTE_CLUSTER_1 is unavailable
            // 3) both remotes are marked as skip_un=true
            String query = "FROM nomatch*," + REMOTE_CLUSTER_1 + ":logs-*," + REMOTE_CLUSTER_2 + ":nomatch* | STATS sum (v)";
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, LOCAL_CLUSTER)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remote1Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote1Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote1Cluster.getTotalShards(), equalTo(0));
                assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote1Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("nomatch*"));
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote2Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote2Cluster.getTotalShards(), equalTo(0));
                assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote2Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("nomatch*"));
                // local cluster should never be marked as SKIPPED
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(localCluster.getTotalShards(), equalTo(0));
                assertThat(localCluster.getSuccessfulShards(), equalTo(0));
                assertThat(localCluster.getSkippedShards(), equalTo(0));
                assertThat(localCluster.getFailedShards(), equalTo(0));

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta);
            }

            // close remote-cluster-2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();

            try (EsqlQueryResponse resp = runQuery("FROM logs-*,*:logs-* | STATS sum (v)", requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(1));
                assertThat(values.get(0), equalTo(List.of(45L)));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, LOCAL_CLUSTER)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remote1Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote1Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote1Cluster.getTotalShards(), equalTo(0));
                assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote1Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote2Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote2Cluster.getTotalShards(), equalTo(0));
                assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote2Cluster.getFailedShards(), equalTo(0));

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
            clearSkipUnavailable(numClusters);
        }
    }

    public void testRemoteOnlyCCSAgainstDisconnectedRemoteWithSkipUnavailableTrue() throws Exception {
        int numClusters = 3;
        setupClusters(numClusters);
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        try {
            // close remote cluster 1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            boolean responseExpectMeta = includeCCSMetadata.v2();

            // query only the REMOTE_CLUSTER_1
            try (EsqlQueryResponse resp = runQuery("FROM " + REMOTE_CLUSTER_1 + ":logs-* | STATS sum (v)", requestIncludeMeta)) {
                List<ColumnInfoImpl> columns = resp.columns();
                assertThat(columns.size(), equalTo(1));
                // column from an empty result should be {"name":"<no-fields>","type":"null"}
                assertThat(columns.get(0).name(), equalTo("<no-fields>"));
                assertThat(columns.get(0).type(), equalTo(DataType.NULL));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1)));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
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

            // close remote cluster 2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();

            // query only the both remote clusters
            try (
                EsqlQueryResponse resp = runQuery(
                    "FROM " + REMOTE_CLUSTER_1 + ":logs-*," + REMOTE_CLUSTER_2 + ":logs-* | STATS sum (v)",
                    requestIncludeMeta
                )
            ) {
                List<ColumnInfoImpl> columns = resp.columns();
                assertThat(columns.size(), equalTo(1));
                // column from an empty result should be {"name":"<no-fields>","type":"null"}
                assertThat(columns.get(0).name(), equalTo("<no-fields>"));
                assertThat(columns.get(0).type(), equalTo(DataType.NULL));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remote1Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote1Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote1Cluster.getTotalShards(), equalTo(0));
                assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote1Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote2Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote2Cluster.getTotalShards(), equalTo(0));
                assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote2Cluster.getFailedShards(), equalTo(0));

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta);
            }

        } finally {
            clearSkipUnavailable(numClusters);
        }
    }

    public void testCCSAgainstDisconnectedRemoteWithSkipUnavailableFalse() throws Exception {
        int numClusters = 2;
        setupClusters(numClusters);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);

        try {
            // close the remote cluster so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();

            final Exception exception = expectThrows(
                Exception.class,
                () -> runQuery("FROM logs-*,*:logs-* | STATS sum (v)", requestIncludeMeta)
            );
            assertThat(ExceptionsHelper.isRemoteUnavailableException(exception), is(true));
        } finally {
            clearSkipUnavailable(numClusters);
        }
    }

    public void testRemoteOnlyCCSAgainstDisconnectedRemoteWithSkipUnavailableFalse() throws Exception {
        int numClusters = 3;
        setupClusters(numClusters);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, randomBoolean());

        try {
            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            {
                // close the remote cluster so that it is unavailable
                cluster(REMOTE_CLUSTER_1).close();
                Exception exception = expectThrows(Exception.class, () -> runQuery("FROM *:logs-* | STATS sum (v)", requestIncludeMeta));
                assertThat(ExceptionsHelper.isRemoteUnavailableException(exception), is(true));
            }
            {
                // close remote cluster 2 so that it is unavailable
                cluster(REMOTE_CLUSTER_2).close();
                Exception exception = expectThrows(Exception.class, () -> runQuery("FROM *:logs-* | STATS sum (v)", requestIncludeMeta));
                assertThat(ExceptionsHelper.isRemoteUnavailableException(exception), is(true));
            }
        } finally {
            clearSkipUnavailable(numClusters);
        }
    }

    private void setSkipUnavailable(String clusterAlias, boolean skip) {
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().put("cluster.remote." + clusterAlias + ".skip_unavailable", skip).build())
            .get();
    }

    private void clearSkipUnavailable(int numClusters) {
        assert numClusters == 2 || numClusters == 3 : "Only 2 or 3 clusters supported";
        Settings.Builder settingsBuilder = Settings.builder().putNull("cluster.remote." + REMOTE_CLUSTER_1 + ".skip_unavailable");
        if (numClusters == 3) {
            settingsBuilder.putNull("cluster.remote." + REMOTE_CLUSTER_2 + ".skip_unavailable");
        }
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(settingsBuilder.build())
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

    Map<String, Object> setupClusters(int numClusters) {
        assert numClusters == 2 || numClusters == 3 : "2 or 3 clusters supported not: " + numClusters;
        String localIndex = "logs-1";
        int numShardsLocal = randomIntBetween(1, 5);
        populateLocalIndices(localIndex, numShardsLocal);

        String remoteIndex = "logs-2";
        int numShardsRemote = randomIntBetween(1, 5);
        populateRemoteIndices(REMOTE_CLUSTER_1, remoteIndex, numShardsRemote);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", localIndex);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", remoteIndex);

        if (numClusters == 3) {
            int numShardsRemote2 = randomIntBetween(1, 5);
            populateRemoteIndices(REMOTE_CLUSTER_2, remoteIndex, numShardsRemote2);
            clusterInfo.put("remote2.index", remoteIndex);
            clusterInfo.put("remote2.num_shards", numShardsRemote2);
        }

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

    void populateRemoteIndices(String clusterAlias, String indexName, int numShards) {
        Client remoteClient = client(clusterAlias);
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
