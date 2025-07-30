/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public abstract class AbstractCrossClusterTestCase extends AbstractMultiClustersTestCase {
    protected static final String REMOTE_CLUSTER_1 = "cluster-a";
    protected static final String REMOTE_CLUSTER_2 = "remote-b";
    protected static final String LOCAL_INDEX = "logs-1";
    protected static final String REMOTE_INDEX = "logs-2";
    protected static final String INDEX_WITH_BLOCKING_MAPPING = "blocking";
    protected static final String INDEX_WITH_FAIL_MAPPING = "failing";
    protected static final AtomicLong NEXT_DOC_ID = new AtomicLong(0);

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false, REMOTE_CLUSTER_2, randomBoolean());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class); // allows the async_search DELETE action
        plugins.add(CrossClusterAsyncQueryIT.InternalExchangePlugin.class);
        plugins.add(SimplePauseFieldPlugin.class);
        plugins.add(FailingPauseFieldPlugin.class);
        plugins.add(FailingFieldPlugin.class);
        plugins.add(CrossClusterAsyncQueryIT.CountingPauseFieldPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(EsqlPlugin.QUERY_ALLOW_PARTIAL_RESULTS.getKey(), false).build();
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

    public static class CountingPauseFieldPlugin extends SimplePauseFieldPlugin {
        public static AtomicLong count = new AtomicLong(0);

        protected String scriptTypeName() {
            return "pause_count";
        }

        public static void resetPlugin() {
            count.set(0);
        }

        @Override
        public boolean onWait() throws InterruptedException {
            count.incrementAndGet();
            return allowEmitting.await(30, TimeUnit.SECONDS);
        }
    }

    @Before
    public void resetPlugin() {
        SimplePauseFieldPlugin.resetPlugin();
        FailingPauseFieldPlugin.resetPlugin();
        CrossClusterAsyncQueryIT.CountingPauseFieldPlugin.resetPlugin();
    }

    @After
    public void releaseLatches() {
        SimplePauseFieldPlugin.release();
        FailingPauseFieldPlugin.release();
        CrossClusterAsyncQueryIT.CountingPauseFieldPlugin.release();
    }

    protected void assertClusterInfoSuccess(EsqlExecutionInfo.Cluster cluster, int numShards) {
        assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
        assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertThat(cluster.getTotalShards(), equalTo(numShards));
        assertThat(cluster.getSuccessfulShards(), equalTo(numShards));
        assertThat(cluster.getSkippedShards(), equalTo(0));
        assertThat(cluster.getFailedShards(), equalTo(0));
        assertThat(cluster.getFailures().size(), equalTo(0));
    }

    protected static void assertClusterMetadataInResponse(EsqlQueryResponse resp, boolean responseExpectMeta, int numClusters) {
        try {
            final Map<String, Object> esqlResponseAsMap = XContentTestUtils.convertToMap(resp);
            final Object clusters = esqlResponseAsMap.get("_clusters");
            if (responseExpectMeta) {
                assertNotNull(clusters);
                // test a few entries to ensure it looks correct (other tests do a full analysis of the metadata in the response)
                @SuppressWarnings("unchecked")
                Map<String, Object> inner = (Map<String, Object>) clusters;
                assertTrue(inner.containsKey("total"));
                assertThat((int) inner.get("total"), equalTo(numClusters));
                assertTrue(inner.containsKey("details"));
            } else {
                final Object partial = esqlResponseAsMap.get("is_partial");
                if (partial != null && (Boolean) partial) {
                    // If we have partial response, we could have cluster metadata, it should contain details.
                    // Details should not be empty, and it should contain clusters with failures.
                    if (clusters != null) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> inner = (Map<String, Object>) clusters;
                        assertThat(inner, aMapWithSize(1));
                        assertTrue(inner.containsKey("details"));
                        @SuppressWarnings("unchecked")
                        Map<String, Object> details = (Map<String, Object>) inner.get("details");
                        assertThat(details.size(), greaterThanOrEqualTo(1));
                        details.forEach((k, v) -> {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> cluster = (Map<String, Object>) v;
                            assertTrue(cluster.containsKey("failures"));
                        });
                    }
                } else {
                    assertNull(clusters);
                }
            }
        } catch (IOException e) {
            fail("Could not convert ESQLQueryResponse to Map: " + e);
        }
    }

    protected Map<String, Object> setupClusters(int numClusters) throws IOException {
        assert numClusters == 2 || numClusters == 3 : "2 or 3 clusters supported not: " + numClusters;
        int numShardsLocal = randomIntBetween(1, 5);
        populateIndex(LOCAL_CLUSTER, LOCAL_INDEX, numShardsLocal, 10);

        int numShardsRemote = randomIntBetween(1, 5);
        populateRemoteIndices(REMOTE_CLUSTER_1, REMOTE_INDEX, numShardsRemote);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", LOCAL_INDEX);
        clusterInfo.put("remote1.num_shards", numShardsRemote);
        clusterInfo.put("remote1.index", REMOTE_INDEX);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", REMOTE_INDEX);

        if (numClusters == 3) {
            int numShardsRemote2 = randomIntBetween(1, 5);
            populateRemoteIndices(REMOTE_CLUSTER_2, REMOTE_INDEX, numShardsRemote2);
            clusterInfo.put("remote2.index", REMOTE_INDEX);
            clusterInfo.put("remote2.num_shards", numShardsRemote2);
        }

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER_1);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER_1).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);
        clusterInfo.put("remote.skip_unavailable", skipUnavailable);

        return clusterInfo;
    }

    protected Set<String> populateIndex(String clusterAlias, String indexName, int numShards, int numDocs) {
        Client client = client(clusterAlias);
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping("id", "type=keyword", "tag", "type=keyword", "v", "type=long", "const", "type=long")
        );
        Set<String> ids = new HashSet<>();
        String tag = Strings.isEmpty(clusterAlias) ? "local" : clusterAlias;
        for (int i = 0; i < numDocs; i++) {
            String id = Long.toString(NEXT_DOC_ID.incrementAndGet());
            client.prepareIndex(indexName).setSource("id", id, "tag", tag, "v", i).get();
            ids.add(id);
        }
        client.admin().indices().prepareRefresh(indexName).get();
        return ids;
    }

    protected void populateRuntimeIndex(String clusterAlias, String langName, String indexName) throws IOException {
        populateRuntimeIndex(clusterAlias, langName, indexName, 10);
    }

    protected void populateRuntimeIndex(String clusterAlias, String langName, String indexName, int count) throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("const");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", langName).endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        mapping.endObject();
        client(clusterAlias).admin().indices().prepareCreate(indexName).setMapping(mapping).get();
        BulkRequestBuilder bulk = client(clusterAlias).prepareBulk(indexName).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < count; i++) {
            bulk.add(new IndexRequest().source("foo", i));
        }
        bulk.get();
    }

    protected void populateRemoteIndices(String clusterAlias, String indexName, int numShards) throws IOException {
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

    protected void setSkipUnavailable(String clusterAlias, boolean skip) {
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().put("cluster.remote." + clusterAlias + ".skip_unavailable", skip).build())
            .get();
    }

    protected void clearSkipUnavailable(int numClusters) {
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

    protected void clearSkipUnavailable() {
        clearSkipUnavailable(3);
    }

    protected EsqlQueryResponse runQuery(EsqlQueryRequest request) {
        return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
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

    static List<TaskInfo> getDriverTasks(Client client) {
        return client.admin().cluster().prepareListTasks().setActions(DriverTaskRunner.ACTION_NAME).setDetailed(true).get().getTasks();
    }
}
