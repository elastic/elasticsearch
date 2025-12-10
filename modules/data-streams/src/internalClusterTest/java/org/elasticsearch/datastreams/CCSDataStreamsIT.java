/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CCSDataStreamsIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "cluster_a";
    private static long EARLIEST_TIMESTAMP = 1691348810000L;
    private static long LATEST_TIMESTAMP = 1691348820000L;

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, randomBoolean());
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.concatLists(List.of(MapperExtrasPlugin.class, DataStreamsPlugin.class), super.nodePlugins(clusterAlias));
    }

    @Test
    public void testSuccessfulCCS() throws Exception {
        String localDS = "logs-local";
        String remoteDS = "logs-remote";
        TestClusterInfo testClusterInfo = setupClusters(List.of(localDS), List.of(remoteDS));

        SearchRequest searchRequest = new SearchRequest(localDS, REMOTE_CLUSTER + ":" + remoteDS);
        if (randomBoolean()) {
            searchRequest = searchRequest.scroll(TimeValue.timeValueMinutes(1));
        }
        searchRequest.allowPartialSearchResults(false);
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }
        boolean minimizeRoundTrips = randomBoolean();
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundTrips);
        boolean dfs = randomBoolean();
        if (dfs) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);

            Clusters clusters = response.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getIndexExpression(), equalTo(localDS));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(testClusterInfo.localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(testClusterInfo.localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getIndexExpression(), equalTo(remoteDS));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(testClusterInfo.remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(testClusterInfo.remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        });
    }

    @Test
    public void testFailureStoreCCS() throws Exception {
        String localDS = "logs-local";
        String remoteDS = "logs-remote";
        TestClusterInfo testClusterInfo = setupClusters(List.of(localDS), List.of(remoteDS));

        SearchRequest searchRequest = new SearchRequest(localDS + "::failures", REMOTE_CLUSTER + ":" + remoteDS + "::failures");
        if (randomBoolean()) {
            searchRequest = searchRequest.scroll(TimeValue.timeValueMinutes(1));
        }
        searchRequest.allowPartialSearchResults(false);
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }
        boolean minimizeRoundTrips = randomBoolean();
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundTrips);
        boolean dfs = randomBoolean();
        if (dfs) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);

            Clusters clusters = response.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getIndexExpression(), equalTo(localDS + "::failures"));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(testClusterInfo.localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(testClusterInfo.localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getIndexExpression(), equalTo(remoteDS + "::failures"));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(testClusterInfo.remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(testClusterInfo.remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        });
    }

    private static String remoteResource(String remoteDS) {
        return REMOTE_CLUSTER + RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR + remoteDS;
    }

    record TestClusterInfo(int localNumShards, int remoteNumShards, boolean remoteSkipUnavailable) {}

    private TestClusterInfo setupClusters(List<String> localStreams, List<String> remoteStreams) throws IOException {
        cluster(LOCAL_CLUSTER).ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        int numShardsLocal = randomIntBetween(2, 10);
        for (String localStream : localStreams) {
            setupDataStream(LOCAL_CLUSTER, localStream, numShardsLocal);
        }
        cluster(REMOTE_CLUSTER).ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        int numShardsRemote = randomIntBetween(2, 10);
        for (String remoteStream : remoteStreams) {
            setupDataStream(REMOTE_CLUSTER, remoteStream, numShardsRemote);
        }
        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);

        return new TestClusterInfo(numShardsLocal, numShardsRemote, skipUnavailable);
    }

    private void setupDataStream(String clusterName, String dataStreamName, int numShards) throws IOException {
        Settings settings = indexSettings(numShards, randomIntBetween(0, 1)).build();
        putComposableIndexTemplate(
            clusterName,
            dataStreamName + "-template",
            """
               {
                 "properties": {
                   "f": {
                     "type": "text"
                   },
                   "@timestamp": {
                     "type": "date"
                   }
                 }
               }""",
            List.of(dataStreamName),
            settings,
            null,
            null,
            null,
            true
        );
        indexDocs(client(clusterName), dataStreamName);
        indexFailureDocs(client(clusterName), dataStreamName);
    }

    private void putComposableIndexTemplate(
        String cluster,
        String id,
        @Nullable String mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable Map<String, AliasMetadata> aliases,
        @Nullable DataStreamLifecycle.Template lifecycle,
        boolean withFailureStore
    ) throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(
                    Template.builder()
                        .settings(settings)
                        .mappings(mappings == null ? null : CompressedXContent.fromJSON(mappings))
                        .aliases(aliases)
                        .lifecycle(lifecycle)
                        .dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(withFailureStore))
                )
                .metadata(metadata)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client(cluster).execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet());
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(500, 1200);
        for (int i = 0; i < numDocs; i++) {
            long ts = EARLIEST_TIMESTAMP + i;
            if (i == numDocs - 1) {
                ts = LATEST_TIMESTAMP;
            }
            client.prepareIndex(index).setOpType(DocWriteRequest.OpType.CREATE).setSource("f", "v", "@timestamp", ts).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    private int indexFailureDocs(Client client, String index) {
        int numDocs = between(50, 120);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setOpType(DocWriteRequest.OpType.CREATE).setSource("f", "v").get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }
}
