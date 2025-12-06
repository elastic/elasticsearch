/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoResponse;
import org.elasticsearch.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.inference.integration.IntegrationTestUtils.createInferenceEndpoint;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public abstract class AbstractSemanticCrossClusterSearchTestCase extends AbstractMultiClustersTestCase {
    protected static final String REMOTE_CLUSTER = "cluster_a";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, DEFAULT_SKIP_UNAVAILABLE);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, FakeMlPlugin.class);
    }

    protected void setupTwoClusters(TestIndexInfo localIndexInfo, TestIndexInfo remoteIndexInfo) throws Exception {
        setupCluster(LOCAL_CLUSTER, localIndexInfo);
        setupCluster(REMOTE_CLUSTER, remoteIndexInfo);
        waitUntilRemoteClusterConnected(REMOTE_CLUSTER);
    }

    protected void setupCluster(String clusterAlias, TestIndexInfo indexInfo) throws IOException {
        final Client client = client(clusterAlias);
        final String indexName = indexInfo.name();
        final int dataNodeCount = cluster(clusterAlias).numDataNodes();

        for (var entry : indexInfo.inferenceEndpoints().entrySet()) {
            String inferenceId = entry.getKey();
            MinimalServiceSettings minimalServiceSettings = entry.getValue();

            Map<String, Object> serviceSettings = new HashMap<>();
            serviceSettings.put("model", randomAlphaOfLength(5));
            serviceSettings.put("api_key", randomAlphaOfLength(5));
            if (minimalServiceSettings.taskType() == TaskType.TEXT_EMBEDDING) {
                serviceSettings.put("dimensions", minimalServiceSettings.dimensions());
                serviceSettings.put("similarity", minimalServiceSettings.similarity());
                serviceSettings.put("element_type", minimalServiceSettings.elementType());
            }

            createInferenceEndpoint(client, minimalServiceSettings.taskType(), inferenceId, serviceSettings);
        }

        Settings indexSettings = indexSettings(randomIntBetween(1, dataNodeCount), 0).build();
        assertAcked(client.admin().indices().prepareCreate(indexName).setSettings(indexSettings).setMapping(indexInfo.mappings()));
        assertFalse(
            client.admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, indexName)
                .setWaitForGreenStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );

        for (var entry : indexInfo.docs().entrySet()) {
            String docId = entry.getKey();
            Map<String, Object> doc = entry.getValue();

            DocWriteResponse response = client.prepareIndex(indexName).setId(docId).setSource(doc).execute().actionGet();
            assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        }
        BroadcastResponse refreshResponse = client.admin().indices().prepareRefresh(indexName).execute().actionGet();
        assertThat(refreshResponse.getStatus(), is(RestStatus.OK));
    }

    protected void waitUntilRemoteClusterConnected(String clusterAlias) throws Exception {
        RemoteInfoRequest request = new RemoteInfoRequest();
        assertBusy(() -> {
            RemoteInfoResponse response = client().execute(TransportRemoteInfoAction.TYPE, request).actionGet(TEST_REQUEST_TIMEOUT);
            boolean connected = response.getInfos()
                .stream()
                .filter(i -> i.getClusterAlias().equals(clusterAlias))
                .anyMatch(RemoteConnectionInfo::isConnected);
            assertThat(connected, is(true));
        }, 30, TimeUnit.SECONDS);
    }

    protected BytesReference openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        return response.getPointInTimeId();
    }

    protected void assertSearchResponse(QueryBuilder queryBuilder, List<IndexWithBoost> indices, List<SearchResult> expectedSearchResults)
        throws Exception {
        assertSearchResponse(queryBuilder, indices, expectedSearchResults, null, null);
    }

    protected void assertSearchResponse(
        QueryBuilder queryBuilder,
        List<IndexWithBoost> indices,
        List<SearchResult> expectedSearchResults,
        ClusterFailure expectedRemoteFailure,
        Consumer<SearchRequest> searchRequestModifier
    ) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder).size(expectedSearchResults.size());
        indices.forEach(i -> searchSourceBuilder.indexBoost(RemoteClusterAware.splitIndexName(i.index())[1], i.boost()));

        SearchRequest searchRequest = new SearchRequest(convertToArray(indices), searchSourceBuilder);
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        if (searchRequestModifier != null) {
            searchRequestModifier.accept(searchRequest);
        }

        assertResponse(client().search(searchRequest), response -> {
            SearchHit[] hits = response.getHits().getHits();
            assertThat(hits.length, equalTo(expectedSearchResults.size()));

            Iterator<SearchResult> searchResultIterator = expectedSearchResults.iterator();
            for (int i = 0; i < hits.length; i++) {
                SearchResult expectedSearchResult = searchResultIterator.next();
                SearchHit actualSearchResult = hits[i];

                assertThat(actualSearchResult.getClusterAlias(), equalTo(expectedSearchResult.clusterAlias()));
                assertThat(actualSearchResult.getIndex(), equalTo(expectedSearchResult.index()));
                assertThat(actualSearchResult.getId(), equalTo(expectedSearchResult.id()));
            }

            SearchResponse.Clusters clusters = response.getClusters();
            assertThat(clusters.getCluster(LOCAL_CLUSTER).getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(clusters.getCluster(LOCAL_CLUSTER).getFailures().isEmpty(), is(true));

            SearchResponse.Cluster remoteCluster = clusters.getCluster(REMOTE_CLUSTER);
            if (expectedRemoteFailure != null) {
                assertThat(remoteCluster.getStatus(), equalTo(expectedRemoteFailure.status()));

                Set<FailureCause> expectedFailures = expectedRemoteFailure.failures();
                Set<FailureCause> actualFailures = remoteCluster.getFailures()
                    .stream()
                    .map(f -> new FailureCause(f.getCause().getClass(), f.getCause().getMessage()))
                    .collect(Collectors.toSet());
                assertThat(actualFailures, equalTo(expectedFailures));
            } else {
                assertThat(remoteCluster.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
                assertThat(remoteCluster.getFailures().isEmpty(), is(true));
            }
        });
    }

    protected static MinimalServiceSettings sparseEmbeddingServiceSettings() {
        return new MinimalServiceSettings(null, TaskType.SPARSE_EMBEDDING, null, null, null);
    }

    protected static MinimalServiceSettings textEmbeddingServiceSettings(
        int dimensions,
        SimilarityMeasure similarity,
        DenseVectorFieldMapper.ElementType elementType
    ) {
        return new MinimalServiceSettings(null, TaskType.TEXT_EMBEDDING, dimensions, similarity, elementType);
    }

    protected static Map<String, Object> semanticTextMapping(String inferenceId) {
        return Map.of("type", SemanticTextFieldMapper.CONTENT_TYPE, "inference_id", inferenceId);
    }

    protected static Map<String, Object> textMapping() {
        return Map.of("type", "text");
    }

    protected static Map<String, Object> denseVectorMapping(int dimensions) {
        return Map.of("type", DenseVectorFieldMapper.CONTENT_TYPE, "dims", dimensions);
    }

    protected static Map<String, Object> sparseVectorMapping() {
        return Map.of("type", SparseVectorFieldMapper.CONTENT_TYPE);
    }

    protected static String fullyQualifiedIndexName(String clusterAlias, String indexName) {
        return RemoteClusterAware.buildRemoteIndexName(clusterAlias, indexName);
    }

    protected static float[] generateDenseVectorFieldValue(int dimensions, DenseVectorFieldMapper.ElementType elementType, float value) {
        if (elementType == DenseVectorFieldMapper.ElementType.BIT) {
            assert dimensions % 8 == 0;
            dimensions /= 8;
        }

        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            // Use a constant value so that relevance is consistent
            vector[i] = value;
        }

        return vector;
    }

    protected static Map<String, Float> generateSparseVectorFieldValue(float weight) {
        // Generate values that have the same recall behavior as those produced by TestSparseInferenceServiceExtension. Use a constant token
        // weight so that relevance is consistent.
        return Map.of("feature_0", weight);
    }

    protected static String[] convertToArray(List<IndexWithBoost> indices) {
        return indices.stream().map(IndexWithBoost::index).toArray(String[]::new);
    }

    protected record TestIndexInfo(
        String name,
        Map<String, MinimalServiceSettings> inferenceEndpoints,
        Map<String, Object> mappings,
        Map<String, Map<String, Object>> docs
    ) {
        @Override
        public Map<String, Object> mappings() {
            return Map.of("properties", mappings);
        }
    }

    protected record SearchResult(@Nullable String clusterAlias, String index, String id) {}

    protected record FailureCause(Class<? extends Throwable> causeClass, String message) {}

    protected record ClusterFailure(SearchResponse.Cluster.Status status, Set<FailureCause> failures) {}

    protected record IndexWithBoost(String index, float boost) {
        public IndexWithBoost(String index) {
            this(index, 1.0f);
        }
    }
}
