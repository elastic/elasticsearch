/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SemanticCrossClusterSearchIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster_a";

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

    public void testSemanticQuery() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };

        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";
        final String remoteInferenceId = "remote-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String variableInferenceIdField = "variable-inference-id-field";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            localIndexName,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings(), localInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                variableInferenceIdField,
                semanticTextMapping(localInferenceId)
            ),
            Map.of("local_doc_1", Map.of(commonInferenceIdField, "a"), "local_doc_2", Map.of(variableInferenceIdField, "b"))
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            remoteIndexName,
            Map.of(
                commonInferenceId,
                textEmbeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                remoteInferenceId,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                variableInferenceIdField,
                semanticTextMapping(remoteInferenceId)
            ),
            Map.of("remote_doc_1", Map.of(commonInferenceIdField, "x"), "remote_doc_2", Map.of(variableInferenceIdField, "y"))
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new SemanticQueryBuilder(commonInferenceIdField, "a"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_1"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_1")
            )
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new SemanticQueryBuilder(variableInferenceIdField, "b"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_2"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_2")
            )
        );
    }

    public void testSemanticQueryWithCcMinimizeRoundTripsFalse() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };
        final SemanticQueryBuilder queryBuilder = new SemanticQueryBuilder("foo", "bar");
        final Consumer<SearchRequest> assertCcsMinimizeRoundTripsFalseFailure = s -> {
            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> client().search(s).actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(
                e.getMessage(),
                equalTo("semantic query does not support cross-cluster search when [ccs_minimize_roundtrips] is false")
            );
        };

        final TestIndexInfo localIndexInfo = new TestIndexInfo(localIndexName, Map.of(), Map.of(), Map.of());
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(remoteIndexName, Map.of(), Map.of(), Map.of());
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Explicitly set ccs_minimize_roundtrips=false in the search request
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);
        SearchRequest searchRequestWithCcMinimizeRoundTripsFalse = new SearchRequest(queryIndices, searchSourceBuilder);
        searchRequestWithCcMinimizeRoundTripsFalse.setCcsMinimizeRoundtrips(false);
        assertCcsMinimizeRoundTripsFalseFailure.accept(searchRequestWithCcMinimizeRoundTripsFalse);

        // Using a point in time implicitly sets ccs_minimize_roundtrips=false
        BytesReference pitId = openPointInTime(queryIndices, TimeValue.timeValueMinutes(2));
        SearchSourceBuilder searchSourceBuilderWithPit = new SearchSourceBuilder().query(queryBuilder)
            .pointInTimeBuilder(new PointInTimeBuilder(pitId));
        SearchRequest searchRequestWithPit = new SearchRequest().source(searchSourceBuilderWithPit);
        assertCcsMinimizeRoundTripsFalseFailure.accept(searchRequestWithPit);
    }

    public void testMatchQuery() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };

        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";
        final String remoteInferenceId = "remote-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String variableInferenceIdField = "variable-inference-id-field";
        final String mixedTypeField = "mixed-type-field";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            localIndexName,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings(), localInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                variableInferenceIdField,
                semanticTextMapping(localInferenceId),
                mixedTypeField,
                semanticTextMapping(localInferenceId)
            ),
            Map.of(
                "local_doc_1",
                Map.of(commonInferenceIdField, "a"),
                "local_doc_2",
                Map.of(variableInferenceIdField, "b"),
                "local_doc_3",
                Map.of(mixedTypeField, "c")
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            remoteIndexName,
            Map.of(
                commonInferenceId,
                textEmbeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                remoteInferenceId,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                variableInferenceIdField,
                semanticTextMapping(remoteInferenceId),
                mixedTypeField,
                Map.of("type", "text")
            ),
            Map.of(
                "remote_doc_1",
                Map.of(commonInferenceIdField, "x"),
                "remote_doc_2",
                Map.of(variableInferenceIdField, "y"),
                "remote_doc_3",
                Map.of(mixedTypeField, "z")
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new MatchQueryBuilder(commonInferenceIdField, "a"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_1"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_1")
            )
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new MatchQueryBuilder(variableInferenceIdField, "b"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_2"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_2")
            )
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new MatchQueryBuilder(mixedTypeField, "z"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_3"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_3")
            )
        );
    }

    private void setupTwoClusters(TestIndexInfo localIndexInfo, TestIndexInfo remoteIndexInfo) throws IOException {
        setupCluster(LOCAL_CLUSTER, localIndexInfo);
        setupCluster(REMOTE_CLUSTER, remoteIndexInfo);
    }

    private void setupCluster(String clusterAlias, TestIndexInfo indexInfo) throws IOException {
        final Client client = client(clusterAlias);
        final String indexName = indexInfo.name();

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

        Settings indexSettings = indexSettings(randomIntBetween(2, 5), randomIntBetween(0, 1)).build();
        assertAcked(client.admin().indices().prepareCreate(indexName).setSettings(indexSettings).setMapping(indexInfo.mappings()));
        assertFalse(
            client.admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, indexName)
                .setWaitForYellowStatus()
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

    private BytesReference openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        return response.getPointInTimeId();
    }

    private static void createInferenceEndpoint(Client client, TaskType taskType, String inferenceId, Map<String, Object> serviceSettings)
        throws IOException {
        final String service = switch (taskType) {
            case TEXT_EMBEDDING -> TestDenseInferenceServiceExtension.TestInferenceService.NAME;
            case SPARSE_EMBEDDING -> TestSparseInferenceServiceExtension.TestInferenceService.NAME;
            default -> throw new IllegalArgumentException("Unhandled task type [" + taskType + "]");
        };

        final BytesReference content;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("service", service);
            builder.field("service_settings", serviceSettings);
            builder.endObject();

            content = BytesReference.bytes(builder);
        }

        PutInferenceModelAction.Request request = new PutInferenceModelAction.Request(
            taskType,
            inferenceId,
            content,
            XContentType.JSON,
            TEST_REQUEST_TIMEOUT
        );
        var responseFuture = client.execute(PutInferenceModelAction.INSTANCE, request);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getModel().getInferenceEntityId(), equalTo(inferenceId));
    }

    private void assertSearchResponse(QueryBuilder queryBuilder, String[] indices, List<SearchResult> expectedSearchResults)
        throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder).size(expectedSearchResults.size());
        SearchRequest searchRequest = new SearchRequest(indices, searchSourceBuilder);

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
        });
    }

    private static MinimalServiceSettings sparseEmbeddingServiceSettings() {
        return new MinimalServiceSettings(null, TaskType.SPARSE_EMBEDDING, null, null, null);
    }

    private static MinimalServiceSettings textEmbeddingServiceSettings(
        int dimensions,
        SimilarityMeasure similarity,
        DenseVectorFieldMapper.ElementType elementType
    ) {
        return new MinimalServiceSettings(null, TaskType.TEXT_EMBEDDING, dimensions, similarity, elementType);
    }

    private static Map<String, Object> semanticTextMapping(String inferenceId) {
        return Map.of("type", SemanticTextFieldMapper.CONTENT_TYPE, "inference_id", inferenceId);
    }

    private static String fullyQualifiedIndexName(String clusterAlias, String indexName) {
        return clusterAlias + ":" + indexName;
    }

    public static class FakeMlPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return new MlInferenceNamedXContentProvider().getNamedWriteables();
        }

        @Override
        public List<QueryVectorBuilderSpec<?>> getQueryVectorBuilders() {
            return List.of(
                new QueryVectorBuilderSpec<>(
                    TextEmbeddingQueryVectorBuilder.NAME,
                    TextEmbeddingQueryVectorBuilder::new,
                    TextEmbeddingQueryVectorBuilder.PARSER
                )
            );
        }
    }

    private record TestIndexInfo(
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

    private record SearchResult(String clusterAlias, String index, String id) {}
}
