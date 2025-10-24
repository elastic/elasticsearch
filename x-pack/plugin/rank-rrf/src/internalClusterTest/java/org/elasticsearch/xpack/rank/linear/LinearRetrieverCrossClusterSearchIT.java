/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.ccs.AbstractSemanticCrossClusterSearchTestCase;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.junit.Before;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LinearRetrieverCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String LOCAL_INDEX_NAME = "local-index";
    private static final String REMOTE_INDEX_NAME = "remote-index";

    // Boost the local index so that we can use the same doc values for local and remote indices and have consistent relevance
    private static final List<IndexWithBoost> QUERY_INDICES = List.of(
        new IndexWithBoost(LOCAL_INDEX_NAME, 10.0f),
        new IndexWithBoost(fullyQualifiedIndexName(REMOTE_CLUSTER, REMOTE_INDEX_NAME))
    );

    private static final String COMMON_INFERENCE_ID_FIELD = "common-inference-id-field";
    private static final String VARIABLE_INFERENCE_ID_FIELD = "variable-inference-id-field";
    private static final String MIXED_TYPE_FIELD_1 = "mixed-type-field-1";
    private static final String MIXED_TYPE_FIELD_2 = "mixed-type-field-2";
    private static final String TEXT_FIELD = "text-field";

    boolean clustersConfigured = false;

    @Override
    protected boolean reuseClusters() {
        return true;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (clustersConfigured == false) {
            configureClusters();
            clustersConfigured = true;
        }
    }

    public void testLinearRetriever() throws Exception {
        LinearRetrieverBuilder retrieverBuilder = new LinearRetrieverBuilder(
            null,
            List.of(COMMON_INFERENCE_ID_FIELD),
            "a",
            MinMaxScoreNormalizer.INSTANCE,
            10,
            new float[0],
            new ScoreNormalizer[0]
        );
        assertSearchResponse(
            retrieverBuilder,
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
            ),
            null,
            null
        );
    }

    private void configureClusters() throws Exception {
        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";
        final String remoteInferenceId = "remote-inference-id";

        final Map<String, Map<String, Object>> docs = Map.of(
            getDocId(COMMON_INFERENCE_ID_FIELD),
            Map.of(COMMON_INFERENCE_ID_FIELD, "a"),
            getDocId(VARIABLE_INFERENCE_ID_FIELD),
            Map.of(VARIABLE_INFERENCE_ID_FIELD, "b"),
            getDocId(MIXED_TYPE_FIELD_1),
            Map.of(MIXED_TYPE_FIELD_1, "c"),
            getDocId(MIXED_TYPE_FIELD_2),
            Map.of(MIXED_TYPE_FIELD_2, "d"),
            getDocId(TEXT_FIELD),
            Map.of(TEXT_FIELD, "e")
        );

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings(), localInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(commonInferenceId),
                VARIABLE_INFERENCE_ID_FIELD,
                semanticTextMapping(localInferenceId),
                MIXED_TYPE_FIELD_1,
                semanticTextMapping(localInferenceId),
                MIXED_TYPE_FIELD_2,
                textMapping(),
                TEXT_FIELD,
                textMapping()
            ),
            docs
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            REMOTE_INDEX_NAME,
            Map.of(
                commonInferenceId,
                textEmbeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                remoteInferenceId,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(commonInferenceId),
                VARIABLE_INFERENCE_ID_FIELD,
                semanticTextMapping(remoteInferenceId),
                MIXED_TYPE_FIELD_1,
                textMapping(),
                MIXED_TYPE_FIELD_2,
                semanticTextMapping(remoteInferenceId),
                TEXT_FIELD,
                textMapping()
            ),
            docs
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);
    }

    protected void assertSearchResponse(
        RetrieverBuilder retrieverBuilder,
        List<IndexWithBoost> indices,
        List<SearchResult> expectedSearchResults,
        ClusterFailure expectedRemoteFailure,
        Consumer<SearchRequest> searchRequestModifier
    ) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().retriever(retrieverBuilder).size(expectedSearchResults.size());
        indices.forEach(i -> searchSourceBuilder.indexBoost(i.index(), i.boost()));

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

    private static String getDocId(String field) {
        return field + "_doc";
    }
}
