/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class SemanticQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String LOCAL_INDEX_NAME = "local-index";
    private static final String REMOTE_INDEX_NAME = "remote-index";
    private static final List<IndexWithBoost> QUERY_INDICES = List.of(
        new IndexWithBoost(LOCAL_INDEX_NAME, 10.0f),
        new IndexWithBoost(fullyQualifiedIndexName(REMOTE_CLUSTER, REMOTE_INDEX_NAME))
    );

    private static final String COMMON_INFERENCE_ID_FIELD = "common-inference-id-field";
    private static final String VARIABLE_INFERENCE_ID_FIELD = "variable-inference-id-field";

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

    public void testSemanticQuery() throws Exception {
        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new SemanticQueryBuilder(COMMON_INFERENCE_ID_FIELD, "a"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
            )
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new SemanticQueryBuilder(VARIABLE_INFERENCE_ID_FIELD, "b"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD))
            )
        );
    }

    public void testSemanticQueryWithCcMinimizeRoundTripsFalse() throws Exception {
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

        final TestIndexInfo localIndexInfo = new TestIndexInfo(LOCAL_INDEX_NAME, Map.of(), Map.of(), Map.of());
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(REMOTE_INDEX_NAME, Map.of(), Map.of(), Map.of());
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Explicitly set ccs_minimize_roundtrips=false in the search request
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);
        SearchRequest searchRequestWithCcMinimizeRoundTripsFalse = new SearchRequest(convertToArray(QUERY_INDICES), searchSourceBuilder);
        searchRequestWithCcMinimizeRoundTripsFalse.setCcsMinimizeRoundtrips(false);
        assertCcsMinimizeRoundTripsFalseFailure.accept(searchRequestWithCcMinimizeRoundTripsFalse);

        // Using a point in time implicitly sets ccs_minimize_roundtrips=false
        BytesReference pitId = openPointInTime(convertToArray(QUERY_INDICES), TimeValue.timeValueMinutes(2));
        SearchSourceBuilder searchSourceBuilderWithPit = new SearchSourceBuilder().query(queryBuilder)
            .pointInTimeBuilder(new PointInTimeBuilder(pitId));
        SearchRequest searchRequestWithPit = new SearchRequest().source(searchSourceBuilderWithPit);
        assertCcsMinimizeRoundTripsFalseFailure.accept(searchRequestWithPit);
    }

    private void configureClusters() throws Exception {
        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";
        final String remoteInferenceId = "remote-inference-id";

        final Map<String, Map<String, Object>> docs = Map.of(
            getDocId(COMMON_INFERENCE_ID_FIELD),
            Map.of(COMMON_INFERENCE_ID_FIELD, "a"),
            getDocId(VARIABLE_INFERENCE_ID_FIELD),
            Map.of(VARIABLE_INFERENCE_ID_FIELD, "b")
        );

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings(), localInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(commonInferenceId),
                VARIABLE_INFERENCE_ID_FIELD,
                semanticTextMapping(localInferenceId)
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
                semanticTextMapping(remoteInferenceId)
            ),
            docs
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);
    }

    private static String getDocId(String field) {
        return field + "_doc";
    }
}
