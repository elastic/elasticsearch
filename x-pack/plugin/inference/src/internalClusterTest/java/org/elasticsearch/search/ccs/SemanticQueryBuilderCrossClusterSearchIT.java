/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class SemanticQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final int LOCAL_INFERENCE_EMBEDDING_DIMENSIONS = 256;
    private static final int REMOTE_INFERENCE_EMBEDDING_DIMENSIONS = 384;
    private static final String SEMANTIC_TEXT_FIELD_WITH_COMMON_INFERENCE_ID = "common-inference-id-field";
    private static final String SEMANTIC_TEXT_FIELD_WITH_VARIABLE_INFERENCE_ID = "variable-inference-id-field";
    private static final String SEMANTIC_FIELD_WITH_COMMON_INFERENCE_ID = "common-semantic-field";
    private static final String SEMANTIC_FIELD_WITH_VARIABLE_INFERENCE_ID = "variable-semantic-field";

    private final String fieldType;

    public SemanticQueryBuilderCrossClusterSearchIT(@Name("fieldType") String fieldType) {
        this.fieldType = fieldType;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { "semantic_text" }, new Object[] { "semantic" });
    }

    boolean clustersConfigured = false;

    @Override
    protected boolean reuseClusters() {
        return true;
    }

    @Before
    public void configureClustersIfNeeded() throws Exception {
        if (clustersConfigured == false) {
            configureClusters();
            clustersConfigured = true;
        }
    }

    private String commonField() {
        return "semantic".equals(fieldType) ? SEMANTIC_FIELD_WITH_COMMON_INFERENCE_ID : SEMANTIC_TEXT_FIELD_WITH_COMMON_INFERENCE_ID;
    }

    private String variableField() {
        return "semantic".equals(fieldType) ? SEMANTIC_FIELD_WITH_VARIABLE_INFERENCE_ID : SEMANTIC_TEXT_FIELD_WITH_VARIABLE_INFERENCE_ID;
    }

    public void testSemanticQueryWithCcMinimizeRoundTripsTrue() throws Exception {
        semanticQueryBaseTestCases(true);
    }

    public void testSemanticQueryWithCcMinimizeRoundTripsFalse() throws Exception {
        semanticQueryBaseTestCases(false);

        // Use a point in time to implicitly set ccs_minimize_roundtrips=false
        BytesReference pitId = openPointInTime(QUERY_INDICES, TimeValue.timeValueMinutes(2));
        assertSearchResponse(
            new SemanticQueryBuilder(commonField(), "value of field with common inference id"),
            null,
            List.of(
                new SearchResult(null, LOCAL_INDEX_NAME, getDocId(commonField())),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(commonField()))
            ),
            null,
            s -> s.source().pointInTimeBuilder(new PointInTimeBuilder(pitId))
        );
    }

    public void testBlankQueryHandling() throws Exception {
        List<Boolean> ccsMinimizeRoundTripsValues = List.of(true, false);
        for (Boolean ccsMinimizeRoundTrips : ccsMinimizeRoundTripsValues) {
            final Consumer<SearchRequest> searchRequestModifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundTrips);
            final String expectedLocalClusterAlias = getExpectedLocalClusterAlias(ccsMinimizeRoundTrips);

            assertSearchResponse(
                new SemanticQueryBuilder(commonField(), "   "),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(commonField())),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(commonField()))
                ),
                null,
                searchRequestModifier
            );

            assertSearchResponse(
                new SemanticQueryBuilder(variableField(), "   "),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(variableField())),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(variableField()))
                ),
                null,
                searchRequestModifier
            );
        }
    }

    private void semanticQueryBaseTestCases(boolean ccsMinimizeRoundTrips) throws Exception {
        final Consumer<SearchRequest> searchRequestModifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundTrips);
        final String expectedLocalClusterAlias = getExpectedLocalClusterAlias(ccsMinimizeRoundTrips);

        // Query a field that has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new SemanticQueryBuilder(commonField(), "value of field with common inference id"),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(commonField())),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(commonField()))
            ),
            null,
            searchRequestModifier
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new SemanticQueryBuilder(variableField(), "value of field with different inference id"),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(variableField())),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(variableField()))
            ),
            null,
            searchRequestModifier
        );

        // Query an inference field on a remote cluster
        assertSearchResponse(
            new SemanticQueryBuilder(commonField(), "value of field with common inference id"),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(commonField()))),
            null,
            searchRequestModifier
        );

        // Query using index patterns
        assertSearchResponse(
            new SemanticQueryBuilder(commonField(), "value of field with common inference id"),
            List.of("local-*", fullyQualifiedIndexName("cluster_*", "remote-*")),
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(commonField())),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(commonField()))
            ),
            null,
            searchRequestModifier
        );
    }

    private void configureClusters() throws Exception {
        final String commonSemanticTextInferenceId = "common-inference-id";
        final String localSemanticTextInferenceId = "local-inference-id";
        final String remoteSemanticTextInferenceId = "remote-inference-id";
        final String commonSemanticInferenceId = "common-semantic-inference-id";
        final String localSemanticInferenceId = "local-semantic-inference-id";
        final String remoteSemanticInferenceId = "remote-semantic-inference-id";

        final Map<String, Map<String, Object>> docs = Map.of(
            getDocId(SEMANTIC_TEXT_FIELD_WITH_COMMON_INFERENCE_ID),
            Map.of(SEMANTIC_TEXT_FIELD_WITH_COMMON_INFERENCE_ID, "value of field with common inference id"),
            getDocId(SEMANTIC_TEXT_FIELD_WITH_VARIABLE_INFERENCE_ID),
            Map.of(SEMANTIC_TEXT_FIELD_WITH_VARIABLE_INFERENCE_ID, "value of field with different inference id"),
            getDocId(SEMANTIC_FIELD_WITH_COMMON_INFERENCE_ID),
            Map.of(SEMANTIC_FIELD_WITH_COMMON_INFERENCE_ID, "value of field with common inference id"),
            getDocId(SEMANTIC_FIELD_WITH_VARIABLE_INFERENCE_ID),
            Map.of(SEMANTIC_FIELD_WITH_VARIABLE_INFERENCE_ID, "value of field with different inference id")
        );

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(
                commonSemanticTextInferenceId,
                sparseEmbeddingServiceSettings(),
                localSemanticTextInferenceId,
                sparseEmbeddingServiceSettings(),
                commonSemanticInferenceId,
                embeddingServiceSettings(
                    LOCAL_INFERENCE_EMBEDDING_DIMENSIONS,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.FLOAT
                ),
                localSemanticInferenceId,
                embeddingServiceSettings(
                    LOCAL_INFERENCE_EMBEDDING_DIMENSIONS,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.FLOAT
                )
            ),
            Map.of(
                SEMANTIC_TEXT_FIELD_WITH_COMMON_INFERENCE_ID,
                semanticTextMapping(commonSemanticTextInferenceId),
                SEMANTIC_TEXT_FIELD_WITH_VARIABLE_INFERENCE_ID,
                semanticTextMapping(localSemanticTextInferenceId),
                SEMANTIC_FIELD_WITH_COMMON_INFERENCE_ID,
                semanticFieldMapping(commonSemanticInferenceId),
                SEMANTIC_FIELD_WITH_VARIABLE_INFERENCE_ID,
                semanticFieldMapping(localSemanticInferenceId)
            ),
            docs
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            REMOTE_INDEX_NAME,
            Map.of(
                commonSemanticTextInferenceId,
                textEmbeddingServiceSettings(
                    REMOTE_INFERENCE_EMBEDDING_DIMENSIONS,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.FLOAT
                ),
                remoteSemanticTextInferenceId,
                textEmbeddingServiceSettings(
                    REMOTE_INFERENCE_EMBEDDING_DIMENSIONS,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.FLOAT
                ),
                commonSemanticInferenceId,
                embeddingServiceSettings(
                    REMOTE_INFERENCE_EMBEDDING_DIMENSIONS,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.FLOAT
                ),
                remoteSemanticInferenceId,
                embeddingServiceSettings(
                    REMOTE_INFERENCE_EMBEDDING_DIMENSIONS,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.FLOAT
                )
            ),
            Map.of(
                SEMANTIC_TEXT_FIELD_WITH_COMMON_INFERENCE_ID,
                semanticTextMapping(commonSemanticTextInferenceId),
                SEMANTIC_TEXT_FIELD_WITH_VARIABLE_INFERENCE_ID,
                semanticTextMapping(remoteSemanticTextInferenceId),
                SEMANTIC_FIELD_WITH_COMMON_INFERENCE_ID,
                semanticFieldMapping(commonSemanticInferenceId),
                SEMANTIC_FIELD_WITH_VARIABLE_INFERENCE_ID,
                semanticFieldMapping(remoteSemanticInferenceId)
            ),
            docs
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);
    }

    private static String getDocId(String field) {
        return field + "_doc";
    }
}
