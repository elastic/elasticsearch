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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.inference.queries.GenericQueryVectorBuilder;
import org.elasticsearch.xpack.inference.vectors.EmbeddingQueryVectorBuilder;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.inference.Utils.randomInferenceStringGroup;

public class KnnVectorQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String COMMON_INFERENCE_ID_FIELD = "common-inference-id-field";
    private static final String VARIABLE_INFERENCE_ID_FIELD = "variable-inference-id-field";
    private static final String MIXED_TYPE_FIELD_1 = "mixed-type-field-1";
    private static final String MIXED_TYPE_FIELD_2 = "mixed-type-field-2";
    private static final String DENSE_VECTOR_FIELD = "dense-vector-field";

    private static final String COMMON_INFERENCE_ID = "common-inference-id";
    private static final String LOCAL_INFERENCE_ID = "local-inference-id";
    private static final String REMOTE_INFERENCE_ID = "remote-inference-id";

    private static final int DENSE_VECTOR_FIELD_DIMENSIONS = 256;

    private static final Exception GENERIC_QUERY_VECTOR_BUILDER_ERROR = new IllegalArgumentException(
        "Generic query vector builder failure"
    );

    private static final String MISSING_INFERENCE_ID_ERROR = "[inference_id] must be specified";

    private final String semanticFieldType;

    public KnnVectorQueryBuilderCrossClusterSearchIT(@Name("semanticFieldType") String semanticFieldType) {
        this.semanticFieldType = semanticFieldType;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { "semantic_text" }, new Object[] { "semantic" });
    }

    @Override
    protected boolean reuseClusters() {
        return true;
    }

    @Before
    public void setupClusters() throws Exception {
        configureClusters();
    }

    public void testKnnQueryWithCcsMinimizeRoundTripsTrue() throws Exception {
        knnQueryBaseTestCases(true);

        // Check that omitting the inference ID when querying a remote dense vector field leads to the expected partial failure
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                MIXED_TYPE_FIELD_2,
                new EmbeddingQueryVectorBuilder(null, randomInferenceStringGroup(), null),
                10,
                100,
                10f,
                null
            ),
            QUERY_INDICES,
            List.of(new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(new FailureCause(IllegalArgumentException.class, MISSING_INFERENCE_ID_ERROR))
            ),
            null
        );
    }

    public void testKnnQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
        knnQueryBaseTestCases(false);

        // Query an inference field on a remote cluster
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                COMMON_INFERENCE_ID_FIELD,
                new EmbeddingQueryVectorBuilder(null, randomInferenceStringGroup(), null),
                10,
                100,
                10f,
                null
            ),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );

        // Check that omitting the inference ID when querying a remote dense vector field leads to the expected failure
        assertSearchFailure(
            new KnnVectorQueryBuilder(
                MIXED_TYPE_FIELD_2,
                new EmbeddingQueryVectorBuilder(null, randomInferenceStringGroup(), null),
                10,
                100,
                10f,
                null
            ),
            QUERY_INDICES,
            IllegalArgumentException.class,
            MISSING_INFERENCE_ID_ERROR,
            s -> s.setCcsMinimizeRoundtrips(false)
        );
        assertSearchFailure(
            new KnnVectorQueryBuilder(
                MIXED_TYPE_FIELD_2,
                new EmbeddingQueryVectorBuilder(null, randomInferenceStringGroup(), null),
                10,
                100,
                10f,
                null
            ),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            IllegalArgumentException.class,
            MISSING_INFERENCE_ID_ERROR,
            s -> s.setCcsMinimizeRoundtrips(false)
        );
    }

    public void testBlankQueryHandling() throws Exception {
        List<Boolean> ccsMinimizeRoundTripsValues = List.of(true, false);
        for (Boolean ccsMinimizeRoundTrips : ccsMinimizeRoundTripsValues) {
            final Consumer<SearchRequest> searchRequestModifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundTrips);
            final String expectedLocalClusterAlias = getExpectedLocalClusterAlias(ccsMinimizeRoundTrips);

            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    COMMON_INFERENCE_ID_FIELD,
                    new EmbeddingQueryVectorBuilder(null, new InferenceStringGroup("   "), null),
                    10,
                    100,
                    10f,
                    null
                ),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
                ),
                null,
                searchRequestModifier
            );

            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    VARIABLE_INFERENCE_ID_FIELD,
                    new EmbeddingQueryVectorBuilder(null, new InferenceStringGroup("   "), null),
                    10,
                    100,
                    10f,
                    null
                ),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD))
                ),
                null,
                searchRequestModifier
            );

            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    MIXED_TYPE_FIELD_1,
                    new EmbeddingQueryVectorBuilder(LOCAL_INFERENCE_ID, new InferenceStringGroup("   "), null),
                    10,
                    100,
                    10f,
                    null
                ),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
                ),
                null,
                searchRequestModifier
            );

            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    MIXED_TYPE_FIELD_2,
                    new EmbeddingQueryVectorBuilder(LOCAL_INFERENCE_ID, new InferenceStringGroup("   "), null),
                    10,
                    100,
                    10f,
                    null
                ),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
                ),
                null,
                searchRequestModifier
            );

            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    DENSE_VECTOR_FIELD,
                    new EmbeddingQueryVectorBuilder(COMMON_INFERENCE_ID, new InferenceStringGroup("   "), null),
                    10,
                    100,
                    10f,
                    null
                ),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(DENSE_VECTOR_FIELD)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(DENSE_VECTOR_FIELD))
                ),
                null,
                searchRequestModifier
            );
        }
    }

    public void testGenericQueryVectorBuilderReturnsVector() throws Exception {
        List<Boolean> ccsMinimizeRoundTripsValues = List.of(true, false);
        for (Boolean ccsMinimizeRoundTrips : ccsMinimizeRoundTripsValues) {
            final Consumer<SearchRequest> searchRequestModifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundTrips);
            final String expectedLocalClusterAlias = getExpectedLocalClusterAlias(ccsMinimizeRoundTrips);

            // DENSE_VECTOR_FIELD: 256 dims on both clusters
            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    DENSE_VECTOR_FIELD,
                    new GenericQueryVectorBuilder(
                        generateDenseVectorFieldValue(DENSE_VECTOR_FIELD_DIMENSIONS, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f)
                    ),
                    10,
                    100,
                    10f,
                    null
                ),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(DENSE_VECTOR_FIELD)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(DENSE_VECTOR_FIELD))
                ),
                null,
                searchRequestModifier
            );

            // MIXED_TYPE_FIELD_1: 384 dims on both clusters (dense_vector on local, semantic_text on remote)
            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    MIXED_TYPE_FIELD_1,
                    new GenericQueryVectorBuilder(generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)),
                    10,
                    100,
                    10f,
                    null
                ),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
                ),
                null,
                searchRequestModifier
            );

            // MIXED_TYPE_FIELD_2: 384 dims on both clusters (semantic_text on local, dense_vector on remote)
            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    MIXED_TYPE_FIELD_2,
                    new GenericQueryVectorBuilder(generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)),
                    10,
                    100,
                    10f,
                    null
                ),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
                ),
                null,
                searchRequestModifier
            );

            // COMMON_INFERENCE_ID_FIELD: Different dims across clusters (256 local, 384 remote)
            // Query local cluster only with 256 dims
            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    COMMON_INFERENCE_ID_FIELD,
                    new GenericQueryVectorBuilder(generateDenseVectorFieldValue(256, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f)),
                    10,
                    100,
                    10f,
                    null
                ),
                List.of(LOCAL_INDEX_NAME),
                List.of(new SearchResult(null, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))),
                null,
                searchRequestModifier
            );

            // COMMON_INFERENCE_ID_FIELD: Query remote cluster only with 384 dims
            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    COMMON_INFERENCE_ID_FIELD,
                    new GenericQueryVectorBuilder(generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f)),
                    10,
                    100,
                    10f,
                    null
                ),
                List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
                List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))),
                null,
                searchRequestModifier
            );

            // VARIABLE_INFERENCE_ID_FIELD: Different dims across clusters (384 local, 256 remote)
            // Query local cluster only with 384 dims
            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    VARIABLE_INFERENCE_ID_FIELD,
                    new GenericQueryVectorBuilder(generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f)),
                    10,
                    100,
                    10f,
                    null
                ),
                List.of(LOCAL_INDEX_NAME),
                List.of(new SearchResult(null, LOCAL_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD))),
                null,
                searchRequestModifier
            );

            // VARIABLE_INFERENCE_ID_FIELD: Query remote cluster only with 256 dims
            assertSearchResponse(
                new KnnVectorQueryBuilder(
                    VARIABLE_INFERENCE_ID_FIELD,
                    new GenericQueryVectorBuilder(generateDenseVectorFieldValue(256, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f)),
                    10,
                    100,
                    10f,
                    null
                ),
                List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
                List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD))),
                null,
                searchRequestModifier
            );
        }
    }

    public void testGenericQueryVectorBuilderThrowsError() {
        List<Boolean> ccsMinimizeRoundTripsValues = List.of(true, false);
        List<String> fields = List.of(
            DENSE_VECTOR_FIELD,
            MIXED_TYPE_FIELD_1,
            MIXED_TYPE_FIELD_2,
            COMMON_INFERENCE_ID_FIELD,
            VARIABLE_INFERENCE_ID_FIELD
        );

        for (Boolean ccsMinimizeRoundTrips : ccsMinimizeRoundTripsValues) {
            final Consumer<SearchRequest> searchRequestModifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundTrips);
            for (String field : fields) {
                assertSearchFailure(
                    new KnnVectorQueryBuilder(field, new GenericQueryVectorBuilder(GENERIC_QUERY_VECTOR_BUILDER_ERROR), 10, 100, 10f, null),
                    QUERY_INDICES,
                    GENERIC_QUERY_VECTOR_BUILDER_ERROR.getClass(),
                    GENERIC_QUERY_VECTOR_BUILDER_ERROR.getMessage(),
                    searchRequestModifier
                );
            }
        }
    }

    private void knnQueryBaseTestCases(boolean ccsMinimizeRoundTrips) throws Exception {
        final Consumer<SearchRequest> searchRequestModifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundTrips);
        final String expectedLocalClusterAlias = getExpectedLocalClusterAlias(ccsMinimizeRoundTrips);

        // Query a field that has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                COMMON_INFERENCE_ID_FIELD,
                new EmbeddingQueryVectorBuilder(null, randomInferenceStringGroup(), null),
                10,
                100,
                10f,
                null
            ),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
            ),
            null,
            searchRequestModifier
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                VARIABLE_INFERENCE_ID_FIELD,
                new EmbeddingQueryVectorBuilder(null, randomInferenceStringGroup(), null),
                10,
                100,
                10f,
                null
            ),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD))
            ),
            null,
            searchRequestModifier
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                MIXED_TYPE_FIELD_1,
                new EmbeddingQueryVectorBuilder(LOCAL_INFERENCE_ID, randomInferenceStringGroup(), null),
                10,
                100,
                10f,
                null
            ),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
            ),
            null,
            searchRequestModifier
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                MIXED_TYPE_FIELD_2,
                new EmbeddingQueryVectorBuilder(LOCAL_INFERENCE_ID, randomInferenceStringGroup(), null),
                10,
                100,
                10f,
                null
            ),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
            ),
            null,
            searchRequestModifier
        );

        // Query a field that has mixed types across clusters using a query vector
        final VectorData queryVector = new VectorData(
            generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_1, queryVector, 10, 100, 10f, null, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
            ),
            null,
            searchRequestModifier
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_2, queryVector, 10, 100, 10f, null, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
            ),
            null,
            searchRequestModifier
        );

        // Query using index patterns
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                COMMON_INFERENCE_ID_FIELD,
                new EmbeddingQueryVectorBuilder(null, randomInferenceStringGroup(), null),
                10,
                100,
                10f,
                null
            ),
            List.of("local-*", fullyQualifiedIndexName("cluster_*", "remote-*")),
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
            ),
            null,
            searchRequestModifier
        );

        // Validate that a CCS knn query functions when only dense vector fields are queried
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                DENSE_VECTOR_FIELD,
                generateDenseVectorFieldValue(DENSE_VECTOR_FIELD_DIMENSIONS, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f),
                10,
                100,
                10f,
                null,
                null
            ),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(DENSE_VECTOR_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(DENSE_VECTOR_FIELD))
            ),
            null,
            searchRequestModifier
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                DENSE_VECTOR_FIELD,
                generateDenseVectorFieldValue(DENSE_VECTOR_FIELD_DIMENSIONS, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f),
                10,
                100,
                10f,
                null,
                null
            ),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(DENSE_VECTOR_FIELD))),
            null,
            searchRequestModifier
        );
    }

    private Map<String, Object> generateMapping(String inferenceId) {
        return switch (semanticFieldType) {
            case "semantic_text" -> semanticTextMapping(inferenceId);
            case "semantic" -> semanticFieldMapping(inferenceId);
            default -> throw new IllegalArgumentException("Unknown semantic field type: " + semanticFieldType);
        };
    }

    private void configureClusters() throws Exception {
        Map<String, Object> localFieldMappings = new HashMap<>(
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                generateMapping(COMMON_INFERENCE_ID),
                VARIABLE_INFERENCE_ID_FIELD,
                generateMapping(LOCAL_INFERENCE_ID),
                MIXED_TYPE_FIELD_1,
                denseVectorMapping(384),
                MIXED_TYPE_FIELD_2,
                generateMapping(LOCAL_INFERENCE_ID),
                DENSE_VECTOR_FIELD,
                denseVectorMapping(DENSE_VECTOR_FIELD_DIMENSIONS)
            )
        );
        Map<String, Map<String, Object>> localDocs = new HashMap<>(
            Map.of(
                getDocId(COMMON_INFERENCE_ID_FIELD),
                Map.of(COMMON_INFERENCE_ID_FIELD, randomAlphaOfLengthBetween(2, 5)),
                getDocId(VARIABLE_INFERENCE_ID_FIELD),
                Map.of(VARIABLE_INFERENCE_ID_FIELD, randomAlphaOfLengthBetween(2, 5)),
                getDocId(MIXED_TYPE_FIELD_1),
                Map.of(MIXED_TYPE_FIELD_1, generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)),
                getDocId(MIXED_TYPE_FIELD_2),
                Map.of(MIXED_TYPE_FIELD_2, randomAlphaOfLengthBetween(2, 5)),
                getDocId(DENSE_VECTOR_FIELD),
                Map.of(
                    DENSE_VECTOR_FIELD,
                    generateDenseVectorFieldValue(DENSE_VECTOR_FIELD_DIMENSIONS, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f)
                )
            )
        );
        Map<String, Object> remoteFieldMappings = new HashMap<>(
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                generateMapping(COMMON_INFERENCE_ID),
                VARIABLE_INFERENCE_ID_FIELD,
                generateMapping(REMOTE_INFERENCE_ID),
                MIXED_TYPE_FIELD_1,
                generateMapping(COMMON_INFERENCE_ID),
                MIXED_TYPE_FIELD_2,
                denseVectorMapping(384),
                DENSE_VECTOR_FIELD,
                denseVectorMapping(DENSE_VECTOR_FIELD_DIMENSIONS)
            )
        );
        Map<String, Map<String, Object>> remoteDocs = new HashMap<>(
            Map.of(
                getDocId(COMMON_INFERENCE_ID_FIELD),
                Map.of(COMMON_INFERENCE_ID_FIELD, randomAlphaOfLengthBetween(2, 5)),
                getDocId(VARIABLE_INFERENCE_ID_FIELD),
                Map.of(VARIABLE_INFERENCE_ID_FIELD, randomAlphaOfLengthBetween(2, 5)),
                getDocId(MIXED_TYPE_FIELD_1),
                Map.of(MIXED_TYPE_FIELD_1, randomAlphaOfLengthBetween(2, 5)),
                getDocId(MIXED_TYPE_FIELD_2),
                Map.of(MIXED_TYPE_FIELD_2, generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)),
                getDocId(DENSE_VECTOR_FIELD),
                Map.of(
                    DENSE_VECTOR_FIELD,
                    generateDenseVectorFieldValue(DENSE_VECTOR_FIELD_DIMENSIONS, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)
                )
            )
        );
        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(
                COMMON_INFERENCE_ID,
                embeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                LOCAL_INFERENCE_ID,
                embeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            localFieldMappings,
            localDocs
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            REMOTE_INDEX_NAME,
            Map.of(
                COMMON_INFERENCE_ID,
                embeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                REMOTE_INFERENCE_ID,
                embeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            remoteFieldMappings,
            remoteDocs
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);
    }

    private static String getDocId(String field) {
        return field + "_doc";
    }
}
