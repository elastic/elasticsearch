/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

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

    public void testKnnQueryWithCcsMinimizeRoundTripsTrue() throws Exception {
        knnQueryBaseTestCases(true);

        // Check that omitting the inference ID when querying a remote dense vector field leads to the expected partial failure
        assertSearchResponse(
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_2, new TextEmbeddingQueryVectorBuilder(null, "c"), 10, 100, 10f, null, null),
            QUERY_INDICES,
            List.of(new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(new FailureCause(IllegalArgumentException.class, "[model_id] must not be null."))
            ),
            null
        );
    }

    public void testKnnQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
        knnQueryBaseTestCases(false);

        // Query an inference field on a remote cluster
        assertSearchResponse(
            new KnnVectorQueryBuilder(COMMON_INFERENCE_ID_FIELD, new TextEmbeddingQueryVectorBuilder(null, "a"), 10, 100, 10f, null, null),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );

        // Check that omitting the inference ID when querying a remote dense vector field leads to the expected failure
        assertSearchFailure(
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_2, new TextEmbeddingQueryVectorBuilder(null, "c"), 10, 100, 10f, null, null),
            QUERY_INDICES,
            IllegalArgumentException.class,
            "[model_id] must not be null.",
            s -> s.setCcsMinimizeRoundtrips(false)
        );
        assertSearchFailure(
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_2, new TextEmbeddingQueryVectorBuilder(null, "c"), 10, 100, 10f, null, null),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            IllegalArgumentException.class,
            "[model_id] must not be null.",
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
                    new TextEmbeddingQueryVectorBuilder(null, "   "),
                    10,
                    100,
                    10f,
                    null,
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
                    new TextEmbeddingQueryVectorBuilder(null, "   "),
                    10,
                    100,
                    10f,
                    null,
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
                    new TextEmbeddingQueryVectorBuilder(LOCAL_INFERENCE_ID, "   "),
                    10,
                    100,
                    10f,
                    null,
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
                    new TextEmbeddingQueryVectorBuilder(LOCAL_INFERENCE_ID, "   "),
                    10,
                    100,
                    10f,
                    null,
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
                    new TextEmbeddingQueryVectorBuilder(COMMON_INFERENCE_ID, "   "),
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
        }
    }

    private void knnQueryBaseTestCases(boolean ccsMinimizeRoundTrips) throws Exception {
        final Consumer<SearchRequest> searchRequestModifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundTrips);
        final String expectedLocalClusterAlias = getExpectedLocalClusterAlias(ccsMinimizeRoundTrips);

        // Query a field that has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new KnnVectorQueryBuilder(COMMON_INFERENCE_ID_FIELD, new TextEmbeddingQueryVectorBuilder(null, "a"), 10, 100, 10f, null, null),
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
                new TextEmbeddingQueryVectorBuilder(null, "b"),
                10,
                100,
                10f,
                null,
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
                new TextEmbeddingQueryVectorBuilder(LOCAL_INFERENCE_ID, "z"),
                10,
                100,
                10f,
                null,
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
                new TextEmbeddingQueryVectorBuilder(LOCAL_INFERENCE_ID, "c"),
                10,
                100,
                10f,
                null,
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
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_1, queryVector, 10, 100, 10f, null, null, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
            ),
            null,
            searchRequestModifier
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_2, queryVector, 10, 100, 10f, null, null, null),
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
            new KnnVectorQueryBuilder(COMMON_INFERENCE_ID_FIELD, new TextEmbeddingQueryVectorBuilder(null, "a"), 10, 100, 10f, null, null),
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
                null,
                null
            ),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(DENSE_VECTOR_FIELD))),
            null,
            searchRequestModifier
        );
    }

    private void configureClusters() throws Exception {
        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(
                COMMON_INFERENCE_ID,
                textEmbeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                LOCAL_INFERENCE_ID,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(COMMON_INFERENCE_ID),
                VARIABLE_INFERENCE_ID_FIELD,
                semanticTextMapping(LOCAL_INFERENCE_ID),
                MIXED_TYPE_FIELD_1,
                denseVectorMapping(384),
                MIXED_TYPE_FIELD_2,
                semanticTextMapping(LOCAL_INFERENCE_ID),
                DENSE_VECTOR_FIELD,
                denseVectorMapping(DENSE_VECTOR_FIELD_DIMENSIONS)
            ),
            Map.of(
                getDocId(COMMON_INFERENCE_ID_FIELD),
                Map.of(COMMON_INFERENCE_ID_FIELD, "a"),
                getDocId(VARIABLE_INFERENCE_ID_FIELD),
                Map.of(VARIABLE_INFERENCE_ID_FIELD, "b"),
                getDocId(MIXED_TYPE_FIELD_1),
                Map.of(MIXED_TYPE_FIELD_1, generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)),
                getDocId(MIXED_TYPE_FIELD_2),
                Map.of(MIXED_TYPE_FIELD_2, "c"),
                getDocId(DENSE_VECTOR_FIELD),
                Map.of(
                    DENSE_VECTOR_FIELD,
                    generateDenseVectorFieldValue(DENSE_VECTOR_FIELD_DIMENSIONS, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f)
                )
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            REMOTE_INDEX_NAME,
            Map.of(
                COMMON_INFERENCE_ID,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                REMOTE_INFERENCE_ID,
                textEmbeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(COMMON_INFERENCE_ID),
                VARIABLE_INFERENCE_ID_FIELD,
                semanticTextMapping(REMOTE_INFERENCE_ID),
                MIXED_TYPE_FIELD_1,
                semanticTextMapping(COMMON_INFERENCE_ID),
                MIXED_TYPE_FIELD_2,
                denseVectorMapping(384),
                DENSE_VECTOR_FIELD,
                denseVectorMapping(DENSE_VECTOR_FIELD_DIMENSIONS)
            ),
            Map.of(
                getDocId(COMMON_INFERENCE_ID_FIELD),
                Map.of(COMMON_INFERENCE_ID_FIELD, "x"),
                getDocId(VARIABLE_INFERENCE_ID_FIELD),
                Map.of(VARIABLE_INFERENCE_ID_FIELD, "y"),
                getDocId(MIXED_TYPE_FIELD_1),
                Map.of(MIXED_TYPE_FIELD_1, "z"),
                getDocId(MIXED_TYPE_FIELD_2),
                Map.of(MIXED_TYPE_FIELD_2, generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)),
                getDocId(DENSE_VECTOR_FIELD),
                Map.of(
                    DENSE_VECTOR_FIELD,
                    generateDenseVectorFieldValue(DENSE_VECTOR_FIELD_DIMENSIONS, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)
                )
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);
    }

    private static String getDocId(String field) {
        return field + "_doc";
    }
}
