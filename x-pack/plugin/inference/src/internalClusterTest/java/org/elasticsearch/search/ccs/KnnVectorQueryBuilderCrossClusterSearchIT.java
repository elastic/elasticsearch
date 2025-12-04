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
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.equalTo;

public class KnnVectorQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String LOCAL_INDEX_NAME = "local-index";
    private static final String REMOTE_INDEX_NAME = "remote-index";
    private static final String FULLY_QUALIFIED_REMOTE_INDEX_NAME = fullyQualifiedIndexName(REMOTE_CLUSTER, REMOTE_INDEX_NAME);

    private static final List<IndexWithBoost> QUERY_INDICES = List.of(
        new IndexWithBoost(LOCAL_INDEX_NAME),
        new IndexWithBoost(FULLY_QUALIFIED_REMOTE_INDEX_NAME)
    );

    private static final String COMMON_INFERENCE_ID_FIELD = "common-inference-id-field";
    private static final String MIXED_TYPE_FIELD_1 = "mixed-type-field-1";
    private static final String MIXED_TYPE_FIELD_2 = "mixed-type-field-2";
    private static final String DENSE_VECTOR_FIELD = "dense-vector-field";

    private static final String COMMON_INFERENCE_ID = "common-inference-id";
    private static final String LOCAL_INFERENCE_ID = "local-inference-id";

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

    public void testKnnQuery() throws Exception {
        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new KnnVectorQueryBuilder(COMMON_INFERENCE_ID_FIELD, new TextEmbeddingQueryVectorBuilder(null, "a"), 10, 100, 10f, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
            )
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_1, new TextEmbeddingQueryVectorBuilder(LOCAL_INFERENCE_ID, "y"), 10, 100, 10f, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
            )
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_2, new TextEmbeddingQueryVectorBuilder(LOCAL_INFERENCE_ID, "c"), 10, 100, 10f, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
            )
        );

        // Query a field that has mixed types across clusters using a query vector
        final VectorData queryVector = new VectorData(
            generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_1, queryVector, 10, 100, 10f, null, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
            )
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_2, queryVector, 10, 100, 10f, null, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
            )
        );

        // Check that omitting the inference ID when querying a remote dense vector field leads to the expected partial failure
        assertSearchResponse(
            new KnnVectorQueryBuilder(MIXED_TYPE_FIELD_2, new TextEmbeddingQueryVectorBuilder(null, "c"), 10, 100, 10f, null),
            QUERY_INDICES,
            List.of(new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(new FailureCause(IllegalArgumentException.class, "[model_id] must not be null."))
            ),
            null
        );
    }

    // TODO: Adjust test
    @AwaitsFix(bugUrl = "http://fake.url")
    public void testKnnQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
        final BiConsumer<String, TextEmbeddingQueryVectorBuilder> assertCcsMinimizeRoundTripsFalseFailure = (f, qvb) -> {
            KnnVectorQueryBuilder queryBuilder = new KnnVectorQueryBuilder(f, qvb, 10, 100, 10f, null);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);
            SearchRequest searchRequest = new SearchRequest(convertToArray(QUERY_INDICES), searchSourceBuilder);
            searchRequest.setCcsMinimizeRoundtrips(false);

            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> client().search(searchRequest).actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(
                e.getMessage(),
                equalTo(
                    "knn query does not support cross-cluster search when querying a [semantic_text] field when "
                        + "[ccs_minimize_roundtrips] is false"
                )
            );
        };

        // Validate that expected cases fail
        assertCcsMinimizeRoundTripsFalseFailure.accept(
            COMMON_INFERENCE_ID_FIELD,
            new TextEmbeddingQueryVectorBuilder(null, randomAlphaOfLength(5))
        );
        assertCcsMinimizeRoundTripsFalseFailure.accept(
            MIXED_TYPE_FIELD_1,
            new TextEmbeddingQueryVectorBuilder(COMMON_INFERENCE_ID, randomAlphaOfLength(5))
        );

        // Validate the expected ccs_minimize_roundtrips=false detection gap and failure mode when querying non-inference fields locally
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                MIXED_TYPE_FIELD_2,
                new TextEmbeddingQueryVectorBuilder(COMMON_INFERENCE_ID, "foo"),
                10,
                100,
                10f,
                null
            ),
            QUERY_INDICES,
            List.of(new SearchResult(null, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(
                    new FailureCause(
                        QueryShardException.class,
                        "failed to create query: [knn] queries are only supported on [dense_vector] fields"
                    )
                )
            ),
            s -> s.setCcsMinimizeRoundtrips(false)
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
                new SearchResult(null, LOCAL_INDEX_NAME, getDocId(DENSE_VECTOR_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(DENSE_VECTOR_FIELD))
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
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
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(COMMON_INFERENCE_ID),
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
                getDocId(MIXED_TYPE_FIELD_1),
                Map.of(MIXED_TYPE_FIELD_1, "y"),
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
