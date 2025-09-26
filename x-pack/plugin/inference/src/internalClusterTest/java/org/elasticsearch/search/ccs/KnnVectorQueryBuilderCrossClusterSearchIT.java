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
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.equalTo;

public class KnnVectorQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String LOCAL_INDEX_NAME = "local-index";
    private static final String REMOTE_INDEX_NAME = "remote-index";
    private static final List<IndexWithBoost> QUERY_INDICES = List.of(
        new IndexWithBoost(LOCAL_INDEX_NAME),
        new IndexWithBoost(fullyQualifiedIndexName(REMOTE_CLUSTER, REMOTE_INDEX_NAME))
    );

    public void testKnnQuery() throws Exception {
        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(
                commonInferenceId,
                textEmbeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                localInferenceId,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                denseVectorMapping(384),
                mixedTypeField2,
                semanticTextMapping(localInferenceId)
            ),
            Map.of(
                "local_doc_1",
                Map.of(commonInferenceIdField, "a"),
                "local_doc_2",
                Map.of(mixedTypeField1, generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)),
                "local_doc_3",
                Map.of(mixedTypeField2, "c")
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            REMOTE_INDEX_NAME,
            Map.of(
                commonInferenceId,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                semanticTextMapping(commonInferenceId),
                mixedTypeField2,
                denseVectorMapping(384)
            ),
            Map.of(
                "remote_doc_1",
                Map.of(commonInferenceIdField, "x"),
                "remote_doc_2",
                Map.of(mixedTypeField1, "y"),
                "remote_doc_3",
                Map.of(mixedTypeField2, generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f))
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new KnnVectorQueryBuilder(commonInferenceIdField, new TextEmbeddingQueryVectorBuilder(null, "a"), 10, 100, 10f, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_1"),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_1")
            )
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new KnnVectorQueryBuilder(mixedTypeField1, new TextEmbeddingQueryVectorBuilder(localInferenceId, "y"), 10, 100, 10f, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_2"),
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_2")
            )
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(mixedTypeField2, new TextEmbeddingQueryVectorBuilder(localInferenceId, "c"), 10, 100, 10f, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_3"),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_3")
            )
        );

        // Query a field that has mixed types across clusters using a query vector
        final VectorData queryVector = new VectorData(
            generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(mixedTypeField1, queryVector, 10, 100, 10f, null, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_2"),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_2")
            )
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(mixedTypeField2, queryVector, 10, 100, 10f, null, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_3"),
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_3")
            )
        );

        // Check that omitting the inference ID when querying a remote dense vector field leads to the expected partial failure
        assertSearchResponse(
            new KnnVectorQueryBuilder(mixedTypeField2, new TextEmbeddingQueryVectorBuilder(null, "c"), 10, 100, 10f, null),
            QUERY_INDICES,
            List.of(new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_3")),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(new FailureCause(IllegalArgumentException.class, "[model_id] must not be null."))
            ),
            null
        );
    }

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

        final int dimensions = 256;
        final String commonInferenceId = "common-inference-id";
        final MinimalServiceSettings commonInferenceIdServiceSettings = textEmbeddingServiceSettings(
            dimensions,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT
        );

        final String commonInferenceIdField = "common-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";
        final String denseVectorField = "dense-vector-field";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(commonInferenceId, commonInferenceIdServiceSettings),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                semanticTextMapping(commonInferenceId),
                mixedTypeField2,
                denseVectorMapping(dimensions),
                denseVectorField,
                denseVectorMapping(dimensions)
            ),
            Map.of(
                mixedTypeField2 + "_doc",
                Map.of(mixedTypeField2, generateDenseVectorFieldValue(dimensions, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)),
                denseVectorField + "_doc",
                Map.of(denseVectorField, generateDenseVectorFieldValue(dimensions, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f))
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            REMOTE_INDEX_NAME,
            Map.of(commonInferenceId, commonInferenceIdServiceSettings),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                denseVectorMapping(dimensions),
                mixedTypeField2,
                semanticTextMapping(commonInferenceId),
                denseVectorField,
                denseVectorMapping(dimensions)
            ),
            Map.of(
                mixedTypeField2 + "_doc",
                Map.of(mixedTypeField2, "a"),
                denseVectorField + "_doc",
                Map.of(denseVectorField, generateDenseVectorFieldValue(dimensions, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f))
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Validate that expected cases fail
        assertCcsMinimizeRoundTripsFalseFailure.accept(
            commonInferenceIdField,
            new TextEmbeddingQueryVectorBuilder(null, randomAlphaOfLength(5))
        );
        assertCcsMinimizeRoundTripsFalseFailure.accept(
            mixedTypeField1,
            new TextEmbeddingQueryVectorBuilder(commonInferenceId, randomAlphaOfLength(5))
        );

        // Validate the expected ccs_minimize_roundtrips=false detection gap and failure mode when querying non-inference fields locally
        assertSearchResponse(
            new KnnVectorQueryBuilder(mixedTypeField2, new TextEmbeddingQueryVectorBuilder(commonInferenceId, "foo"), 10, 100, 10f, null),
            QUERY_INDICES,
            List.of(new SearchResult(null, LOCAL_INDEX_NAME, mixedTypeField2 + "_doc")),
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
                denseVectorField,
                generateDenseVectorFieldValue(dimensions, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f),
                10,
                100,
                10f,
                null,
                null
            ),
            QUERY_INDICES,
            List.of(
                new SearchResult(null, LOCAL_INDEX_NAME, denseVectorField + "_doc"),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, denseVectorField + "_doc")
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );
    }
}
