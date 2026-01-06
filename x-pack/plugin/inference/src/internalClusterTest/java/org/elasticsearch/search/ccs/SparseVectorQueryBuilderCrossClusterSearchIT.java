/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class SparseVectorQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String COMMON_INFERENCE_ID_FIELD = "common-inference-id-field";
    private static final String VARIABLE_INFERENCE_ID_FIELD = "variable-inference-id-field";
    private static final String MIXED_TYPE_FIELD_1 = "mixed-type-field-1";
    private static final String MIXED_TYPE_FIELD_2 = "mixed-type-field-2";
    private static final String SPARSE_VECTOR_FIELD = "sparse-vector-field";

    private static final String COMMON_INFERENCE_ID = "common-inference-id";
    private static final String LOCAL_INFERENCE_ID = "local-inference-id";
    private static final String REMOTE_INFERENCE_ID = "remote-inference-id";

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

    public void testSparseVectorQueryWithCcsMinimizeRoundTripsTrue() throws Exception {
        sparseVectorQueryBaseTestCases(true);

        // Check that omitting the inference ID when querying a remote sparse vector field leads to the expected partial failure
        assertSearchResponse(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_2, null, "c"),
            QUERY_INDICES,
            List.of(new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(new FailureCause(IllegalArgumentException.class, "inference_id required to perform vector search on query string"))
            ),
            null
        );
    }

    public void testSparseVectorQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
        sparseVectorQueryBaseTestCases(false);

        // Query an inference field on a remote cluster
        assertSearchResponse(
            new SparseVectorQueryBuilder(COMMON_INFERENCE_ID_FIELD, null, "a"),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );

        // Check that omitting the inference ID when querying a remote sparse vector field leads to the expected failure
        assertSearchFailure(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_2, null, "c"),
            QUERY_INDICES,
            IllegalArgumentException.class,
            "inference_id required to perform vector search on query string",
            s -> s.setCcsMinimizeRoundtrips(false)
        );
        assertSearchFailure(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_2, null, "c"),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            IllegalArgumentException.class,
            "inference_id required to perform vector search on query string",
            s -> s.setCcsMinimizeRoundtrips(false)
        );
    }

    public void testBlankQueryHandling() throws Exception {
        List<Boolean> ccsMinimizeRoundTripsValues = List.of(true, false);
        for (Boolean ccsMinimizeRoundTrips : ccsMinimizeRoundTripsValues) {
            final Consumer<SearchRequest> searchRequestModifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundTrips);
            final String expectedLocalClusterAlias = getExpectedLocalClusterAlias(ccsMinimizeRoundTrips);

            assertSearchResponse(
                new SparseVectorQueryBuilder(COMMON_INFERENCE_ID_FIELD, null, "   "),
                QUERY_INDICES,
                List.of(
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
                ),
                null,
                searchRequestModifier
            );

            assertSearchResponse(
                new SparseVectorQueryBuilder(VARIABLE_INFERENCE_ID_FIELD, null, "   "),
                QUERY_INDICES,
                List.of(
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD)),
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD))
                ),
                null,
                searchRequestModifier
            );

            assertSearchResponse(
                new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_1, COMMON_INFERENCE_ID, "   "),
                QUERY_INDICES,
                List.of(
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
                ),
                null,
                searchRequestModifier
            );

            assertSearchResponse(
                new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_2, COMMON_INFERENCE_ID, "   "),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
                ),
                null,
                searchRequestModifier
            );

            assertSearchResponse(
                new SparseVectorQueryBuilder(SPARSE_VECTOR_FIELD, COMMON_INFERENCE_ID, "   "),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(SPARSE_VECTOR_FIELD)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(SPARSE_VECTOR_FIELD))
                ),
                null,
                searchRequestModifier
            );
        }
    }

    private void sparseVectorQueryBaseTestCases(boolean ccsMinimizeRoundTrips) throws Exception {
        final Consumer<SearchRequest> searchRequestModifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundTrips);
        final String expectedLocalClusterAlias = ccsMinimizeRoundTrips ? LOCAL_CLUSTER : null;

        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new SparseVectorQueryBuilder(COMMON_INFERENCE_ID_FIELD, null, "a"),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
            ),
            null,
            searchRequestModifier
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new SparseVectorQueryBuilder(VARIABLE_INFERENCE_ID_FIELD, null, "b"),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD)),
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD))
            ),
            null,
            searchRequestModifier
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_1, COMMON_INFERENCE_ID, "b"),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
            ),
            null,
            searchRequestModifier
        );
        assertSearchResponse(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_2, COMMON_INFERENCE_ID, "c"),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
            ),
            null,
            searchRequestModifier
        );

        // Query a field that has mixed types across clusters using a query vector
        final List<WeightedToken> queryVector = generateSparseVectorFieldValue(1.0f).entrySet()
            .stream()
            .map(e -> new WeightedToken(e.getKey(), e.getValue()))
            .toList();
        assertSearchResponse(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_1, queryVector, null, null, null, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
            ),
            null,
            searchRequestModifier
        );
        assertSearchResponse(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_2, queryVector, null, null, null, null),
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
            new SparseVectorQueryBuilder(COMMON_INFERENCE_ID_FIELD, null, "a"),
            List.of("local-*", fullyQualifiedIndexName("cluster_*", "remote-*")),
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
            ),
            null,
            searchRequestModifier
        );

        // Validate that a CCS sparse vector query functions when only sparse vector fields are queried
        assertSearchResponse(
            new SparseVectorQueryBuilder(SPARSE_VECTOR_FIELD, COMMON_INFERENCE_ID, "foo"),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(SPARSE_VECTOR_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(SPARSE_VECTOR_FIELD))
            ),
            null,
            searchRequestModifier
        );
        assertSearchResponse(
            new SparseVectorQueryBuilder(SPARSE_VECTOR_FIELD, COMMON_INFERENCE_ID, "foo"),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(SPARSE_VECTOR_FIELD))),
            null,
            searchRequestModifier
        );
    }

    private void configureClusters() throws Exception {
        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(COMMON_INFERENCE_ID, sparseEmbeddingServiceSettings(), LOCAL_INFERENCE_ID, sparseEmbeddingServiceSettings()),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(COMMON_INFERENCE_ID),
                VARIABLE_INFERENCE_ID_FIELD,
                semanticTextMapping(LOCAL_INFERENCE_ID),
                MIXED_TYPE_FIELD_1,
                sparseVectorMapping(),
                MIXED_TYPE_FIELD_2,
                semanticTextMapping(COMMON_INFERENCE_ID),
                SPARSE_VECTOR_FIELD,
                sparseVectorMapping()
            ),
            Map.of(
                getDocId(COMMON_INFERENCE_ID_FIELD),
                Map.of(COMMON_INFERENCE_ID_FIELD, "a"),
                getDocId(VARIABLE_INFERENCE_ID_FIELD),
                Map.of(VARIABLE_INFERENCE_ID_FIELD, "b"),
                getDocId(MIXED_TYPE_FIELD_1),
                Map.of(MIXED_TYPE_FIELD_1, generateSparseVectorFieldValue(1.0f)),
                getDocId(MIXED_TYPE_FIELD_2),
                Map.of(MIXED_TYPE_FIELD_2, "c"),
                getDocId(SPARSE_VECTOR_FIELD),
                Map.of(SPARSE_VECTOR_FIELD, generateSparseVectorFieldValue(1.0f))
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            REMOTE_INDEX_NAME,
            Map.of(COMMON_INFERENCE_ID, sparseEmbeddingServiceSettings(), REMOTE_INFERENCE_ID, sparseEmbeddingServiceSettings()),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(COMMON_INFERENCE_ID),
                VARIABLE_INFERENCE_ID_FIELD,
                semanticTextMapping(REMOTE_INFERENCE_ID),
                MIXED_TYPE_FIELD_1,
                semanticTextMapping(COMMON_INFERENCE_ID),
                MIXED_TYPE_FIELD_2,
                sparseVectorMapping(),
                SPARSE_VECTOR_FIELD,
                sparseVectorMapping()
            ),
            Map.of(
                getDocId(COMMON_INFERENCE_ID_FIELD),
                Map.of(COMMON_INFERENCE_ID_FIELD, "x"),
                getDocId(VARIABLE_INFERENCE_ID_FIELD),
                Map.of(VARIABLE_INFERENCE_ID_FIELD, "y"),
                getDocId(MIXED_TYPE_FIELD_1),
                Map.of(MIXED_TYPE_FIELD_1, "z"),
                getDocId(MIXED_TYPE_FIELD_2),
                Map.of(MIXED_TYPE_FIELD_2, generateSparseVectorFieldValue(1.0f)),
                getDocId(SPARSE_VECTOR_FIELD),
                Map.of(SPARSE_VECTOR_FIELD, generateSparseVectorFieldValue(0.5f))
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);
    }

    private static String getDocId(String field) {
        return field + "_doc";
    }
}
