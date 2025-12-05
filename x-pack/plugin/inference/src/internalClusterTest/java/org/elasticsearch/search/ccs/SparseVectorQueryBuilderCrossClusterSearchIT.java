/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class SparseVectorQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String LOCAL_INDEX_NAME = "local-index";
    private static final String REMOTE_INDEX_NAME = "remote-index";
    private static final List<IndexWithBoost> QUERY_INDICES = List.of(
        new IndexWithBoost(LOCAL_INDEX_NAME),
        new IndexWithBoost(fullyQualifiedIndexName(REMOTE_CLUSTER, REMOTE_INDEX_NAME))
    );

    private static final String COMMON_INFERENCE_ID_FIELD = "common-inference-id-field";
    private static final String MIXED_TYPE_FIELD_1 = "mixed-type-field-1";
    private static final String MIXED_TYPE_FIELD_2 = "mixed-type-field-2";
    private static final String SPARSE_VECTOR_FIELD = "sparse-vector-field";

    private static final String COMMON_INFERENCE_ID = "common-inference-id";

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

    public void testSparseVectorQuery() throws Exception {
        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new SparseVectorQueryBuilder(COMMON_INFERENCE_ID_FIELD, null, "a"),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
            )
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_1, COMMON_INFERENCE_ID, "b"),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
            )
        );
        assertSearchResponse(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_2, COMMON_INFERENCE_ID, "c"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
            )
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
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
            )
        );
        assertSearchResponse(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_2, queryVector, null, null, null, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
            )
        );

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

    // TODO: Adjust test
    @AwaitsFix(bugUrl = "http://fake.url")
    public void testSparseVectorQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
        final Consumer<QueryBuilder> assertCcsMinimizeRoundTripsFalseFailure = q -> {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(q);
            SearchRequest searchRequest = new SearchRequest(convertToArray(QUERY_INDICES), searchSourceBuilder);
            searchRequest.setCcsMinimizeRoundtrips(false);

            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> client().search(searchRequest).actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(
                e.getMessage(),
                equalTo(
                    "sparse_vector query does not support cross-cluster search when querying a [semantic_text] field when "
                        + "[ccs_minimize_roundtrips] is false"
                )
            );
        };

        // Validate that expected cases fail
        assertCcsMinimizeRoundTripsFalseFailure.accept(
            new SparseVectorQueryBuilder(COMMON_INFERENCE_ID_FIELD, null, randomAlphaOfLength(5))
        );
        assertCcsMinimizeRoundTripsFalseFailure.accept(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_1, COMMON_INFERENCE_ID, randomAlphaOfLength(5))
        );

        // Validate the expected ccs_minimize_roundtrips=false detection gap and failure mode when querying non-inference fields locally
        assertSearchResponse(
            new SparseVectorQueryBuilder(MIXED_TYPE_FIELD_2, COMMON_INFERENCE_ID, "foo"),
            QUERY_INDICES,
            List.of(new SearchResult(null, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(
                    new FailureCause(
                        QueryShardException.class,
                        "failed to create query: field [mixed-type-field-2] must be type [sparse_vector] but is type [semantic_text]"
                    )
                )
            ),
            s -> s.setCcsMinimizeRoundtrips(false)
        );

        // Validate that a CCS sparse vector query functions when only sparse vector fields are queried
        assertSearchResponse(
            new SparseVectorQueryBuilder(SPARSE_VECTOR_FIELD, COMMON_INFERENCE_ID, "foo"),
            QUERY_INDICES,
            List.of(
                new SearchResult(null, LOCAL_INDEX_NAME, getDocId(SPARSE_VECTOR_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(SPARSE_VECTOR_FIELD))
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );
    }

    private void configureClusters() throws Exception {
        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(COMMON_INFERENCE_ID, sparseEmbeddingServiceSettings()),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(COMMON_INFERENCE_ID),
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
            Map.of(COMMON_INFERENCE_ID, sparseEmbeddingServiceSettings()),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(COMMON_INFERENCE_ID),
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
                getDocId(MIXED_TYPE_FIELD_1),
                Map.of(MIXED_TYPE_FIELD_1, "y"),
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
