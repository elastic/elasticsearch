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

    public void testSparseVectorQuery() throws Exception {
        final String commonInferenceId = "common-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                sparseVectorMapping(),
                mixedTypeField2,
                semanticTextMapping(commonInferenceId)
            ),
            Map.of(
                "local_doc_1",
                Map.of(commonInferenceIdField, "a"),
                "local_doc_2",
                Map.of(mixedTypeField1, generateSparseVectorFieldValue(1.0f)),
                "local_doc_3",
                Map.of(mixedTypeField2, "c")
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            REMOTE_INDEX_NAME,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                semanticTextMapping(commonInferenceId),
                mixedTypeField2,
                sparseVectorMapping()
            ),
            Map.of(
                "remote_doc_1",
                Map.of(commonInferenceIdField, "x"),
                "remote_doc_2",
                Map.of(mixedTypeField1, "y"),
                "remote_doc_3",
                Map.of(mixedTypeField2, generateSparseVectorFieldValue(1.0f))
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new SparseVectorQueryBuilder(commonInferenceIdField, null, "a"),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_1"),
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_1")
            )
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField1, commonInferenceId, "b"),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_2"),
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_2")
            )
        );
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField2, commonInferenceId, "c"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_3"),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_3")
            )
        );

        // Query a field that has mixed types across clusters using a query vector
        final List<WeightedToken> queryVector = generateSparseVectorFieldValue(1.0f).entrySet()
            .stream()
            .map(e -> new WeightedToken(e.getKey(), e.getValue()))
            .toList();
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField1, queryVector, null, null, null, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_2"),
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_2")
            )
        );
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField2, queryVector, null, null, null, null),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_3"),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_3")
            )
        );

        // Check that omitting the inference ID when querying a remote sparse vector field leads to the expected partial failure
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField2, null, "c"),
            QUERY_INDICES,
            List.of(new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_3")),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(new FailureCause(IllegalArgumentException.class, "inference_id required to perform vector search on query string"))
            ),
            null
        );
    }

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

        final String commonInferenceId = "common-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";
        final String sparseVectorField = "sparse-vector-field";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                semanticTextMapping(commonInferenceId),
                mixedTypeField2,
                sparseVectorMapping(),
                sparseVectorField,
                sparseVectorMapping()
            ),
            Map.of(
                mixedTypeField2 + "_doc",
                Map.of(mixedTypeField2, generateSparseVectorFieldValue(1.0f)),
                sparseVectorField + "_doc",
                Map.of(sparseVectorField, generateSparseVectorFieldValue(1.0f))
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            REMOTE_INDEX_NAME,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                sparseVectorMapping(),
                mixedTypeField2,
                semanticTextMapping(commonInferenceId),
                sparseVectorField,
                sparseVectorMapping()
            ),
            Map.of(
                mixedTypeField2 + "_doc",
                Map.of(mixedTypeField2, "a"),
                sparseVectorField + "_doc",
                Map.of(sparseVectorField, generateSparseVectorFieldValue(0.5f))
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Validate that expected cases fail
        assertCcsMinimizeRoundTripsFalseFailure.accept(new SparseVectorQueryBuilder(commonInferenceIdField, null, randomAlphaOfLength(5)));
        assertCcsMinimizeRoundTripsFalseFailure.accept(
            new SparseVectorQueryBuilder(mixedTypeField1, commonInferenceId, randomAlphaOfLength(5))
        );

        // Validate the expected ccs_minimize_roundtrips=false detection gap and failure mode when querying non-inference fields locally
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField2, commonInferenceId, "foo"),
            QUERY_INDICES,
            List.of(new SearchResult(null, LOCAL_INDEX_NAME, mixedTypeField2 + "_doc")),
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
            new SparseVectorQueryBuilder(sparseVectorField, commonInferenceId, "foo"),
            QUERY_INDICES,
            List.of(
                new SearchResult(null, LOCAL_INDEX_NAME, sparseVectorField + "_doc"),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, sparseVectorField + "_doc")
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );
    }
}
