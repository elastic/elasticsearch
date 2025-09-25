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
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class MatchQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String LOCAL_INDEX_NAME = "local-index";
    private static final String REMOTE_INDEX_NAME = "remote-index";
    private static final String[] QUERY_INDICES = new String[] {
        LOCAL_INDEX_NAME,
        fullyQualifiedIndexName(REMOTE_CLUSTER, REMOTE_INDEX_NAME) };

    public void testMatchQuery() throws Exception {
        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";
        final String remoteInferenceId = "remote-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String variableInferenceIdField = "variable-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings(), localInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                variableInferenceIdField,
                semanticTextMapping(localInferenceId),
                mixedTypeField1,
                semanticTextMapping(localInferenceId),
                mixedTypeField2,
                textMapping()
            ),
            Map.of(
                "local_doc_1",
                Map.of(commonInferenceIdField, "a"),
                "local_doc_2",
                Map.of(variableInferenceIdField, "b"),
                "local_doc_3",
                Map.of(mixedTypeField1, "c"),
                "local_doc_4",
                Map.of(mixedTypeField2, "d")
            )
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
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                variableInferenceIdField,
                semanticTextMapping(remoteInferenceId),
                mixedTypeField1,
                textMapping(),
                mixedTypeField2,
                semanticTextMapping(remoteInferenceId)
            ),
            Map.of(
                "remote_doc_1",
                Map.of(commonInferenceIdField, "w"),
                "remote_doc_2",
                Map.of(variableInferenceIdField, "x"),
                "remote_doc_3",
                Map.of(mixedTypeField1, "y"),
                "remote_doc_4",
                Map.of(mixedTypeField2, "z")
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new MatchQueryBuilder(commonInferenceIdField, "a"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_1"),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_1")
            )
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new MatchQueryBuilder(variableInferenceIdField, "b"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_2"),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_2")
            )
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new MatchQueryBuilder(mixedTypeField1, "y"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_3"),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_3")
            )
        );
        assertSearchResponse(
            new MatchQueryBuilder(mixedTypeField2, "d"),
            QUERY_INDICES,
            List.of(
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, "remote_doc_4"),
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, "local_doc_4")
            )
        );
    }

    public void testMatchQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
        final Consumer<QueryBuilder> assertCcsMinimizeRoundTripsFalseFailure = q -> {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(q);
            SearchRequest searchRequest = new SearchRequest(QUERY_INDICES, searchSourceBuilder);
            searchRequest.setCcsMinimizeRoundtrips(false);

            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> client().search(searchRequest).actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(
                e.getMessage(),
                equalTo(
                    "match query does not support cross-cluster search when querying a [semantic_text] field when "
                        + "[ccs_minimize_roundtrips] is false"
                )
            );
        };

        final String commonInferenceId = "common-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";
        final String textField = "text-field";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                semanticTextMapping(commonInferenceId),
                mixedTypeField2,
                textMapping(),
                textField,
                textMapping()
            ),
            Map.of(mixedTypeField2 + "_doc", Map.of(mixedTypeField2, "a"), textField + "_doc", Map.of(textField, "b b b"))
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            REMOTE_INDEX_NAME,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                textMapping(),
                mixedTypeField2,
                semanticTextMapping(commonInferenceId),
                textField,
                textMapping()
            ),
            Map.of(textField + "_doc", Map.of(textField, "b"))
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Validate that expected cases fail
        assertCcsMinimizeRoundTripsFalseFailure.accept(new MatchQueryBuilder(commonInferenceIdField, randomAlphaOfLength(5)));
        assertCcsMinimizeRoundTripsFalseFailure.accept(new MatchQueryBuilder(mixedTypeField1, randomAlphaOfLength(5)));

        // Validate the expected ccs_minimize_roundtrips=false detection gap and failure mode when querying non-inference fields locally
        assertSearchResponse(
            new MatchQueryBuilder(mixedTypeField2, "a"),
            QUERY_INDICES,
            List.of(new SearchResult(null, LOCAL_INDEX_NAME, mixedTypeField2 + "_doc")),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(
                    new FailureCause(
                        QueryShardException.class,
                        "failed to create query: Field [mixed-type-field-2] of type [semantic_text] does not support match queries"
                    )
                )
            ),
            s -> s.setCcsMinimizeRoundtrips(false)
        );

        // Validate that a CCS match query functions when only text fields are queried
        assertSearchResponse(
            new MatchQueryBuilder(textField, "b"),
            QUERY_INDICES,
            List.of(
                new SearchResult(null, LOCAL_INDEX_NAME, textField + "_doc"),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, textField + "_doc")
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );
    }
}
