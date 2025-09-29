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
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class MatchQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String LOCAL_INDEX_NAME = "local-index";
    private static final String REMOTE_INDEX_NAME = "remote-index";

    // Boost the local index so that we can use the same doc values for local and remote indices and have consistent relevance
    private static final List<IndexWithBoost> QUERY_INDICES = List.of(
        new IndexWithBoost(LOCAL_INDEX_NAME, 10.0f),
        new IndexWithBoost(fullyQualifiedIndexName(REMOTE_CLUSTER, REMOTE_INDEX_NAME))
    );

    private static final String COMMON_INFERENCE_ID_FIELD = "common-inference-id-field";
    private static final String VARIABLE_INFERENCE_ID_FIELD = "variable-inference-id-field";
    private static final String MIXED_TYPE_FIELD_1 = "mixed-type-field-1";
    private static final String MIXED_TYPE_FIELD_2 = "mixed-type-field-2";
    private static final String TEXT_FIELD = "text-field";

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

    public void testMatchQuery() throws Exception {
        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new MatchQueryBuilder(COMMON_INFERENCE_ID_FIELD, "a"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
            )
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new MatchQueryBuilder(VARIABLE_INFERENCE_ID_FIELD, "b"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD))
            )
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new MatchQueryBuilder(MIXED_TYPE_FIELD_1, "c"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
            )
        );
        assertSearchResponse(
            new MatchQueryBuilder(MIXED_TYPE_FIELD_2, "d"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
            )
        );
    }

    public void testMatchQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
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
                    "match query does not support cross-cluster search when querying a [semantic_text] field when "
                        + "[ccs_minimize_roundtrips] is false"
                )
            );
        };

        // Validate that expected cases fail
        assertCcsMinimizeRoundTripsFalseFailure.accept(new MatchQueryBuilder(COMMON_INFERENCE_ID_FIELD, randomAlphaOfLength(5)));
        assertCcsMinimizeRoundTripsFalseFailure.accept(new MatchQueryBuilder(MIXED_TYPE_FIELD_1, randomAlphaOfLength(5)));

        // Validate the expected ccs_minimize_roundtrips=false detection gap and failure mode when querying non-inference fields locally
        assertSearchResponse(
            new MatchQueryBuilder(MIXED_TYPE_FIELD_2, "d"),
            QUERY_INDICES,
            List.of(new SearchResult(null, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))),
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
            new MatchQueryBuilder(TEXT_FIELD, "e"),
            QUERY_INDICES,
            List.of(
                new SearchResult(null, LOCAL_INDEX_NAME, getDocId(TEXT_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(TEXT_FIELD))
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );
    }

    private void configureClusters() throws Exception {
        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";
        final String remoteInferenceId = "remote-inference-id";

        final Map<String, Map<String, Object>> docs = Map.of(
            getDocId(COMMON_INFERENCE_ID_FIELD),
            Map.of(COMMON_INFERENCE_ID_FIELD, "a"),
            getDocId(VARIABLE_INFERENCE_ID_FIELD),
            Map.of(VARIABLE_INFERENCE_ID_FIELD, "b"),
            getDocId(MIXED_TYPE_FIELD_1),
            Map.of(MIXED_TYPE_FIELD_1, "c"),
            getDocId(MIXED_TYPE_FIELD_2),
            Map.of(MIXED_TYPE_FIELD_2, "d"),
            getDocId(TEXT_FIELD),
            Map.of(TEXT_FIELD, "e")
        );

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings(), localInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(commonInferenceId),
                VARIABLE_INFERENCE_ID_FIELD,
                semanticTextMapping(localInferenceId),
                MIXED_TYPE_FIELD_1,
                semanticTextMapping(localInferenceId),
                MIXED_TYPE_FIELD_2,
                textMapping(),
                TEXT_FIELD,
                textMapping()
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
                semanticTextMapping(remoteInferenceId),
                MIXED_TYPE_FIELD_1,
                textMapping(),
                MIXED_TYPE_FIELD_2,
                semanticTextMapping(remoteInferenceId),
                TEXT_FIELD,
                textMapping()
            ),
            docs
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);
    }

    private static String getDocId(String field) {
        return field + "_doc";
    }
}
