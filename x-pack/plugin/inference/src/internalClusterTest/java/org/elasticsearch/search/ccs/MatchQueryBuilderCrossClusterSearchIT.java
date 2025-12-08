/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.inference.SimilarityMeasure;
import org.junit.Before;

import java.util.List;
import java.util.Map;

public class MatchQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
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

    public void testMatchQueryWithCcsMinimizeRoundTripsTrue() throws Exception {
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

        // Query an inference field on a remote cluster
        assertSearchResponse(
            new MatchQueryBuilder(COMMON_INFERENCE_ID_FIELD, "a"),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)))
        );

        // Validate that a CCS match query functions when only text fields are queried
        assertSearchResponse(
            new MatchQueryBuilder(TEXT_FIELD, "e"),
            QUERY_INDICES,
            List.of(
                new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, getDocId(TEXT_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(TEXT_FIELD))
            )
        );
        assertSearchResponse(
            new MatchQueryBuilder(TEXT_FIELD, "e"),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(TEXT_FIELD)))
        );
    }

    public void testMatchQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new MatchQueryBuilder(COMMON_INFERENCE_ID_FIELD, "a"),
            QUERY_INDICES,
            List.of(
                new SearchResult(null, LOCAL_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new MatchQueryBuilder(VARIABLE_INFERENCE_ID_FIELD, "b"),
            QUERY_INDICES,
            List.of(
                new SearchResult(null, LOCAL_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(VARIABLE_INFERENCE_ID_FIELD))
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new MatchQueryBuilder(MIXED_TYPE_FIELD_1, "c"),
            QUERY_INDICES,
            List.of(
                new SearchResult(null, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_1))
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );
        assertSearchResponse(
            new MatchQueryBuilder(MIXED_TYPE_FIELD_2, "d"),
            QUERY_INDICES,
            List.of(
                new SearchResult(null, LOCAL_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(MIXED_TYPE_FIELD_2))
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );

        // Query an inference field on a remote cluster
        assertSearchResponse(
            new MatchQueryBuilder(COMMON_INFERENCE_ID_FIELD, "a"),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(COMMON_INFERENCE_ID_FIELD))),
            null,
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
        assertSearchResponse(
            new MatchQueryBuilder(TEXT_FIELD, "e"),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(TEXT_FIELD))),
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
