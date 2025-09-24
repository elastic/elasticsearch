/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class SemanticQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    public void testSemanticQuery() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };

        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";
        final String remoteInferenceId = "remote-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String variableInferenceIdField = "variable-inference-id-field";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            localIndexName,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings(), localInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                variableInferenceIdField,
                semanticTextMapping(localInferenceId)
            ),
            Map.of("local_doc_1", Map.of(commonInferenceIdField, "a"), "local_doc_2", Map.of(variableInferenceIdField, "b"))
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            remoteIndexName,
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
                semanticTextMapping(remoteInferenceId)
            ),
            Map.of("remote_doc_1", Map.of(commonInferenceIdField, "x"), "remote_doc_2", Map.of(variableInferenceIdField, "y"))
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new SemanticQueryBuilder(commonInferenceIdField, "a"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_1"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_1")
            )
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new SemanticQueryBuilder(variableInferenceIdField, "b"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_2"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_2")
            )
        );
    }

    public void testSemanticQueryWithCcMinimizeRoundTripsFalse() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };
        final SemanticQueryBuilder queryBuilder = new SemanticQueryBuilder("foo", "bar");
        final Consumer<SearchRequest> assertCcsMinimizeRoundTripsFalseFailure = s -> {
            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> client().search(s).actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(
                e.getMessage(),
                equalTo("semantic query does not support cross-cluster search when [ccs_minimize_roundtrips] is false")
            );
        };

        final TestIndexInfo localIndexInfo = new TestIndexInfo(localIndexName, Map.of(), Map.of(), Map.of());
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(remoteIndexName, Map.of(), Map.of(), Map.of());
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Explicitly set ccs_minimize_roundtrips=false in the search request
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);
        SearchRequest searchRequestWithCcMinimizeRoundTripsFalse = new SearchRequest(queryIndices, searchSourceBuilder);
        searchRequestWithCcMinimizeRoundTripsFalse.setCcsMinimizeRoundtrips(false);
        assertCcsMinimizeRoundTripsFalseFailure.accept(searchRequestWithCcMinimizeRoundTripsFalse);

        // Using a point in time implicitly sets ccs_minimize_roundtrips=false
        BytesReference pitId = openPointInTime(queryIndices, TimeValue.timeValueMinutes(2));
        SearchSourceBuilder searchSourceBuilderWithPit = new SearchSourceBuilder().query(queryBuilder)
            .pointInTimeBuilder(new PointInTimeBuilder(pitId));
        SearchRequest searchRequestWithPit = new SearchRequest().source(searchSourceBuilderWithPit);
        assertCcsMinimizeRoundTripsFalseFailure.accept(searchRequestWithPit);
    }
}
