/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.inference.EndpointClusterState;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.inference.integration.IntegrationTestUtils.deleteInferenceEndpoint;

public class MatchQueryBuilderCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String TEXT_FIELD = "text-field";
    private static final Set<String> allFieldTypes = Set.of("text", "semantic_text", "semantic");
    private static final Set<String> semanticFieldTypes = Set.of("semantic_text", "semantic");
    // We don't use SPARSE_EMBEDDING for semantic_text since it becomes tricky to assert the order of results when
    // both dense and sparse embeddings (which generate high score values) are used in the same query. We use boostLocalIndex() to
    // ensure that local result is always ranked higher but for sparse embeddings, we need to provide a significanly higher boost value.
    private static final Map<String, Collection<TaskType>> taskTypes = Map.of(
        "semantic_text",
        List.of(TaskType.TEXT_EMBEDDING, TaskType.EMBEDDING),
        "semantic",
        List.of(TaskType.EMBEDDING)
    );

    private final Map<String, EndpointClusterState> localInferenceIds = new HashMap<>();
    private final Map<String, EndpointClusterState> remoteInferenceIds = new HashMap<>();

    @Override
    protected boolean reuseClusters() {
        return true;
    }

    @Before
    public void configureInferenceEndpoints() throws Exception {
        configureClusters();
    }

    @After
    public void cleanup() {
        // The cleanup method of base class only deletes user indices and not the system indices. Hence, we explicitly delete
        // the inference endpoints so that next test can re-create them with the same inference ID values.
        for (var entry : localInferenceIds.entrySet()) {
            deleteInferenceEndpoint(client(LOCAL_CLUSTER), entry.getValue().taskType(), entry.getKey());
        }
        for (var entry : remoteInferenceIds.entrySet()) {
            deleteInferenceEndpoint(client(REMOTE_CLUSTER), entry.getValue().taskType(), entry.getKey());
        }
    }

    public void testMatchQueryWithCcsMinimizeRoundTripsTrue() throws Exception {
        matchQueryBaseTestCases(true);
    }

    public void testMatchQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
        matchQueryBaseTestCases(false);
    }

    public void testBlankQueryHandlingWithCcsMinimizeRoundTripsTrue() throws Exception {
        blankQueryHandlingTestCase(true);
    }

    public void testBlankQueryHandlingWithCcsMinimizeRoundTripsFalse() throws Exception {
        blankQueryHandlingTestCase(false);
    }

    private void blankQueryHandlingTestCase(boolean ccsMinimizeRoundTrips) throws Exception {
        final Consumer<SearchRequest> searchRequestModifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundTrips);
        final String expectedLocalClusterAlias = getExpectedLocalClusterAlias(ccsMinimizeRoundTrips);

        for (String semanticFieldType : semanticFieldTypes) {
            String commonInferenceIdFieldName = commonInferenceIdFieldName(semanticFieldType);
            assertSearchResponse(
                new MatchQueryBuilder(commonInferenceIdFieldName, "   "),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(commonInferenceIdFieldName)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(commonInferenceIdFieldName))
                ),
                null,
                searchRequestModifier
            );

            String variableInferenceIdFieldName = variableInferenceIdFieldName(semanticFieldType);
            assertSearchResponse(
                new MatchQueryBuilder(variableInferenceIdFieldName, "   "),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(variableInferenceIdFieldName)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(variableInferenceIdFieldName))
                ),
                null,
                searchRequestModifier
            );

            // only semantic field in local index should return results for blank query string
            String mixedField1 = mixedTypeFieldName(semanticFieldType, "text");
            assertSearchResponse(
                new MatchQueryBuilder(mixedField1, "   "),
                QUERY_INDICES,
                List.of(new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(mixedField1))),
                null,
                searchRequestModifier
            );

            // only semantic field on remote index should return results for blank query string
            String mixedField2 = mixedTypeFieldName("text", semanticFieldType);
            assertSearchResponse(
                new MatchQueryBuilder(mixedField2, "   "),
                QUERY_INDICES,
                List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(mixedField2))),
                null,
                searchRequestModifier
            );
        }

        // assert "text" fields with blank query string returns no results
        assertSearchResponse(new MatchQueryBuilder(TEXT_FIELD, "   "), QUERY_INDICES, List.of(), null, searchRequestModifier);
    }

    private void matchQueryBaseTestCases(boolean ccsMinimizeRoundTrips) throws Exception {
        final Consumer<SearchRequest> searchRequestModifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundTrips);
        final String expectedLocalClusterAlias = getExpectedLocalClusterAlias(ccsMinimizeRoundTrips);

        for (String semanticFieldType : semanticFieldTypes) {
            // Query the field with same inference ID across clusters, but with different backing inference services
            String commonInferenceIdFieldName = commonInferenceIdFieldName(semanticFieldType);
            assertSearchResponse(
                new MatchQueryBuilder(commonInferenceIdFieldName, getFieldValue(commonInferenceIdFieldName)),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(commonInferenceIdFieldName)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(commonInferenceIdFieldName))
                ),
                null,
                searchRequestModifier
            );

            // Query a field that has different inference ID values across clusters
            String variableInferenceIdFieldName = variableInferenceIdFieldName(semanticFieldType);
            assertSearchResponse(
                new MatchQueryBuilder(variableInferenceIdFieldName, getFieldValue(variableInferenceIdFieldName)),
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(variableInferenceIdFieldName)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(variableInferenceIdFieldName))
                ),
                null,
                searchRequestModifier
            );

            // Query an inference field on a remote cluster
            assertSearchResponse(
                new MatchQueryBuilder(commonInferenceIdFieldName, getFieldValue(commonInferenceIdFieldName)),
                List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
                List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(commonInferenceIdFieldName))),
                null,
                searchRequestModifier
            );

            // Query using index patterns
            assertSearchResponse(
                new MatchQueryBuilder(commonInferenceIdFieldName, getFieldValue(commonInferenceIdFieldName)),
                List.of("local-*", fullyQualifiedIndexName("cluster_*", "remote-*")),
                List.of(
                    new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(commonInferenceIdFieldName)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(commonInferenceIdFieldName))
                ),
                null,
                searchRequestModifier
            );
        }

        // Query fields with mixed types across clusters
        for (String localFieldType : allFieldTypes) {
            for (String remoteFieldType : allFieldTypes) {
                if (localFieldType.equals(remoteFieldType)) {
                    continue;
                }
                String mixedFieldName = mixedTypeFieldName(localFieldType, remoteFieldType);
                assertSearchResponse(
                    new MatchQueryBuilder(mixedFieldName, getFieldValue(mixedFieldName)),
                    QUERY_INDICES,
                    List.of(
                        new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(mixedFieldName)),
                        new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(mixedFieldName))
                    ),
                    null,
                    searchRequestModifier
                );
            }
        }

        // Validate that a CCS match query functions when only text fields are queried
        assertSearchResponse(
            new MatchQueryBuilder(TEXT_FIELD, getFieldValue(TEXT_FIELD)),
            QUERY_INDICES,
            List.of(
                new SearchResult(expectedLocalClusterAlias, LOCAL_INDEX_NAME, getDocId(TEXT_FIELD)),
                new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(TEXT_FIELD))
            ),
            null,
            searchRequestModifier
        );
        assertSearchResponse(
            new MatchQueryBuilder(TEXT_FIELD, getFieldValue(TEXT_FIELD)),
            List.of(FULLY_QUALIFIED_REMOTE_INDEX_NAME),
            List.of(new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, getDocId(TEXT_FIELD))),
            null,
            searchRequestModifier
        );
    }

    private void configureClusters() throws Exception {
        final Map<String, Object> localMappings = new HashMap<>();
        final Map<String, Object> remoteMappings = new HashMap<>();
        final Map<String, Map<String, Object>> docs = new HashMap<>();

        for (String semanticFieldType : semanticFieldTypes) {
            // Create fields with common inference ID across clusters
            String commonInferenceIdFieldName = commonInferenceIdFieldName(semanticFieldType);
            String commonInferenceId = semanticFieldType + "-common-inference-id";
            localInferenceIds.put(commonInferenceId, getServiceSettings(semanticFieldType));
            remoteInferenceIds.put(commonInferenceId, getServiceSettings(semanticFieldType));
            localMappings.put(commonInferenceIdFieldName, fieldMapping(semanticFieldType, commonInferenceId));
            remoteMappings.put(commonInferenceIdFieldName, fieldMapping(semanticFieldType, commonInferenceId));
            docs.put(getDocId(commonInferenceIdFieldName), Map.of(commonInferenceIdFieldName, getFieldValue(commonInferenceIdFieldName)));

            // Create fields with variable inference ID across clusters
            String variableInferenceIdFieldName = variableInferenceIdFieldName(semanticFieldType);
            String localInferenceId = localInferenceId(semanticFieldType);
            String remoteInferenceId = remoteInferenceId(semanticFieldType);
            localInferenceIds.put(localInferenceId, getServiceSettings(semanticFieldType));
            remoteInferenceIds.put(remoteInferenceId, getServiceSettings(semanticFieldType));
            localMappings.put(variableInferenceIdFieldName, fieldMapping(semanticFieldType, localInferenceId));
            remoteMappings.put(variableInferenceIdFieldName, fieldMapping(semanticFieldType, remoteInferenceId));
            docs.put(
                getDocId(variableInferenceIdFieldName),
                Map.of(variableInferenceIdFieldName, getFieldValue(variableInferenceIdFieldName))
            );
        }

        // Create fields with mixed types across clusters
        String sharedInferenceId = "shared-inference-id";
        EndpointClusterState sharedSettings = getServiceSettings("semantic");
        localInferenceIds.put(sharedInferenceId, sharedSettings);
        remoteInferenceIds.put(sharedInferenceId, sharedSettings);
        for (String localFieldType : allFieldTypes) {
            for (String remoteFieldType : allFieldTypes) {
                if (localFieldType.equals(remoteFieldType)) {
                    continue;
                }
                String mixedFieldName = mixedTypeFieldName(localFieldType, remoteFieldType);
                localMappings.put(
                    mixedFieldName,
                    localFieldType.equals("text") ? textMapping() : fieldMapping(localFieldType, sharedInferenceId)
                );
                remoteMappings.put(
                    mixedFieldName,
                    remoteFieldType.equals("text") ? textMapping() : fieldMapping(remoteFieldType, sharedInferenceId)
                );
                docs.put(getDocId(mixedFieldName), Map.of(mixedFieldName, getFieldValue(mixedFieldName)));
            }
        }

        // create simple "text" fields
        localMappings.put(TEXT_FIELD, textMapping());
        remoteMappings.put(TEXT_FIELD, textMapping());
        docs.put(getDocId(TEXT_FIELD), Map.of(TEXT_FIELD, getFieldValue(TEXT_FIELD)));

        final TestIndexInfo localIndexInfo = new TestIndexInfo(LOCAL_INDEX_NAME, localInferenceIds, localMappings, docs);
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(REMOTE_INDEX_NAME, remoteInferenceIds, remoteMappings, docs);
        setupTwoClusters(localIndexInfo, remoteIndexInfo);
    }

    private static Map<String, Object> fieldMapping(String semanticFieldType, String inferenceId) {
        return Map.of("type", semanticFieldType, "inference_id", inferenceId);
    }

    private static EndpointClusterState getServiceSettings(String semanticFieldType) {
        TaskType taskType = randomFrom(taskTypes.get(semanticFieldType));
        return new EndpointClusterState(TestModel.createRandomInstance(taskType, List.of(SimilarityMeasure.DOT_PRODUCT)));
    }

    private static String mixedTypeFieldName(String localFieldType, String remoteFieldType) {
        return localFieldType + "-mixed-type-field-" + remoteFieldType;
    }

    private static String commonInferenceIdFieldName(String semanticFieldType) {
        return semanticFieldType + "_common_inference_id_field";
    }

    private static String localInferenceId(String semanticFieldType) {
        return semanticFieldType + "-local-inference-id";
    }

    private static String remoteInferenceId(String semanticFieldType) {
        return semanticFieldType + "-remote-inference-id";
    }

    private String variableInferenceIdFieldName(String semanticFieldType) {
        return semanticFieldType + "_variable_inference_id_field";
    }

    private static String getFieldValue(String field) {
        return field + "_value";
    }

    private static String getDocId(String field) {
        return field + "_doc";
    }
}
