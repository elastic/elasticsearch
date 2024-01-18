/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.inference.InferenceProvider;
import org.elasticsearch.inference.InferenceResults;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class BulkShardRequestInferenceProvider {

    public static final String ROOT_RESULT_FIELD = "_ml_inference";
    public static final String INFERENCE_FIELD = "result";
    public static final String TEXT_FIELD = "text";

    private final InferenceProvider inferenceProvider;

    public BulkShardRequestInferenceProvider(InferenceProvider inferenceProvider) {
        this.inferenceProvider = inferenceProvider;
    }

    public void processBulkShardRequest(
        BulkShardRequest bulkShardRequest,
        ClusterState clusterState,
        TriConsumer<BulkShardRequest, BulkItemRequest, Exception> onBulkItemFailure,
        Consumer<BulkShardRequest> nextAction
    ) {

        Map<String, Set<String>> fieldsForModels = clusterState.metadata()
            .index(bulkShardRequest.shardId().getIndex())
            .getFieldsForModels();
        // No inference fields? Just execute the request
        if (fieldsForModels.isEmpty()) {
            nextAction.accept(bulkShardRequest);
            return;
        }

        Runnable onInferenceComplete = () -> {
            // We need to remove items that have had an inference error, as the response will have been updated already
            // and we don't need to process them further
            BulkShardRequest errorsFilteredShardRequest = new BulkShardRequest(
                bulkShardRequest.shardId(),
                bulkShardRequest.getRefreshPolicy(),
                Arrays.stream(bulkShardRequest.items()).filter(Objects::nonNull).toArray(BulkItemRequest[]::new)
            );
            nextAction.accept(errorsFilteredShardRequest);
        };

        try (var bulkItemReqRef = new RefCountingRunnable(onInferenceComplete)) {
            for (BulkItemRequest bulkItemRequest : bulkShardRequest.items()) {
                performInferenceOnBulkItemRequest(
                    bulkShardRequest,
                    bulkItemRequest,
                    fieldsForModels,
                    onBulkItemFailure,
                    bulkItemReqRef.acquire()
                );
            }
        }
    }

    private void performInferenceOnBulkItemRequest(
        BulkShardRequest bulkShardRequest,
        BulkItemRequest bulkItemRequest,
        Map<String, Set<String>> fieldsForModels,
        TriConsumer<BulkShardRequest, BulkItemRequest, Exception> onBulkItemFailure,
        Releasable releaseOnFinish
    ) {
        if (inferenceProvider.performsInference() == false) {
            releaseOnFinish.close();
            return;
        }

        DocWriteRequest<?> docWriteRequest = bulkItemRequest.request();
        Map<String, Object> sourceMap = null;
        if (docWriteRequest instanceof IndexRequest indexRequest) {
            sourceMap = indexRequest.sourceAsMap();
        } else if (docWriteRequest instanceof UpdateRequest updateRequest) {
            sourceMap = updateRequest.docAsUpsert() ? updateRequest.upsertRequest().sourceAsMap() : updateRequest.doc().sourceAsMap();
        }
        if (sourceMap == null || sourceMap.isEmpty()) {
            releaseOnFinish.close();
            return;
        }
        final Map<String, Object> docMap = new ConcurrentHashMap<>(sourceMap);

        // When a document completes processing, update the source with the inference
        try (var docRef = new RefCountingRunnable(() -> {
            if (docWriteRequest instanceof IndexRequest indexRequest) {
                indexRequest.source(docMap);
            } else if (docWriteRequest instanceof UpdateRequest updateRequest) {
                if (updateRequest.docAsUpsert()) {
                    updateRequest.upsertRequest().source(docMap);
                } else {
                    updateRequest.doc().source(docMap);
                }
            }
            releaseOnFinish.close();
        })) {

            for (Map.Entry<String, Set<String>> fieldModelsEntrySet : fieldsForModels.entrySet()) {
                String modelId = fieldModelsEntrySet.getKey();

                @SuppressWarnings("unchecked")
                Map<String, Object> rootInferenceFieldMap = (Map<String, Object>) docMap.computeIfAbsent(
                    ROOT_RESULT_FIELD,
                    k -> new HashMap<String, Object>()
                );

                List<String> inferenceFieldNames = getFieldNamesForInference(fieldModelsEntrySet, docMap);

                if (inferenceFieldNames.isEmpty()) {
                    continue;
                }

                docRef.acquire();

                inferenceProvider.textInference(
                    modelId,
                    inferenceFieldNames.stream().map(docMap::get).map(String::valueOf).collect(Collectors.toList()),
                    new ActionListener<>() {

                        @Override
                        public void onResponse(List<InferenceResults> results) {

                            if (results == null) {
                                throw new IllegalArgumentException(
                                    "No inference retrieved for model ID " + modelId + " in document " + docWriteRequest.id()
                                );
                            }

                            int i = 0;
                            for (InferenceResults inferenceResults : results) {
                                String fieldName = inferenceFieldNames.get(i++);
                                @SuppressWarnings("unchecked")
                                Map<String, Object> inferenceFieldMap = (Map<String, Object>) rootInferenceFieldMap.computeIfAbsent(
                                    fieldName,
                                    k -> new HashMap<String, Object>()
                                );

                                inferenceFieldMap.put(INFERENCE_FIELD, inferenceResults.asMap("output").get("output"));
                                inferenceFieldMap.put(TEXT_FIELD, docMap.get(fieldName));
                            }

                            docRef.close();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            onBulkItemFailure.apply(bulkShardRequest, bulkItemRequest, e);
                            docRef.close();
                        }
                    }
                );
            }
        }
    }

    private static List<String> getFieldNamesForInference(Map.Entry<String, Set<String>> fieldModelsEntrySet, Map<String, Object> docMap) {
        List<String> inferenceFieldNames = new ArrayList<>();
        for (String inferenceField : fieldModelsEntrySet.getValue()) {
            Object fieldValue = docMap.get(inferenceField);

            // Perform inference on string, non-null values
            if (fieldValue instanceof String) {
                inferenceFieldNames.add(inferenceField);
            }
        }
        return inferenceFieldNames;
    }

    @SuppressWarnings("unchecked")
    private static String findMapValue(Map<String, Object> map, String... path) {
        Map<String, Object> currentMap = map;
        for (int i = 0; i < path.length - 1; i++) {
            Object value = currentMap.get(path[i]);

            if (value instanceof Map) {
                currentMap = (Map<String, Object>) value;
            } else {
                // Invalid path or non-Map value encountered
                return null;
            }
        }

        // Retrieve the final value in the map, if it's a String
        Object finalValue = currentMap.get(path[path.length - 1]);

        return (finalValue instanceof String) ? (String) finalValue : null;
    }

}
