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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelRegistry;
import org.elasticsearch.inference.SemanticTextModelSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Performs inference on a {@link BulkShardRequest}, updating the source of each document with the inference results.
 */
public class BulkShardRequestInferenceProvider {

    // Root field name for storing inference results
    public static final String ROOT_INFERENCE_FIELD = "_semantic_text_inference";

    // Contains the original text for the field

    public static final String INFERENCE_RESULTS = "inference_results";
    public static final String INFERENCE_CHUNKS_RESULTS = "inference";
    public static final String INFERENCE_CHUNKS_TEXT = "text";

    private final ClusterState clusterState;
    private final Map<String, InferenceProvider> inferenceProvidersMap;

    private record InferenceProvider(Model model, InferenceService service) {
        private InferenceProvider {
            Objects.requireNonNull(model);
            Objects.requireNonNull(service);
        }
    }

    BulkShardRequestInferenceProvider(ClusterState clusterState, Map<String, InferenceProvider> inferenceProvidersMap) {
        this.clusterState = clusterState;
        this.inferenceProvidersMap = inferenceProvidersMap;
    }

    public static void getInstance(
        InferenceServiceRegistry inferenceServiceRegistry,
        ModelRegistry modelRegistry,
        ClusterState clusterState,
        Set<ShardId> shardIds,
        ActionListener<BulkShardRequestInferenceProvider> listener
    ) {
        Set<String> inferenceIds = new HashSet<>();
        shardIds.stream().map(ShardId::getIndex).collect(Collectors.toSet()).stream().forEach(index -> {
            var fieldsForModels = clusterState.metadata().index(index).getFieldsForModels();
            inferenceIds.addAll(fieldsForModels.keySet());
        });
        final Map<String, InferenceProvider> inferenceProviderMap = new ConcurrentHashMap<>();
        Runnable onModelLoadingComplete = () -> listener.onResponse(
            new BulkShardRequestInferenceProvider(clusterState, inferenceProviderMap)
        );
        try (var refs = new RefCountingRunnable(onModelLoadingComplete)) {
            for (var inferenceId : inferenceIds) {
                ActionListener<ModelRegistry.UnparsedModel> modelLoadingListener = new ActionListener<>() {
                    @Override
                    public void onResponse(ModelRegistry.UnparsedModel unparsedModel) {
                        var service = inferenceServiceRegistry.getService(unparsedModel.service());
                        if (service.isEmpty() == false) {
                            InferenceProvider inferenceProvider = new InferenceProvider(
                                service.get()
                                    .parsePersistedConfigWithSecrets(
                                        inferenceId,
                                        unparsedModel.taskType(),
                                        unparsedModel.settings(),
                                        unparsedModel.secrets()
                                    ),
                                service.get()
                            );
                            inferenceProviderMap.put(inferenceId, inferenceProvider);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // Failure on loading a model should not prevent the rest from being loaded and used.
                        // When the model is actually retrieved via the inference ID in the inference process, it will fail
                        // and the user will get the details on the inference failure.
                    }
                };

                modelRegistry.getModelWithSecrets(inferenceId, ActionListener.releaseAfter(modelLoadingListener, refs.acquire()));
            }
        }
    }

    /**
     * Performs inference on the fields that have inference models for a bulk shard request. Bulk items from
     * the original request will be modified with the inference results, to avoid copying the entire requests from
     * the original bulk request.
     *
     * @param bulkShardRequest original BulkShardRequest that will be modified with inference results.
     * @param listener        listener to be called when the inference process is finished with the new BulkShardRequest,
     *                        which may have fewer items than the original because of inference failures
     * @param onBulkItemFailure invoked when a bulk item fails inference
     */
    public void processBulkShardRequest(
        BulkShardRequest bulkShardRequest,
        ActionListener<BulkShardRequest> listener,
        BiConsumer<BulkItemRequest, Exception> onBulkItemFailure
    ) {

        Map<String, Set<String>> fieldsForModels = clusterState.metadata()
            .index(bulkShardRequest.shardId().getIndex())
            .getFieldsForModels();
        // No inference fields? Terminate early
        if (fieldsForModels.isEmpty()) {
            listener.onResponse(bulkShardRequest);
            return;
        }

        Set<Integer> failedItems = Collections.synchronizedSet(new HashSet<>());
        Runnable onInferenceComplete = () -> {
            if (failedItems.isEmpty()) {
                listener.onResponse(bulkShardRequest);
                return;
            }
            // Remove failed items from the original bulk shard request
            BulkItemRequest[] originalItems = bulkShardRequest.items();
            BulkItemRequest[] newItems = new BulkItemRequest[originalItems.length - failedItems.size()];
            for (int i = 0, j = 0; i < originalItems.length; i++) {
                if (failedItems.contains(i) == false) {
                    newItems[j++] = originalItems[i];
                }
            }
            BulkShardRequest newBulkShardRequest = new BulkShardRequest(
                bulkShardRequest.shardId(),
                bulkShardRequest.getRefreshPolicy(),
                newItems
            );
            listener.onResponse(newBulkShardRequest);
        };
        TriConsumer<BulkItemRequest, Integer, Exception> onBulkItemFailureWithIndex = (bulkItemRequest, i, e) -> {
            failedItems.add(i);
            onBulkItemFailure.accept(bulkItemRequest, e);
        };
        try (var bulkItemReqRef = new RefCountingRunnable(onInferenceComplete)) {
            BulkItemRequest[] items = bulkShardRequest.items();
            for (int i = 0; i < items.length; i++) {
                BulkItemRequest bulkItemRequest = items[i];
                // Bulk item might be null because of previous errors, skip in that case
                if (bulkItemRequest != null) {
                    performInferenceOnBulkItemRequest(
                        bulkItemRequest,
                        fieldsForModels,
                        i,
                        onBulkItemFailureWithIndex,
                        bulkItemReqRef.acquire()
                    );
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void performInferenceOnBulkItemRequest(
        BulkItemRequest bulkItemRequest,
        Map<String, Set<String>> fieldsForModels,
        Integer itemIndex,
        TriConsumer<BulkItemRequest, Integer, Exception> onBulkItemFailure,
        Releasable releaseOnFinish
    ) {

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

            Map<String, Object> rootInferenceFieldMap;
            try {
                rootInferenceFieldMap = (Map<String, Object>) docMap.computeIfAbsent(
                    ROOT_INFERENCE_FIELD,
                    k -> new HashMap<String, Object>()
                );
            } catch (ClassCastException e) {
                onBulkItemFailure.apply(
                    bulkItemRequest,
                    itemIndex,
                    new IllegalArgumentException("Inference result field [" + ROOT_INFERENCE_FIELD + "] is not an object")
                );
                return;
            }

            for (Map.Entry<String, Set<String>> fieldModelsEntrySet : fieldsForModels.entrySet()) {
                String modelId = fieldModelsEntrySet.getKey();
                List<String> inferenceFieldNames = getFieldNamesForInference(fieldModelsEntrySet, docMap);
                if (inferenceFieldNames.isEmpty()) {
                    continue;
                }

                InferenceProvider inferenceProvider = inferenceProvidersMap.get(modelId);
                if (inferenceProvider == null) {
                    onBulkItemFailure.apply(
                        bulkItemRequest,
                        itemIndex,
                        new IllegalArgumentException("No inference provider found for model ID " + modelId)
                    );
                    return;
                }
                ActionListener<InferenceServiceResults> inferenceResultsListener = new ActionListener<>() {
                    @Override
                    public void onResponse(InferenceServiceResults results) {
                        if (results == null) {
                            onBulkItemFailure.apply(
                                bulkItemRequest,
                                itemIndex,
                                new IllegalArgumentException(
                                    "No inference results retrieved for model ID " + modelId + " in document " + docWriteRequest.id()
                                )
                            );
                        }

                        int i = 0;
                        for (InferenceResults inferenceResults : results.transformToCoordinationFormat()) {
                            String inferenceFieldName = inferenceFieldNames.get(i++);
                            Map<String, Object> inferenceFieldResult = new LinkedHashMap<>();
                            inferenceFieldResult.putAll(new SemanticTextModelSettings(inferenceProvider.model).asMap());
                            inferenceFieldResult.put(
                                INFERENCE_RESULTS,
                                List.of(
                                    Map.of(
                                        INFERENCE_CHUNKS_RESULTS,
                                        inferenceResults.asMap("output").get("output"),
                                        INFERENCE_CHUNKS_TEXT,
                                        docMap.get(inferenceFieldName)
                                    )
                                )
                            );
                            rootInferenceFieldMap.put(inferenceFieldName, inferenceFieldResult);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onBulkItemFailure.apply(bulkItemRequest, itemIndex, e);
                    }
                };
                inferenceProvider.service()
                    .infer(
                        inferenceProvider.model,
                        inferenceFieldNames.stream().map(docMap::get).map(String::valueOf).collect(Collectors.toList()),
                        // TODO check for additional settings needed
                        Map.of(),
                        InputType.INGEST,
                        ActionListener.releaseAfter(inferenceResultsListener, docRef.acquire())
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
}
