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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class BulkShardRequestInferenceProvider {

    public static final String ROOT_INFERENCE_FIELD = "_semantic_text_inference";
    public static final String TEXT_SUBFIELD_NAME = "text";
    public static final String SPARSE_VECTOR_SUBFIELD_NAME = "sparse_embedding";

    private final Map<String, InferenceProvider> inferenceProvidersMap;

    private record InferenceProvider (Model model, InferenceService service) {
        private InferenceProvider {
            Objects.requireNonNull(model);
            Objects.requireNonNull(service);
        }
    }

    private BulkShardRequestInferenceProvider(Map<String, InferenceProvider> inferenceProvidersMap) {
        this.inferenceProvidersMap = inferenceProvidersMap;
    }

    public static void executeWithInferenceProvider(
        InferenceServiceRegistry inferenceServiceRegistry,
        ModelRegistry modelRegistry,
        Metadata clusterMetadata,
        Set<ShardId> shardIds,
        Consumer<BulkShardRequestInferenceProvider> action
    ) {
        Set<String> inferenceIds = new HashSet<>();
        shardIds.stream().map(ShardId::getIndex).collect(Collectors.toSet()).stream().forEach(index -> {
            var fieldsForModels = clusterMetadata.index(index).getFieldsForModels();
            inferenceIds.addAll(fieldsForModels.keySet());
        });
        final Map<String, InferenceProvider> inferenceProviderMap = new ConcurrentHashMap<>();
        Runnable onModelLoadingComplete = () -> action.accept(new BulkShardRequestInferenceProvider(inferenceProviderMap));
        try (var refs = new RefCountingRunnable(onModelLoadingComplete)) {
            for (var inferenceId : inferenceIds) {
                ActionListener<ModelRegistry.UnparsedModel> modelLoadingListener = new ActionListener<>() {
                    @Override
                    public void onResponse(ModelRegistry.UnparsedModel unparsedModel) {
                        var service = inferenceServiceRegistry.getService(unparsedModel.service());
                        if (service.isEmpty() == false) {
                            InferenceProvider inferenceProvider = new InferenceProvider(
                                service.get().parsePersistedConfig(
                                    inferenceId,
                                    unparsedModel.taskType(),
                                    unparsedModel.settings()),
                                service.get()
                            );
                            inferenceProviderMap.put(inferenceId, inferenceProvider);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // Do nothing - let it fail afterwards
                    }
                };

                modelRegistry.getModel(inferenceId, ActionListener.releaseAfter(modelLoadingListener, refs.acquire()));
            }
        }
    }

    public void processBulkShardRequest(
        BulkShardRequest bulkShardRequest,
        ClusterState clusterState,
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

        Runnable onInferenceComplete = () -> {
            // We need to remove items that have had an inference error, as the response will have been updated already
            // and we don't need to process them further
            BulkShardRequest errorsFilteredShardRequest = new BulkShardRequest(
                bulkShardRequest.shardId(),
                bulkShardRequest.getRefreshPolicy(),
                Arrays.stream(bulkShardRequest.items()).filter(Objects::nonNull).toArray(BulkItemRequest[]::new)
            );
            listener.onResponse(errorsFilteredShardRequest);
        };

        try (var bulkItemReqRef = new RefCountingRunnable(onInferenceComplete)) {
            for (BulkItemRequest bulkItemRequest : bulkShardRequest.items()) {
                performInferenceOnBulkItemRequest(
                    bulkItemRequest,
                    fieldsForModels,
                    onBulkItemFailure,
                    bulkItemReqRef.acquire()
                );
            }
        }
    }

    private void performInferenceOnBulkItemRequest(
        BulkItemRequest bulkItemRequest,
        Map<String, Set<String>> fieldsForModels,
        BiConsumer<BulkItemRequest, Exception> onBulkItemFailure,
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

            for (Map.Entry<String, Set<String>> fieldModelsEntrySet : fieldsForModels.entrySet()) {
                String modelId = fieldModelsEntrySet.getKey();

                @SuppressWarnings("unchecked")
                Map<String, Object> rootInferenceFieldMap = (Map<String, Object>) docMap.computeIfAbsent(
                    ROOT_INFERENCE_FIELD,
                    k -> new HashMap<String, Object>()
                );

                List<String> inferenceFieldNames = getFieldNamesForInference(fieldModelsEntrySet, docMap);

                if (inferenceFieldNames.isEmpty()) {
                    continue;
                }

                InferenceProvider inferenceProvider = inferenceProvidersMap.get(modelId);
                if (inferenceProvider == null) {
                    onBulkItemFailure.accept(
                        bulkItemRequest,
                        new IllegalArgumentException("No inference provider found for model ID " + modelId)
                    );
                    continue;
                }
                ActionListener<InferenceServiceResults> inferenceResultsListener = new ActionListener<>() {
                    @Override
                    public void onResponse(InferenceServiceResults results) {

                        if (results == null) {
                            throw new IllegalArgumentException(
                                "No inference retrieved for model ID " + modelId + " in document " + docWriteRequest.id()
                            );
                        }

                        int i = 0;
                        for (InferenceResults inferenceResults : results.transformToLegacyFormat()) {
                            String fieldName = inferenceFieldNames.get(i++);
                            @SuppressWarnings("unchecked")
                            List<Map<String, Object>> inferenceFieldResultList = (List<Map<String, Object>>) rootInferenceFieldMap
                                .computeIfAbsent(fieldName, k -> new ArrayList<>());

                            // TODO Check inference result type to change subfield name
                            var inferenceFieldMap = Map.of(
                                SPARSE_VECTOR_SUBFIELD_NAME, inferenceResults.asMap("output").get("output"),
                                TEXT_SUBFIELD_NAME, docMap.get(fieldName)
                            );
                            inferenceFieldResultList.add(inferenceFieldMap);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onBulkItemFailure.accept(bulkItemRequest, e);
                    }
                };
                inferenceProvider.service().infer(
                    inferenceProvider.model,
                    inferenceFieldNames.stream().map(docMap::get).map(String::valueOf).collect(Collectors.toList()),
                    // TODO check for additional settings needed
                    Map.of(),
                    ActionListener.releaseAfter(inferenceResultsListener, docRef.acquire()));
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
