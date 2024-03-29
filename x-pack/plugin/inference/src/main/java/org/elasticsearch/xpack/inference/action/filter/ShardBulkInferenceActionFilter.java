/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.inference.mapper.InferenceMetadataFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link MappedActionFilter} intercepting {@link BulkShardRequest}s to apply inference on fields declared as
 * {@link SemanticTextFieldMapper} in the index mapping.
 * The source of each {@link BulkItemRequest} requiring inference is augmented with the results for each field
 * under the {@link InferenceMetadataFieldMapper#NAME} section.
 * For example, for an index with a semantic field named {@code my_semantic_field} the following source document:
 * <br>
 * <pre>
 * {
 *      "my_semantic_text_field": "these are not the droids you're looking for"
 * }
 * </pre>
 * is rewritten into:
 * <br>
 * <pre>
 * {
 *      "_inference": {
 *        "my_semantic_field": {
 *          "inference_id": "my_inference_id",
 *                  "model_settings": {
 *                      "task_type": "SPARSE_EMBEDDING"
 *                  },
 *                  "chunks": [
 *                      {
 *                             "inference": {
 *                                 "lucas": 0.05212344,
 *                                 "ty": 0.041213956,
 *                                 "dragon": 0.50991,
 *                                 "type": 0.23241979,
 *                                 "dr": 1.9312073,
 *                                 "##o": 0.2797593
 *                             },
 *                             "text": "these are not the droids you're looking for"
 *                       }
 *                  ]
 *        }
 *      }
 *      "my_semantic_field": "these are not the droids you're looking for"
 * }
 * </pre>
 * The rewriting process occurs on the bulk coordinator node, and the results are then passed downstream
 * to the {@link TransportShardBulkAction} for actual indexing.
 *
 * TODO: batchSize should be configurable via a cluster setting
 */
public class ShardBulkInferenceActionFilter implements MappedActionFilter {
    private static final Logger logger = LogManager.getLogger(ShardBulkInferenceActionFilter.class);
    protected static final int DEFAULT_BATCH_SIZE = 512;

    private final InferenceServiceRegistry inferenceServiceRegistry;
    private final ModelRegistry modelRegistry;
    private final int batchSize;

    public ShardBulkInferenceActionFilter(InferenceServiceRegistry inferenceServiceRegistry, ModelRegistry modelRegistry) {
        this(inferenceServiceRegistry, modelRegistry, DEFAULT_BATCH_SIZE);
    }

    public ShardBulkInferenceActionFilter(InferenceServiceRegistry inferenceServiceRegistry, ModelRegistry modelRegistry, int batchSize) {
        this.inferenceServiceRegistry = inferenceServiceRegistry;
        this.modelRegistry = modelRegistry;
        this.batchSize = batchSize;
    }

    @Override
    public int order() {
        // must execute last (after the security action filter)
        return Integer.MAX_VALUE;
    }

    @Override
    public String actionName() {
        return TransportShardBulkAction.ACTION_NAME;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        switch (action) {
            case TransportShardBulkAction.ACTION_NAME:
                BulkShardRequest bulkShardRequest = (BulkShardRequest) request;
                var fieldInferenceMetadata = bulkShardRequest.consumeInferenceFieldMap();
                if (fieldInferenceMetadata != null && fieldInferenceMetadata.isEmpty() == false) {
                    Runnable onInferenceCompletion = () -> chain.proceed(task, action, request, listener);
                    processBulkShardRequest(fieldInferenceMetadata, bulkShardRequest, onInferenceCompletion);
                } else {
                    chain.proceed(task, action, request, listener);
                }
                break;

            default:
                chain.proceed(task, action, request, listener);
                break;
        }
    }

    private void processBulkShardRequest(
        Map<String, InferenceFieldMetadata> fieldInferenceMap,
        BulkShardRequest bulkShardRequest,
        Runnable onCompletion
    ) {
        new AsyncBulkShardInferenceAction(fieldInferenceMap, bulkShardRequest, onCompletion).run();
    }

    private record InferenceProvider(InferenceService service, Model model) {}

    private record FieldInferenceRequest(int id, String field, String input) {}

    private record FieldInferenceResponse(String field, @Nullable Model model, @Nullable ChunkedInferenceServiceResults chunkedResults) {}

    private record FieldInferenceResponseAccumulator(int id, List<FieldInferenceResponse> responses, List<Exception> failures) {}

    private class AsyncBulkShardInferenceAction implements Runnable {
        private final Map<String, InferenceFieldMetadata> fieldInferenceMap;
        private final BulkShardRequest bulkShardRequest;
        private final Runnable onCompletion;
        private final AtomicArray<FieldInferenceResponseAccumulator> inferenceResults;

        private AsyncBulkShardInferenceAction(
            Map<String, InferenceFieldMetadata> fieldInferenceMap,
            BulkShardRequest bulkShardRequest,
            Runnable onCompletion
        ) {
            this.fieldInferenceMap = fieldInferenceMap;
            this.bulkShardRequest = bulkShardRequest;
            this.inferenceResults = new AtomicArray<>(bulkShardRequest.items().length);
            this.onCompletion = onCompletion;
        }

        @Override
        public void run() {
            Map<String, List<FieldInferenceRequest>> inferenceRequests = createFieldInferenceRequests(bulkShardRequest);
            Runnable onInferenceCompletion = () -> {
                try {
                    for (var inferenceResponse : inferenceResults.asList()) {
                        var request = bulkShardRequest.items()[inferenceResponse.id];
                        try {
                            applyInferenceResponses(request, inferenceResponse);
                        } catch (Exception exc) {
                            request.abort(bulkShardRequest.index(), exc);
                        }
                    }
                } finally {
                    onCompletion.run();
                }
            };
            try (var releaseOnFinish = new RefCountingRunnable(onInferenceCompletion)) {
                for (var entry : inferenceRequests.entrySet()) {
                    executeShardBulkInferenceAsync(entry.getKey(), null, entry.getValue(), releaseOnFinish.acquire());
                }
            }
        }

        private void executeShardBulkInferenceAsync(
            final String inferenceId,
            @Nullable InferenceProvider inferenceProvider,
            final List<FieldInferenceRequest> requests,
            final Releasable onFinish
        ) {
            if (inferenceProvider == null) {
                ActionListener<ModelRegistry.UnparsedModel> modelLoadingListener = new ActionListener<>() {
                    @Override
                    public void onResponse(ModelRegistry.UnparsedModel unparsedModel) {
                        var service = inferenceServiceRegistry.getService(unparsedModel.service());
                        if (service.isEmpty() == false) {
                            var provider = new InferenceProvider(
                                service.get(),
                                service.get()
                                    .parsePersistedConfigWithSecrets(
                                        inferenceId,
                                        unparsedModel.taskType(),
                                        unparsedModel.settings(),
                                        unparsedModel.secrets()
                                    )
                            );
                            executeShardBulkInferenceAsync(inferenceId, provider, requests, onFinish);
                        } else {
                            try (onFinish) {
                                for (int i = 0; i < requests.size(); i++) {
                                    var request = requests.get(i);
                                    inferenceResults.get(request.id).failures.add(
                                        new ResourceNotFoundException(
                                            "Inference id [{}] not found for field [{}]",
                                            inferenceId,
                                            request.field
                                        )
                                    );
                                }
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception exc) {
                        try (onFinish) {
                            for (int i = 0; i < requests.size(); i++) {
                                var request = requests.get(i);
                                inferenceResults.get(request.id).failures.add(
                                    new ResourceNotFoundException("Inference id [{}] not found for field [{}]", inferenceId, request.field)
                                );
                            }
                        }
                    }
                };
                modelRegistry.getModelWithSecrets(inferenceId, modelLoadingListener);
                return;
            }
            int currentBatchSize = Math.min(requests.size(), batchSize);
            final List<FieldInferenceRequest> currentBatch = requests.subList(0, currentBatchSize);
            final List<FieldInferenceRequest> nextBatch = requests.subList(currentBatchSize, requests.size());
            final List<String> inputs = currentBatch.stream().map(FieldInferenceRequest::input).collect(Collectors.toList());
            ActionListener<List<ChunkedInferenceServiceResults>> completionListener = new ActionListener<>() {
                @Override
                public void onResponse(List<ChunkedInferenceServiceResults> results) {
                    try {
                        for (int i = 0; i < results.size(); i++) {
                            var request = requests.get(i);
                            var result = results.get(i);
                            var acc = inferenceResults.get(request.id);
                            acc.responses.add(new FieldInferenceResponse(request.field, inferenceProvider.model, result));
                        }
                    } finally {
                        onFinish();
                    }
                }

                @Override
                public void onFailure(Exception exc) {
                    try {
                        for (int i = 0; i < requests.size(); i++) {
                            var request = requests.get(i);
                            inferenceResults.get(request.id).failures.add(
                                new ElasticsearchException(
                                    "Exception when running inference id [{}] on field [{}]",
                                    exc,
                                    inferenceProvider.model.getInferenceEntityId(),
                                    request.field
                                )
                            );
                        }
                    } finally {
                        onFinish();
                    }
                }

                private void onFinish() {
                    if (nextBatch.isEmpty()) {
                        onFinish.close();
                    } else {
                        executeShardBulkInferenceAsync(inferenceId, inferenceProvider, nextBatch, onFinish);
                    }
                }
            };
            inferenceProvider.service()
                .chunkedInfer(
                    inferenceProvider.model(),
                    inputs,
                    Map.of(),
                    InputType.INGEST,
                    new ChunkingOptions(null, null),
                    completionListener
                );
        }

        private FieldInferenceResponseAccumulator ensureResponseAccumulatorSlot(int id) {
            FieldInferenceResponseAccumulator acc = inferenceResults.get(id);
            if (acc == null) {
                acc = new FieldInferenceResponseAccumulator(
                    id,
                    Collections.synchronizedList(new ArrayList<>()),
                    Collections.synchronizedList(new ArrayList<>())
                );
                inferenceResults.set(id, acc);
            }
            return acc;
        }

        private void addInferenceResponseFailure(int id, Exception failure) {
            var acc = ensureResponseAccumulatorSlot(id);
            acc.failures().add(failure);
        }

        /**
         * Applies the {@link FieldInferenceResponseAccumulator} to the provided {@link BulkItemRequest}.
         * If the response contains failures, the bulk item request is marked as failed for the downstream action.
         * Otherwise, the source of the request is augmented with the field inference results under the
         * {@link InferenceMetadataFieldMapper#NAME} field.
         */
        private void applyInferenceResponses(BulkItemRequest item, FieldInferenceResponseAccumulator response) {
            if (response.failures().isEmpty() == false) {
                for (var failure : response.failures()) {
                    item.abort(item.index(), failure);
                }
                return;
            }

            final IndexRequest indexRequest = getIndexRequestOrNull(item.request());
            Map<String, Object> newDocMap = indexRequest.sourceAsMap();
            Object inferenceObj = newDocMap.computeIfAbsent(InferenceMetadataFieldMapper.NAME, k -> new LinkedHashMap<String, Object>());
            Map<String, Object> inferenceMap = XContentMapValues.nodeMapValue(inferenceObj, InferenceMetadataFieldMapper.NAME);
            newDocMap.put(InferenceMetadataFieldMapper.NAME, inferenceMap);
            for (FieldInferenceResponse fieldResponse : response.responses()) {
                if (fieldResponse.chunkedResults != null) {
                    try {
                        InferenceMetadataFieldMapper.applyFieldInference(
                            inferenceMap,
                            fieldResponse.field(),
                            fieldResponse.model(),
                            fieldResponse.chunkedResults()
                        );
                    } catch (Exception exc) {
                        item.abort(item.index(), exc);
                    }
                } else {
                    inferenceMap.remove(fieldResponse.field);
                }
            }
            indexRequest.source(newDocMap);
        }

        /**
         * Register a {@link FieldInferenceRequest} for every non-empty field referencing an inference ID in the index.
         * If results are already populated for fields in the existing _inference object,
         * the inference request for this specific field is skipped, and the existing results remain unchanged.
         * Validation of inference ID and model settings occurs in the {@link InferenceMetadataFieldMapper}
         * during field indexing, where an error will be thrown if they mismatch or if the content is malformed.
         *
         * TODO: Should we validate the settings for pre-existing results here and apply the inference only if they differ?
         */
        private Map<String, List<FieldInferenceRequest>> createFieldInferenceRequests(BulkShardRequest bulkShardRequest) {
            Map<String, List<FieldInferenceRequest>> fieldRequestsMap = new LinkedHashMap<>();
            for (var item : bulkShardRequest.items()) {
                if (item.getPrimaryResponse() != null) {
                    // item was already aborted/processed by a filter in the chain upstream (e.g. security)
                    continue;
                }
                final IndexRequest indexRequest;
                if (item.request() instanceof IndexRequest ir) {
                    indexRequest = ir;
                } else if (item.request() instanceof UpdateRequest updateRequest) {
                    if (updateRequest.script() != null) {
                        addInferenceResponseFailure(
                            item.id(),
                            new ElasticsearchStatusException(
                                "Cannot apply update with a script on indices that contain [{}] field(s)",
                                RestStatus.BAD_REQUEST,
                                SemanticTextFieldMapper.CONTENT_TYPE
                            )
                        );
                        continue;
                    }
                    indexRequest = updateRequest.doc();
                } else {
                    // ignore delete request
                    continue;
                }
                final Map<String, Object> docMap = indexRequest.sourceAsMap();
                final Map<String, Object> inferenceMap = XContentMapValues.nodeMapValue(
                    docMap.computeIfAbsent(InferenceMetadataFieldMapper.NAME, k -> new LinkedHashMap<String, Object>()),
                    InferenceMetadataFieldMapper.NAME
                );
                for (var entry : fieldInferenceMap.values()) {
                    String field = entry.getName();
                    String inferenceId = entry.getInferenceId();
                    Object inferenceResult = inferenceMap.remove(field);
                    var value = XContentMapValues.extractValue(field, docMap);
                    if (value == null) {
                        if (inferenceResult != null) {
                            addInferenceResponseFailure(
                                item.id(),
                                new ElasticsearchStatusException(
                                    "The field [{}] is referenced in the [{}] metadata field but has no value",
                                    RestStatus.BAD_REQUEST,
                                    field,
                                    InferenceMetadataFieldMapper.NAME
                                )
                            );
                        }
                        continue;
                    }
                    ensureResponseAccumulatorSlot(item.id());
                    if (value instanceof String valueStr) {
                        List<FieldInferenceRequest> fieldRequests = fieldRequestsMap.computeIfAbsent(inferenceId, k -> new ArrayList<>());
                        fieldRequests.add(new FieldInferenceRequest(item.id(), field, valueStr));
                    } else {
                        addInferenceResponseFailure(
                            item.id(),
                            new ElasticsearchStatusException(
                                "Invalid format for field [{}], expected [String] got [{}]",
                                RestStatus.BAD_REQUEST,
                                field,
                                value.getClass().getSimpleName()
                            )
                        );
                    }
                }
            }
            return fieldRequestsMap;
        }
    }

    static IndexRequest getIndexRequestOrNull(DocWriteRequest<?> docWriteRequest) {
        if (docWriteRequest instanceof IndexRequest indexRequest) {
            return indexRequest;
        } else if (docWriteRequest instanceof UpdateRequest updateRequest) {
            return updateRequest.doc();
        } else {
            return null;
        }
    }
}
