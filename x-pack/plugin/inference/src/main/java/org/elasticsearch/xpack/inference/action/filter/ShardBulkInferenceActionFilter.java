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
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.FieldInferenceMetadata;
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
import org.elasticsearch.xpack.inference.mapper.InferenceResultFieldMapper;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An {@link ActionFilter} that performs inference on {@link BulkShardRequest} asynchronously and stores the results in
 * the individual {@link BulkItemRequest}. The results are then consumed by the {@link InferenceResultFieldMapper}
 * in the subsequent {@link TransportShardBulkAction} downstream.
 */
public class ShardBulkInferenceActionFilter implements MappedActionFilter {
    private static final Logger logger = LogManager.getLogger(ShardBulkInferenceActionFilter.class);

    private final InferenceServiceRegistry inferenceServiceRegistry;
    private final ModelRegistry modelRegistry;

    public ShardBulkInferenceActionFilter(InferenceServiceRegistry inferenceServiceRegistry, ModelRegistry modelRegistry) {
        this.inferenceServiceRegistry = inferenceServiceRegistry;
        this.modelRegistry = modelRegistry;
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
                var fieldInferenceMetadata = bulkShardRequest.consumeFieldInferenceMetadata();
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
        FieldInferenceMetadata fieldInferenceMetadata,
        BulkShardRequest bulkShardRequest,
        Runnable onCompletion
    ) {
        new AsyncBulkShardInferenceAction(fieldInferenceMetadata, bulkShardRequest, onCompletion).run();
    }

    private record InferenceProvider(InferenceService service, Model model) {}

    private record FieldInferenceRequest(int id, String field, String input) {}

    private record FieldInferenceResponse(String field, Model model, ChunkedInferenceServiceResults chunkedResults) {}

    private record FieldInferenceResponseAccumulator(int id, List<FieldInferenceResponse> responses, List<Exception> failures) {}

    private class AsyncBulkShardInferenceAction implements Runnable {
        private final FieldInferenceMetadata fieldInferenceMetadata;
        private final BulkShardRequest bulkShardRequest;
        private final Runnable onCompletion;
        private final AtomicArray<FieldInferenceResponseAccumulator> inferenceResults;

        private AsyncBulkShardInferenceAction(
            FieldInferenceMetadata fieldInferenceMetadata,
            BulkShardRequest bulkShardRequest,
            Runnable onCompletion
        ) {
            this.fieldInferenceMetadata = fieldInferenceMetadata;
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
            final List<String> inputs = requests.stream().map(FieldInferenceRequest::input).collect(Collectors.toList());
            ActionListener<List<ChunkedInferenceServiceResults>> completionListener = new ActionListener<>() {
                @Override
                public void onResponse(List<ChunkedInferenceServiceResults> results) {
                    for (int i = 0; i < results.size(); i++) {
                        var request = requests.get(i);
                        var result = results.get(i);
                        var acc = inferenceResults.get(request.id);
                        acc.responses.add(new FieldInferenceResponse(request.field, inferenceProvider.model, result));
                    }
                }

                @Override
                public void onFailure(Exception exc) {
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
                }
            };
            inferenceProvider.service()
                .chunkedInfer(
                    inferenceProvider.model(),
                    inputs,
                    Map.of(),
                    InputType.INGEST,
                    new ChunkingOptions(null, null),
                    ActionListener.runAfter(completionListener, onFinish::close)
                );
        }

        /**
         * Applies the {@link FieldInferenceResponseAccumulator} to the provider {@link BulkItemRequest}.
         * If the response contains failures, the bulk item request is mark as failed for the downstream action.
         * Otherwise, the source of the request is augmented with the field inference results.
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
            Map<String, Object> inferenceMap = new LinkedHashMap<>();
            // ignore the existing inference map if any
            newDocMap.put(InferenceResultFieldMapper.NAME, inferenceMap);
            for (FieldInferenceResponse fieldResponse : response.responses()) {
                try {
                    InferenceResultFieldMapper.applyFieldInference(
                        inferenceMap,
                        fieldResponse.field(),
                        fieldResponse.model(),
                        fieldResponse.chunkedResults()
                    );
                } catch (Exception exc) {
                    item.abort(item.index(), exc);
                }
            }
            indexRequest.source(newDocMap);
        }

        private Map<String, List<FieldInferenceRequest>> createFieldInferenceRequests(BulkShardRequest bulkShardRequest) {
            Map<String, List<FieldInferenceRequest>> fieldRequestsMap = new LinkedHashMap<>();
            for (var item : bulkShardRequest.items()) {
                if (item.getPrimaryResponse() != null) {
                    // item was already aborted/processed by a filter in the chain upstream (e.g. security)
                    continue;
                }
                final IndexRequest indexRequest = getIndexRequestOrNull(item.request());
                if (indexRequest == null) {
                    continue;
                }
                final Map<String, Object> docMap = indexRequest.sourceAsMap();
                for (var entry : fieldInferenceMetadata.getFieldInferenceOptions().entrySet()) {
                    String field = entry.getKey();
                    String inferenceId = entry.getValue().inferenceId();
                    var value = XContentMapValues.extractValue(field, docMap);
                    if (value == null) {
                        continue;
                    }
                    if (inferenceResults.get(item.id()) == null) {
                        inferenceResults.set(
                            item.id(),
                            new FieldInferenceResponseAccumulator(
                                item.id(),
                                Collections.synchronizedList(new ArrayList<>()),
                                Collections.synchronizedList(new ArrayList<>())
                            )
                        );
                    }
                    if (value instanceof String valueStr) {
                        List<FieldInferenceRequest> fieldRequests = fieldRequestsMap.computeIfAbsent(inferenceId, k -> new ArrayList<>());
                        fieldRequests.add(new FieldInferenceRequest(item.id(), field, valueStr));
                    } else {
                        inferenceResults.get(item.id()).failures.add(
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
