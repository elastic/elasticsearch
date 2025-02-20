/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextUtils;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.inference.InferencePlugin.INFERENCE_API_FEATURE;

/**
 * A {@link MappedActionFilter} that intercepts {@link BulkShardRequest} to apply inference on fields specified
 * as {@link SemanticTextFieldMapper} in the index mapping. For each semantic text field referencing fields in
 * the request source, we generate embeddings and include the results in the source under the semantic text field
 * name as a {@link SemanticTextField}.
 * This transformation happens on the bulk coordinator node, and the {@link SemanticTextFieldMapper} parses the
 * results during indexing on the shard.
 *
 * TODO: batchSize should be configurable via a cluster setting
 */
public class ShardBulkInferenceActionFilter implements MappedActionFilter {
    protected static final int DEFAULT_BATCH_SIZE = 512;
    private static final Object EXPLICIT_NULL = new Object();
    private static final ChunkedInference EMPTY_CHUNKED_INFERENCE = new EmptyChunkedInference();

    private final ClusterService clusterService;
    private final InferenceServiceRegistry inferenceServiceRegistry;
    private final ModelRegistry modelRegistry;
    private final XPackLicenseState licenseState;
    private final int batchSize;

    public ShardBulkInferenceActionFilter(
        ClusterService clusterService,
        InferenceServiceRegistry inferenceServiceRegistry,
        ModelRegistry modelRegistry,
        XPackLicenseState licenseState
    ) {
        this(clusterService, inferenceServiceRegistry, modelRegistry, licenseState, DEFAULT_BATCH_SIZE);
    }

    public ShardBulkInferenceActionFilter(
        ClusterService clusterService,
        InferenceServiceRegistry inferenceServiceRegistry,
        ModelRegistry modelRegistry,
        XPackLicenseState licenseState,
        int batchSize
    ) {
        this.clusterService = clusterService;
        this.inferenceServiceRegistry = inferenceServiceRegistry;
        this.modelRegistry = modelRegistry;
        this.licenseState = licenseState;
        this.batchSize = batchSize;
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
        if (TransportShardBulkAction.ACTION_NAME.equals(action)) {
            BulkShardRequest bulkShardRequest = (BulkShardRequest) request;
            var fieldInferenceMetadata = bulkShardRequest.consumeInferenceFieldMap();
            if (fieldInferenceMetadata != null && fieldInferenceMetadata.isEmpty() == false) {
                Runnable onInferenceCompletion = () -> chain.proceed(task, action, request, listener);
                processBulkShardRequest(fieldInferenceMetadata, bulkShardRequest, onInferenceCompletion);
                return;
            }
        }
        chain.proceed(task, action, request, listener);
    }

    private void processBulkShardRequest(
        Map<String, InferenceFieldMetadata> fieldInferenceMap,
        BulkShardRequest bulkShardRequest,
        Runnable onCompletion
    ) {
        var index = clusterService.state().getMetadata().index(bulkShardRequest.index());
        boolean useLegacyFormat = InferenceMetadataFieldsMapper.isEnabled(index.getSettings()) == false;
        new AsyncBulkShardInferenceAction(useLegacyFormat, fieldInferenceMap, bulkShardRequest, onCompletion).run();
    }

    private record InferenceProvider(InferenceService service, Model model) {}

    /**
     * A field inference request on a single input.
     * @param index The index of the request in the original bulk request.
     * @param field The target field.
     * @param sourceField The source field.
     * @param input The input to run inference on.
     * @param inputOrder The original order of the input.
     * @param offsetAdjustment The adjustment to apply to the chunk text offsets.
     */
    private record FieldInferenceRequest(int index, String field, String sourceField, String input, int inputOrder, int offsetAdjustment) {}

    /**
     * The field inference response.
     * @param field The target field.
     * @param sourceField The input that was used to run inference.
     * @param input The input that was used to run inference.
     * @param inputOrder The original order of the input.
     * @param offsetAdjustment The adjustment to apply to the chunk text offsets.
     * @param model The model used to run inference.
     * @param chunkedResults The actual results.
     */
    private record FieldInferenceResponse(
        String field,
        String sourceField,
        String input,
        int inputOrder,
        int offsetAdjustment,
        Model model,
        ChunkedInference chunkedResults
    ) {}

    private record FieldInferenceResponseAccumulator(
        int id,
        Map<String, List<FieldInferenceResponse>> responses,
        List<Exception> failures
    ) {
        void addOrUpdateResponse(FieldInferenceResponse response) {
            synchronized (this) {
                var list = responses.computeIfAbsent(response.field, k -> new ArrayList<>());
                list.add(response);
            }
        }

        void addFailure(Exception exc) {
            synchronized (this) {
                failures.add(exc);
            }
        }
    }

    private class AsyncBulkShardInferenceAction implements Runnable {
        private final boolean useLegacyFormat;
        private final Map<String, InferenceFieldMetadata> fieldInferenceMap;
        private final BulkShardRequest bulkShardRequest;
        private final Runnable onCompletion;
        private final AtomicArray<FieldInferenceResponseAccumulator> inferenceResults;

        private AsyncBulkShardInferenceAction(
            boolean useLegacyFormat,
            Map<String, InferenceFieldMetadata> fieldInferenceMap,
            BulkShardRequest bulkShardRequest,
            Runnable onCompletion
        ) {
            this.useLegacyFormat = useLegacyFormat;
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
                ActionListener<UnparsedModel> modelLoadingListener = new ActionListener<>() {
                    @Override
                    public void onResponse(UnparsedModel unparsedModel) {
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
                                for (FieldInferenceRequest request : requests) {
                                    inferenceResults.get(request.index).failures.add(
                                        new ResourceNotFoundException(
                                            "Inference service [{}] not found for field [{}]",
                                            unparsedModel.service(),
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
                            for (FieldInferenceRequest request : requests) {
                                Exception failure;
                                if (ExceptionsHelper.unwrap(exc, ResourceNotFoundException.class) instanceof ResourceNotFoundException) {
                                    failure = new ResourceNotFoundException(
                                        "Inference id [{}] not found for field [{}]",
                                        inferenceId,
                                        request.field
                                    );
                                } else {
                                    failure = new ElasticsearchException(
                                        "Error loading inference for inference id [{}] on field [{}]",
                                        exc,
                                        inferenceId,
                                        request.field
                                    );
                                }
                                inferenceResults.get(request.index).failures.add(failure);
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
            ActionListener<List<ChunkedInference>> completionListener = new ActionListener<>() {
                @Override
                public void onResponse(List<ChunkedInference> results) {
                    try {
                        var requestsIterator = requests.iterator();
                        for (ChunkedInference result : results) {
                            var request = requestsIterator.next();
                            var acc = inferenceResults.get(request.index);
                            if (result instanceof ChunkedInferenceError error) {
                                acc.addFailure(
                                    new ElasticsearchException(
                                        "Exception when running inference id [{}] on field [{}]",
                                        error.exception(),
                                        inferenceProvider.model.getInferenceEntityId(),
                                        request.field
                                    )
                                );
                            } else {
                                acc.addOrUpdateResponse(
                                    new FieldInferenceResponse(
                                        request.field(),
                                        request.sourceField(),
                                        request.input(),
                                        request.inputOrder(),
                                        request.offsetAdjustment(),
                                        inferenceProvider.model,
                                        result
                                    )
                                );
                            }
                        }
                    } finally {
                        onFinish();
                    }
                }

                @Override
                public void onFailure(Exception exc) {
                    try {
                        for (FieldInferenceRequest request : requests) {
                            addInferenceResponseFailure(
                                request.index,
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
                .chunkedInfer(inferenceProvider.model(), null, inputs, Map.of(), InputType.INGEST, TimeValue.MAX_VALUE, completionListener);
        }

        private FieldInferenceResponseAccumulator ensureResponseAccumulatorSlot(int id) {
            FieldInferenceResponseAccumulator acc = inferenceResults.get(id);
            if (acc == null) {
                acc = new FieldInferenceResponseAccumulator(id, new HashMap<>(), new ArrayList<>());
                inferenceResults.set(id, acc);
            }
            return acc;
        }

        private void addInferenceResponseFailure(int id, Exception failure) {
            var acc = ensureResponseAccumulatorSlot(id);
            acc.addFailure(failure);
        }

        /**
         * Applies the {@link FieldInferenceResponseAccumulator} to the provided {@link BulkItemRequest}.
         * If the response contains failures, the bulk item request is marked as failed for the downstream action.
         * Otherwise, the source of the request is augmented with the field inference results.
         */
        private void applyInferenceResponses(BulkItemRequest item, FieldInferenceResponseAccumulator response) throws IOException {
            if (response.failures().isEmpty() == false) {
                for (var failure : response.failures()) {
                    item.abort(item.index(), failure);
                }
                return;
            }

            final IndexRequest indexRequest = getIndexRequestOrNull(item.request());
            var newDocMap = indexRequest.sourceAsMap();
            Map<String, Object> inferenceFieldsMap = new HashMap<>();
            for (var entry : response.responses.entrySet()) {
                var fieldName = entry.getKey();
                var responses = entry.getValue();
                Model model = null;

                InferenceFieldMetadata inferenceFieldMetadata = fieldInferenceMap.get(fieldName);
                if (inferenceFieldMetadata == null) {
                    throw new IllegalStateException("No inference field metadata for field [" + fieldName + "]");
                }

                // ensure that the order in the original field is consistent in case of multiple inputs
                Collections.sort(responses, Comparator.comparingInt(FieldInferenceResponse::inputOrder));
                Map<String, List<SemanticTextField.Chunk>> chunkMap = new LinkedHashMap<>();
                for (var resp : responses) {
                    // Get the first non-null model from the response list
                    if (model == null) {
                        model = resp.model;
                    }

                    var lst = chunkMap.computeIfAbsent(resp.sourceField, k -> new ArrayList<>());
                    lst.addAll(
                        SemanticTextField.toSemanticTextFieldChunks(
                            resp.input,
                            resp.offsetAdjustment,
                            resp.chunkedResults,
                            indexRequest.getContentType(),
                            useLegacyFormat
                        )
                    );
                }

                List<String> inputs = responses.stream()
                    .filter(r -> r.sourceField().equals(fieldName))
                    .map(r -> r.input)
                    .collect(Collectors.toList());

                // The model can be null if we are only processing update requests that clear inference results. This is ok because we will
                // merge in the field's existing model settings on the data node.
                var result = new SemanticTextField(
                    useLegacyFormat,
                    fieldName,
                    useLegacyFormat ? inputs : null,
                    new SemanticTextField.InferenceResult(
                        inferenceFieldMetadata.getInferenceId(),
                        model != null ? new MinimalServiceSettings(model) : null,
                        chunkMap
                    ),
                    indexRequest.getContentType()
                );

                if (useLegacyFormat) {
                    SemanticTextUtils.insertValue(fieldName, newDocMap, result);
                } else {
                    inferenceFieldsMap.put(fieldName, result);
                }
            }
            if (useLegacyFormat == false) {
                newDocMap.put(InferenceMetadataFieldsMapper.NAME, inferenceFieldsMap);
            }
            indexRequest.source(newDocMap, indexRequest.getContentType());
        }

        /**
         * Register a {@link FieldInferenceRequest} for every non-empty field referencing an inference ID in the index.
         * If results are already populated for fields in the original index request, the inference request for this specific
         * field is skipped, and the existing results remain unchanged.
         * Validation of inference ID and model settings occurs in the {@link SemanticTextFieldMapper} during field indexing,
         * where an error will be thrown if they mismatch or if the content is malformed.
         * <p>
         * TODO: We should validate the settings for pre-existing results here and apply the inference only if they differ?
         */
        private Map<String, List<FieldInferenceRequest>> createFieldInferenceRequests(BulkShardRequest bulkShardRequest) {
            Map<String, List<FieldInferenceRequest>> fieldRequestsMap = new LinkedHashMap<>();
            for (int itemIndex = 0; itemIndex < bulkShardRequest.items().length; itemIndex++) {
                var item = bulkShardRequest.items()[itemIndex];
                if (item.getPrimaryResponse() != null) {
                    // item was already aborted/processed by a filter in the chain upstream (e.g. security)
                    continue;
                }
                boolean isUpdateRequest = false;
                final IndexRequest indexRequest;
                if (item.request() instanceof IndexRequest ir) {
                    indexRequest = ir;
                } else if (item.request() instanceof UpdateRequest updateRequest) {
                    isUpdateRequest = true;
                    if (updateRequest.script() != null) {
                        addInferenceResponseFailure(
                            itemIndex,
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
                for (var entry : fieldInferenceMap.values()) {
                    String field = entry.getName();
                    String inferenceId = entry.getInferenceId();

                    if (useLegacyFormat) {
                        var originalFieldValue = XContentMapValues.extractValue(field, docMap);
                        if (originalFieldValue instanceof Map || (originalFieldValue == null && entry.getSourceFields().length == 1)) {
                            // Inference has already been computed, or there is no inference required.
                            continue;
                        }
                    } else {
                        var inferenceMetadataFieldsValue = XContentMapValues.extractValue(
                            InferenceMetadataFieldsMapper.NAME + "." + field,
                            docMap,
                            EXPLICIT_NULL
                        );
                        if (inferenceMetadataFieldsValue != null) {
                            // Inference has already been computed
                            continue;
                        }
                    }

                    int order = 0;
                    for (var sourceField : entry.getSourceFields()) {
                        var valueObj = XContentMapValues.extractValue(sourceField, docMap, EXPLICIT_NULL);
                        if (useLegacyFormat == false && isUpdateRequest && valueObj == EXPLICIT_NULL) {
                            /**
                             * It's an update request, and the source field is explicitly set to null,
                             * so we need to propagate this information to the inference fields metadata
                             * to overwrite any inference previously computed on the field.
                             * This ensures that the field is treated as intentionally cleared,
                             * preventing any unintended carryover of prior inference results.
                             */
                            var slot = ensureResponseAccumulatorSlot(itemIndex);
                            slot.addOrUpdateResponse(
                                new FieldInferenceResponse(field, sourceField, null, order++, 0, null, EMPTY_CHUNKED_INFERENCE)
                            );
                            continue;
                        }
                        if (valueObj == null || valueObj == EXPLICIT_NULL) {
                            if (isUpdateRequest && useLegacyFormat) {
                                addInferenceResponseFailure(
                                    itemIndex,
                                    new ElasticsearchStatusException(
                                        "Field [{}] must be specified on an update request to calculate inference for field [{}]",
                                        RestStatus.BAD_REQUEST,
                                        sourceField,
                                        field
                                    )
                                );
                                break;
                            }
                            continue;
                        }
                        ensureResponseAccumulatorSlot(itemIndex);
                        final List<String> values;
                        try {
                            values = SemanticTextUtils.nodeStringValues(field, valueObj);
                        } catch (Exception exc) {
                            addInferenceResponseFailure(itemIndex, exc);
                            break;
                        }

                        if (INFERENCE_API_FEATURE.check(licenseState) == false) {
                            addInferenceResponseFailure(itemIndex, LicenseUtils.newComplianceException(XPackField.INFERENCE));
                            break;
                        }

                        List<FieldInferenceRequest> fieldRequests = fieldRequestsMap.computeIfAbsent(inferenceId, k -> new ArrayList<>());
                        int offsetAdjustment = 0;
                        for (String v : values) {
                            fieldRequests.add(new FieldInferenceRequest(itemIndex, field, sourceField, v, order++, offsetAdjustment));

                            // When using the inference metadata fields format, all the input values are concatenated so that the
                            // chunk text offsets are expressed in the context of a single string. Calculate the offset adjustment
                            // to apply to account for this.
                            offsetAdjustment += v.length() + 1; // Add one for separator char length
                        }
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

    private static class EmptyChunkedInference implements ChunkedInference {
        @Override
        public Iterator<Chunk> chunksAsMatchedTextAndByteReference(XContent xcontent) {
            return Collections.emptyIterator();
        }
    }
}
