/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.inference.InferenceException;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
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
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.toSemanticTextFieldChunks;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.toSemanticTextFieldChunksLegacy;

/**
 * A {@link MappedActionFilter} that intercepts {@link BulkShardRequest} to apply inference on fields specified
 * as {@link SemanticTextFieldMapper} in the index mapping. For each semantic text field referencing fields in
 * the request source, we generate embeddings and include the results in the source under the semantic text field
 * name as a {@link SemanticTextField}.
 * This transformation happens on the bulk coordinator node, and the {@link SemanticTextFieldMapper} parses the
 * results during indexing on the shard.
 *
 */
public class ShardBulkInferenceActionFilter implements MappedActionFilter {
    private static final ByteSizeValue DEFAULT_BATCH_SIZE = ByteSizeValue.ofMb(1);

    /**
     * Defines the cumulative size limit of input data before triggering a batch inference call.
     * This setting controls how much data can be accumulated before an inference request is sent in batch.
     */
    public static Setting<ByteSizeValue> INDICES_INFERENCE_BATCH_SIZE = Setting.byteSizeSetting(
        "indices.inference.batch_size",
        DEFAULT_BATCH_SIZE,
        ByteSizeValue.ONE,
        ByteSizeValue.ofMb(100),
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    private static final Object EXPLICIT_NULL = new Object();
    private static final ChunkedInference EMPTY_CHUNKED_INFERENCE = new EmptyChunkedInference();

    private final ClusterService clusterService;
    private final InferenceServiceRegistry inferenceServiceRegistry;
    private final ModelRegistry modelRegistry;
    private final XPackLicenseState licenseState;
    private final IndexingPressure indexingPressure;
    private volatile long batchSizeInBytes;

    public ShardBulkInferenceActionFilter(
        ClusterService clusterService,
        InferenceServiceRegistry inferenceServiceRegistry,
        ModelRegistry modelRegistry,
        XPackLicenseState licenseState,
        IndexingPressure indexingPressure
    ) {
        this.clusterService = clusterService;
        this.inferenceServiceRegistry = inferenceServiceRegistry;
        this.modelRegistry = modelRegistry;
        this.licenseState = licenseState;
        this.indexingPressure = indexingPressure;
        this.batchSizeInBytes = INDICES_INFERENCE_BATCH_SIZE.get(clusterService.getSettings()).getBytes();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(INDICES_INFERENCE_BATCH_SIZE, this::setBatchSize);
    }

    private void setBatchSize(ByteSizeValue newBatchSize) {
        batchSizeInBytes = newBatchSize.getBytes();
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
                // Maintain coordinating indexing pressure from inference until the indexing operations are complete
                IndexingPressure.Coordinating coordinatingIndexingPressure = indexingPressure.createCoordinatingOperation(false);
                Runnable onInferenceCompletion = () -> chain.proceed(
                    task,
                    action,
                    request,
                    ActionListener.releaseAfter(listener, coordinatingIndexingPressure)
                );
                processBulkShardRequest(fieldInferenceMetadata, bulkShardRequest, onInferenceCompletion, coordinatingIndexingPressure);
                return;
            }
        }
        chain.proceed(task, action, request, listener);
    }

    private void processBulkShardRequest(
        Map<String, InferenceFieldMetadata> fieldInferenceMap,
        BulkShardRequest bulkShardRequest,
        Runnable onCompletion,
        IndexingPressure.Coordinating coordinatingIndexingPressure
    ) {
        final ProjectMetadata project = clusterService.state().getMetadata().getProject();
        var index = project.index(bulkShardRequest.index());
        boolean useLegacyFormat = InferenceMetadataFieldsMapper.isEnabled(index.getSettings()) == false;
        new AsyncBulkShardInferenceAction(useLegacyFormat, fieldInferenceMap, bulkShardRequest, onCompletion, coordinatingIndexingPressure)
            .run();
    }

    private record InferenceProvider(InferenceService service, Model model) {}

    /**
     * A field inference request on a single input.
     * @param bulkItemIndex The index of the item in the original bulk request.
     * @param field The target field.
     * @param sourceField The source field.
     * @param input The input to run inference on.
     * @param inputOrder The original order of the input.
     * @param offsetAdjustment The adjustment to apply to the chunk text offsets.
     * @param chunkingSettings Additional explicitly specified chunking settings, or null to use model defaults
     */
    private record FieldInferenceRequest(
        int bulkItemIndex,
        String field,
        String sourceField,
        String input,
        int inputOrder,
        int offsetAdjustment,
        ChunkingSettings chunkingSettings
    ) {}

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
        private final IndexingPressure.Coordinating coordinatingIndexingPressure;

        private AsyncBulkShardInferenceAction(
            boolean useLegacyFormat,
            Map<String, InferenceFieldMetadata> fieldInferenceMap,
            BulkShardRequest bulkShardRequest,
            Runnable onCompletion,
            IndexingPressure.Coordinating coordinatingIndexingPressure
        ) {
            this.useLegacyFormat = useLegacyFormat;
            this.fieldInferenceMap = fieldInferenceMap;
            this.bulkShardRequest = bulkShardRequest;
            this.inferenceResults = new AtomicArray<>(bulkShardRequest.items().length);
            this.onCompletion = onCompletion;
            this.coordinatingIndexingPressure = coordinatingIndexingPressure;
        }

        @Override
        public void run() {
            executeNext(0);
        }

        private void executeNext(int itemOffset) {
            if (itemOffset >= bulkShardRequest.items().length) {
                onCompletion.run();
                return;
            }

            var items = bulkShardRequest.items();
            Map<String, List<FieldInferenceRequest>> fieldRequestsMap = new HashMap<>();
            long totalInputLength = 0;
            int itemIndex = itemOffset;
            while (itemIndex < items.length && totalInputLength < batchSizeInBytes) {
                var item = items[itemIndex];
                totalInputLength += addFieldInferenceRequests(item, itemIndex, fieldRequestsMap);
                itemIndex += 1;
            }
            int nextItemOffset = itemIndex;
            Runnable onInferenceCompletion = () -> {
                try {
                    for (int i = itemOffset; i < nextItemOffset; i++) {
                        var result = inferenceResults.get(i);
                        if (result == null) {
                            continue;
                        }
                        var item = items[i];
                        try {
                            applyInferenceResponses(item, result);
                        } catch (Exception exc) {
                            item.abort(bulkShardRequest.index(), exc);
                        }
                        // we don't need to keep the inference results around
                        inferenceResults.set(i, null);
                    }
                } finally {
                    executeNext(nextItemOffset);
                }
            };

            try (var releaseOnFinish = new RefCountingRunnable(onInferenceCompletion)) {
                for (var entry : fieldRequestsMap.entrySet()) {
                    executeChunkedInferenceAsync(entry.getKey(), null, entry.getValue(), releaseOnFinish.acquire());
                }
            }
        }

        private void executeChunkedInferenceAsync(
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
                            executeChunkedInferenceAsync(inferenceId, provider, requests, onFinish);
                        } else {
                            try (onFinish) {
                                for (FieldInferenceRequest request : requests) {
                                    inferenceResults.get(request.bulkItemIndex).failures.add(
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
                                    failure = new InferenceException(
                                        "Error loading inference for inference id [{}] on field [{}]",
                                        exc,
                                        inferenceId,
                                        request.field
                                    );
                                }
                                inferenceResults.get(request.bulkItemIndex).failures.add(failure);
                            }
                        }
                    }
                };
                modelRegistry.getModelWithSecrets(inferenceId, modelLoadingListener);
                return;
            }
            final List<ChunkInferenceInput> inputs = requests.stream()
                .map(r -> new ChunkInferenceInput(r.input, r.chunkingSettings))
                .collect(Collectors.toList());

            ActionListener<List<ChunkedInference>> completionListener = new ActionListener<>() {
                @Override
                public void onResponse(List<ChunkedInference> results) {
                    try (onFinish) {
                        var requestsIterator = requests.iterator();
                        for (ChunkedInference result : results) {
                            var request = requestsIterator.next();
                            var acc = inferenceResults.get(request.bulkItemIndex);
                            if (result instanceof ChunkedInferenceError error) {
                                acc.addFailure(
                                    new InferenceException(
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
                                        useLegacyFormat ? request.input() : null,
                                        request.inputOrder(),
                                        request.offsetAdjustment(),
                                        inferenceProvider.model,
                                        result
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
                            addInferenceResponseFailure(
                                request.bulkItemIndex,
                                new InferenceException(
                                    "Exception when running inference id [{}] on field [{}]",
                                    exc,
                                    inferenceProvider.model.getInferenceEntityId(),
                                    request.field
                                )
                            );
                        }
                    }
                }
            };
            inferenceProvider.service()
                .chunkedInfer(
                    inferenceProvider.model(),
                    null,
                    inputs,
                    Map.of(),
                    InputType.INTERNAL_INGEST,
                    TimeValue.MAX_VALUE,
                    completionListener
                );
        }

        /**
         * Adds all inference requests associated with their respective inference IDs to the given {@code requestsMap}
         * for the specified {@code item}.
         *
         * @param item       The bulk request item to process.
         * @param itemIndex  The position of the item within the original bulk request.
         * @param requestsMap A map storing inference requests, where each key is an inference ID,
         *                    and the value is a list of associated {@link FieldInferenceRequest} objects.
         * @return The total content length of all newly added requests, or {@code 0} if no requests were added.
         */
        private long addFieldInferenceRequests(BulkItemRequest item, int itemIndex, Map<String, List<FieldInferenceRequest>> requestsMap) {
            boolean isUpdateRequest = false;
            final IndexRequestWithIndexingPressure indexRequest;
            if (item.request() instanceof IndexRequest ir) {
                indexRequest = new IndexRequestWithIndexingPressure(ir);
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
                    return 0;
                }
                indexRequest = new IndexRequestWithIndexingPressure(updateRequest.doc());
            } else {
                // ignore delete request
                return 0;
            }

            final Map<String, Object> docMap = indexRequest.getIndexRequest().sourceAsMap();
            long inputLength = 0;
            for (var entry : fieldInferenceMap.values()) {
                String field = entry.getName();
                String inferenceId = entry.getInferenceId();
                ChunkingSettings chunkingSettings = ChunkingSettingsBuilder.fromMap(entry.getChunkingSettings(), false);

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
                        if (incrementIndexingPressure(indexRequest, itemIndex) == false) {
                            return inputLength;
                        }

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

                    var slot = ensureResponseAccumulatorSlot(itemIndex);
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

                    List<FieldInferenceRequest> requests = requestsMap.computeIfAbsent(inferenceId, k -> new ArrayList<>());
                    int offsetAdjustment = 0;
                    for (String v : values) {
                        if (incrementIndexingPressure(indexRequest, itemIndex) == false) {
                            return inputLength;
                        }

                        if (v.isBlank()) {
                            slot.addOrUpdateResponse(
                                new FieldInferenceResponse(field, sourceField, v, order++, 0, null, EMPTY_CHUNKED_INFERENCE)
                            );
                        } else {
                            requests.add(
                                new FieldInferenceRequest(itemIndex, field, sourceField, v, order++, offsetAdjustment, chunkingSettings)
                            );
                            inputLength += v.length();
                        }

                        // When using the inference metadata fields format, all the input values are concatenated so that the
                        // chunk text offsets are expressed in the context of a single string. Calculate the offset adjustment
                        // to apply to account for this.
                        offsetAdjustment += v.length() + 1; // Add one for separator char length
                    }
                }
            }

            return inputLength;
        }

        private static class IndexRequestWithIndexingPressure {
            private final IndexRequest indexRequest;
            private boolean indexingPressureIncremented;

            private IndexRequestWithIndexingPressure(IndexRequest indexRequest) {
                this.indexRequest = indexRequest;
                this.indexingPressureIncremented = false;
            }

            private IndexRequest getIndexRequest() {
                return indexRequest;
            }

            private boolean isIndexingPressureIncremented() {
                return indexingPressureIncremented;
            }

            private void setIndexingPressureIncremented() {
                this.indexingPressureIncremented = true;
            }
        }

        private boolean incrementIndexingPressure(IndexRequestWithIndexingPressure indexRequest, int itemIndex) {
            boolean success = true;
            if (indexRequest.isIndexingPressureIncremented() == false) {
                try {
                    // Track operation count as one operation per document source update
                    coordinatingIndexingPressure.increment(1, indexRequest.getIndexRequest().source().ramBytesUsed());
                    indexRequest.setIndexingPressureIncremented();
                } catch (EsRejectedExecutionException e) {
                    addInferenceResponseFailure(
                        itemIndex,
                        new InferenceException(
                            "Insufficient memory available to update source on document [" + indexRequest.getIndexRequest().id() + "]",
                            e
                        )
                    );
                    success = false;
                }
            }

            return success;
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
                    var chunks = useLegacyFormat
                        ? toSemanticTextFieldChunksLegacy(resp.input, resp.chunkedResults, indexRequest.getContentType())
                        : toSemanticTextFieldChunks(resp.offsetAdjustment, resp.chunkedResults, indexRequest.getContentType());
                    lst.addAll(chunks);
                }

                List<String> inputs = useLegacyFormat
                    ? responses.stream().filter(r -> r.sourceField().equals(fieldName)).map(r -> r.input).collect(Collectors.toList())
                    : null;

                // The model can be null if we are only processing update requests that clear inference results. This is ok because we will
                // merge in the field's existing model settings on the data node.
                var result = new SemanticTextField(
                    useLegacyFormat,
                    fieldName,
                    inputs,
                    new SemanticTextField.InferenceResult(
                        inferenceFieldMetadata.getInferenceId(),
                        model != null ? new MinimalServiceSettings(model) : null,
                        ChunkingSettingsBuilder.fromMap(inferenceFieldMetadata.getChunkingSettings(), false),
                        inferenceFieldMetadata.getIndexOptions(),
                        chunkMap
                    ),
                    indexRequest.getContentType()
                );
                inferenceFieldsMap.put(fieldName, result);
            }

            BytesReference originalSource = indexRequest.source();
            if (useLegacyFormat) {
                var newDocMap = indexRequest.sourceAsMap();
                for (var entry : inferenceFieldsMap.entrySet()) {
                    SemanticTextUtils.insertValue(entry.getKey(), newDocMap, entry.getValue());
                }
                indexRequest.source(newDocMap, indexRequest.getContentType());
            } else {
                try (XContentBuilder builder = XContentBuilder.builder(indexRequest.getContentType().xContent())) {
                    appendSourceAndInferenceMetadata(builder, indexRequest.source(), indexRequest.getContentType(), inferenceFieldsMap);
                    indexRequest.source(builder);
                }
            }
            long modifiedSourceSize = indexRequest.source().ramBytesUsed();

            // Add the indexing pressure from the source modifications.
            // Don't increment operation count because we count one source update as one operation, and we already accounted for those
            // in addFieldInferenceRequests.
            try {
                coordinatingIndexingPressure.increment(0, modifiedSourceSize - originalSource.ramBytesUsed());
            } catch (EsRejectedExecutionException e) {
                indexRequest.source(originalSource, indexRequest.getContentType());
                item.abort(
                    item.index(),
                    new InferenceException(
                        "Insufficient memory available to insert inference results into document [" + indexRequest.id() + "]",
                        e
                    )
                );
            }
        }
    }

    /**
     * Appends the original source and the new inference metadata field directly to the provided
     * {@link XContentBuilder}, avoiding the need to materialize the original source as a {@link Map}.
     */
    private static void appendSourceAndInferenceMetadata(
        XContentBuilder builder,
        BytesReference source,
        XContentType xContentType,
        Map<String, Object> inferenceFieldsMap
    ) throws IOException {
        builder.startObject();

        // append the original source
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)) {
            // skip start object
            parser.nextToken();
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                builder.copyCurrentStructure(parser);
            }
        }

        // add the inference metadata field
        builder.field(InferenceMetadataFieldsMapper.NAME);
        try (XContentParser parser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, inferenceFieldsMap)) {
            builder.copyCurrentStructure(parser);
        }

        builder.endObject();
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
        public Iterator<Chunk> chunksAsByteReference(XContent xcontent) {
            return Collections.emptyIterator();
        }
    }
}
