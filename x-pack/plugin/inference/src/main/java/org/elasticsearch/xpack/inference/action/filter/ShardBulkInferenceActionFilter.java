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
import org.elasticsearch.action.index.IndexSource;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.EmbeddingResults;
import org.elasticsearch.xpack.inference.InferenceException;
import org.elasticsearch.xpack.inference.InferenceLicenceCheck;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.elasticsearch.inference.telemetry.InferenceStats.SEMANTIC_TEXT_USE_CASE;

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
    private static final Logger logger = LogManager.getLogger(ShardBulkInferenceActionFilter.class);

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
    private final InferenceStats inferenceStats;
    private volatile long batchSizeInBytes;

    public ShardBulkInferenceActionFilter(
        ClusterService clusterService,
        InferenceServiceRegistry inferenceServiceRegistry,
        ModelRegistry modelRegistry,
        XPackLicenseState licenseState,
        IndexingPressure indexingPressure,
        InferenceStats inferenceStats
    ) {
        this.clusterService = clusterService;
        this.inferenceServiceRegistry = inferenceServiceRegistry;
        this.modelRegistry = modelRegistry;
        this.licenseState = licenseState;
        this.indexingPressure = indexingPressure;
        this.inferenceStats = inferenceStats;
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
        new AsyncBulkShardInferenceAction(index, fieldInferenceMap, bulkShardRequest, onCompletion, coordinatingIndexingPressure).run();
    }

    private record InferenceProvider(InferenceService service, Model model) {}

    private record FieldInferenceResponseAccumulator(
        int id,
        Map<String, List<FieldInferenceResponse>> responses,
        AtomicReference<Exception> failure
    ) {
        private FieldInferenceResponseAccumulator(int id) {
            this(id, new HashMap<>(), new AtomicReference<>(null));
        }

        void addOrUpdateResponse(FieldInferenceResponse response) {
            synchronized (this) {
                var list = responses.computeIfAbsent(response.field(), k -> new ArrayList<>());
                list.add(response);
            }
        }

        void setFailure(Exception exc) {
            // Only keep the first failure and discard all others
            failure.compareAndSet(null, exc);
        }
    }

    private class AsyncBulkShardInferenceAction implements Runnable {
        private final boolean useLegacyFormat;
        private final IndexVersion indexVersion;
        private final Map<String, InferenceFieldMetadata> fieldInferenceMap;
        private final BulkShardRequest bulkShardRequest;
        private final Runnable onCompletion;
        private final AtomicArray<FieldInferenceResponseAccumulator> inferenceResults;
        private final IndexingPressure.Coordinating coordinatingIndexingPressure;

        private AsyncBulkShardInferenceAction(
            IndexMetadata indexMetadata,
            Map<String, InferenceFieldMetadata> fieldInferenceMap,
            BulkShardRequest bulkShardRequest,
            Runnable onCompletion,
            IndexingPressure.Coordinating coordinatingIndexingPressure
        ) {
            this.useLegacyFormat = InferenceMetadataFieldsMapper.isEnabled(indexMetadata.getSettings()) == false;
            this.indexVersion = indexMetadata.getCreationVersion();
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
                    startInferenceAsync(entry.getKey(), entry.getValue(), releaseOnFinish.acquire());
                }
            }
        }

        private void startInferenceAsync(final String inferenceId, final List<FieldInferenceRequest> requests, final Releasable onFinish) {
            ActionListener<UnparsedModel> modelLoadingListener = ActionListener.wrap(unparsedModel -> {
                var service = inferenceServiceRegistry.getService(unparsedModel.service());
                if (service.isEmpty() == false) {
                    var provider = new InferenceProvider(service.get(), service.get().parsePersistedConfig(unparsedModel));
                    executeInferenceAsync(provider, requests, onFinish);
                } else {
                    try (onFinish) {
                        for (FieldInferenceRequest request : requests) {
                            inferenceResults.get(request.bulkItemIndex())
                                .setFailure(
                                    new ResourceNotFoundException(
                                        "Inference service [{}] not found for field [{}]",
                                        unparsedModel.service(),
                                        request.field()
                                    )
                                );
                        }
                    }
                }
            }, exc -> {
                try (onFinish) {
                    for (FieldInferenceRequest request : requests) {
                        Exception failure;
                        if (ExceptionsHelper.unwrap(exc, ResourceNotFoundException.class) instanceof ResourceNotFoundException) {
                            failure = new ResourceNotFoundException(
                                "Inference id [{}] not found for field [{}]",
                                inferenceId,
                                request.field()
                            );
                        } else {
                            failure = new InferenceException(
                                "Error loading inference for inference id [{}] on field [{}]",
                                exc,
                                inferenceId,
                                request.field()
                            );
                        }
                        inferenceResults.get(request.bulkItemIndex()).setFailure(failure);
                    }

                    if (ExceptionsHelper.status(exc).getStatus() >= 500) {
                        List<String> fields = requests.stream().map(FieldInferenceRequest::field).distinct().toList();
                        logger.warn("Error loading inference for inference id [" + inferenceId + "] on fields " + fields, exc);
                    }
                }
            });

            modelRegistry.getModelWithSecrets(inferenceId, modelLoadingListener);
        }

        private void executeInferenceAsync(InferenceProvider inferenceProvider, List<FieldInferenceRequest> requests, Releasable onFinish) {
            if (InferenceLicenceCheck.isServiceLicenced(inferenceProvider.service.name(), licenseState) == false) {
                try (onFinish) {
                    var complianceException = InferenceLicenceCheck.complianceException(inferenceProvider.service.name());
                    failAllInferenceRequests(requests, r -> complianceException);
                    return;
                }
            }

            List<ChunkedStringFieldInferenceRequest> chunkedRequests = new ArrayList<>();
            List<InferenceStringFieldInferenceRequest> embeddingRequests = new ArrayList<>();
            for (var r : requests) {
                if (r instanceof ChunkedStringFieldInferenceRequest c) {
                    chunkedRequests.add(c);
                } else if (r instanceof InferenceStringFieldInferenceRequest e) {
                    embeddingRequests.add(e);
                } else {
                    throw new IllegalStateException("Unexpected field inference request type [" + r.getClass().getName() + "]");
                }
            }

            // Fan out to the chunked and embedding arms under a child counter that closes onFinish when both arms complete.
            // Acquire is guarded by the non-empty checks so an absent arm does not leak a ref and orphan onInferenceCompletion.
            try (var armCounter = new RefCountingRunnable(onFinish::close)) {
                if (chunkedRequests.isEmpty() == false) {
                    executeChunkedInferenceAsync(inferenceProvider, chunkedRequests, armCounter.acquire());
                }
                if (embeddingRequests.isEmpty() == false) {
                    executeEmbeddingInferenceAsync(inferenceProvider, embeddingRequests, armCounter.acquire());
                }
            }
        }

        private void executeChunkedInferenceAsync(
            final InferenceProvider inferenceProvider,
            final List<ChunkedStringFieldInferenceRequest> requests,
            final Releasable onFinish
        ) {
            final List<ChunkInferenceInput> inputs = requests.stream()
                .map(
                    r -> new ChunkInferenceInput(
                        new InferenceStringGroup(singletonList(new InferenceString(DataType.TEXT, r.input()))),
                        r.chunkingSettings()
                    )
                )
                .collect(Collectors.toList());

            ActionListener<List<ChunkedInference>> completionListener = ActionListener.wrap(results -> {
                try (onFinish) {
                    var requestsIterator = requests.iterator();
                    int success = 0;
                    for (ChunkedInference result : results) {
                        var request = requestsIterator.next();
                        var acc = inferenceResults.get(request.bulkItemIndex());
                        if (result instanceof ChunkedInferenceError error) {
                            recordRequestCountMetrics(inferenceProvider.model, 1, error.exception());
                            acc.setFailure(
                                new InferenceException(
                                    "Exception when running inference id [{}] on field [{}]",
                                    error.exception(),
                                    inferenceProvider.model.getInferenceEntityId(),
                                    request.field()
                                )
                            );
                        } else {
                            success++;
                            acc.addOrUpdateResponse(
                                new ChunkedStringFieldInferenceResponse(
                                    request.field(),
                                    request.sourceField(),
                                    useLegacyFormat ? request.input() : null,
                                    request.fieldInputOrder(),
                                    request.offsetAdjustment(),
                                    inferenceProvider.model,
                                    result
                                )
                            );
                        }
                    }
                    if (success > 0) {
                        recordRequestCountMetrics(inferenceProvider.model, success, null);
                    }
                }
            }, exc -> {
                try (onFinish) {
                    onInferenceServiceFailure(inferenceProvider, requests, exc);
                }
            });

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

        private void executeEmbeddingInferenceAsync(
            final InferenceProvider inferenceProvider,
            final List<InferenceStringFieldInferenceRequest> requests,
            final Releasable onFinish
        ) {
            final List<InferenceStringGroup> inputs = requests.stream().map(r -> new InferenceStringGroup(r.input())).toList();

            ActionListener<InferenceServiceResults> completionListener = ActionListener.wrap(results -> {
                try (onFinish) {
                    if (results instanceof EmbeddingResults<?> == false) {
                        var typeMismatchException = new IllegalStateException(
                            "Unexpected inference result type ["
                                + results.getClass().getName()
                                + "] for inference id ["
                                + inferenceProvider.model.getInferenceEntityId()
                                + "]"
                        );
                        recordRequestCountMetrics(inferenceProvider.model, requests.size(), typeMismatchException);
                        failAllInferenceRequests(
                            requests,
                            r -> new InferenceException(
                                "Unexpected state when running inference on field [{}]",
                                typeMismatchException,
                                r.field()
                            )
                        );
                        return;
                    }

                    EmbeddingResults<?> embeddingResults = (EmbeddingResults<?>) results;
                    List<? extends EmbeddingResults.Embedding<?>> embeddings = embeddingResults.embeddings();
                    if (embeddings.size() != requests.size()) {
                        var sizeMismatchException = new IllegalStateException(
                            "Inference result count ["
                                + embeddings.size()
                                + "] does not match request count ["
                                + requests.size()
                                + "] for inference id ["
                                + inferenceProvider.model.getInferenceEntityId()
                                + "]"
                        );
                        recordRequestCountMetrics(inferenceProvider.model, requests.size(), sizeMismatchException);
                        failAllInferenceRequests(
                            requests,
                            r -> new InferenceException(
                                "Unexpected state when running inference on field [{}]",
                                sizeMismatchException,
                                r.field()
                            )
                        );
                        return;
                    }

                    var requestsIterator = requests.iterator();
                    for (var embedding : embeddings) {
                        var request = requestsIterator.next();
                        inferenceResults.get(request.bulkItemIndex())
                            .addOrUpdateResponse(
                                new InferenceStringFieldInferenceResponse(
                                    request.field(),
                                    request.sourceField(),
                                    request.fieldInputOrder(),
                                    request.sourceFieldInputIndex(),
                                    inferenceProvider.model,
                                    embedding
                                )
                            );
                    }
                    recordRequestCountMetrics(inferenceProvider.model, requests.size(), null);
                }
            }, exc -> {
                try (onFinish) {
                    onInferenceServiceFailure(inferenceProvider, requests, exc);
                }
            });

            EmbeddingRequest embeddingRequest = new EmbeddingRequest(inputs, InputType.INTERNAL_INGEST, Map.of());
            inferenceProvider.service()
                .embeddingInfer(inferenceProvider.model(), embeddingRequest, TimeValue.MAX_VALUE, completionListener);
        }

        private void failAllInferenceRequests(
            List<? extends FieldInferenceRequest> requests,
            Function<FieldInferenceRequest, Exception> failure
        ) {
            for (FieldInferenceRequest request : requests) {
                setInferenceResponseFailure(request.bulkItemIndex(), failure.apply(request));
            }
        }

        private void onInferenceServiceFailure(
            InferenceProvider inferenceProvider,
            List<? extends FieldInferenceRequest> requests,
            Exception exc
        ) {
            recordRequestCountMetrics(inferenceProvider.model, requests.size(), exc);
            failAllInferenceRequests(
                requests,
                r -> new InferenceException(
                    "Exception when running inference id [{}] on field [{}]",
                    exc,
                    inferenceProvider.model.getInferenceEntityId(),
                    r.field()
                )
            );

            if (ExceptionsHelper.status(exc).getStatus() >= 500) {
                List<String> fields = requests.stream().map(FieldInferenceRequest::field).distinct().toList();
                logger.warn(
                    "Exception when running inference id [" + inferenceProvider.model.getInferenceEntityId() + "] on fields " + fields,
                    exc
                );
            }
        }

        private void recordRequestCountMetrics(Model model, int incrementBy, Throwable throwable) {
            inferenceStats.requestCount()
                .withModel(model)
                .withThrowable(throwable)
                .withProductUseCase(SEMANTIC_TEXT_USE_CASE)
                .incrementBy(incrementBy);
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
            final ExtendedIndexRequest indexRequest;
            if (item.request() instanceof IndexRequest ir) {
                indexRequest = new ExtendedIndexRequest(ir, false);
            } else if (item.request() instanceof UpdateRequest updateRequest) {
                if (updateRequest.script() != null) {
                    setInferenceResponseFailure(
                        itemIndex,
                        new ElasticsearchStatusException(
                            "Cannot apply update with a script on indices that contain [{}] field(s)",
                            RestStatus.BAD_REQUEST,
                            SemanticTextFieldMapper.CONTENT_TYPE
                        )
                    );
                    return 0;
                }
                indexRequest = new ExtendedIndexRequest(updateRequest.doc(), true);
            } else {
                // ignore delete request
                return 0;
            }

            return addFieldInferenceRequests(indexRequest, itemIndex, requestsMap);
        }

        private long addFieldInferenceRequests(
            ExtendedIndexRequest indexRequest,
            int itemIndex,
            Map<String, List<FieldInferenceRequest>> requestsMap
        ) {
            final Map<String, Object> docMap = indexRequest.getIndexRequest().sourceAsMap();
            final Map<String, List<FieldInferenceRequest>> itemRequests = new HashMap<>();
            long inputLength = 0;
            for (var entry : fieldInferenceMap.values()) {
                if (hasInferenceResponseFailure(itemIndex)) {
                    break;
                }

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
                MinimalServiceSettings serviceSettings = null;
                Boolean allowObjectValues = null;
                for (var sourceField : entry.getSourceFields()) {
                    if (hasInferenceResponseFailure(itemIndex)) {
                        break;
                    }

                    var valueObj = XContentMapValues.extractValue(sourceField, docMap, EXPLICIT_NULL);
                    if (useLegacyFormat == false && indexRequest.isUpdateRequest() && valueObj == EXPLICIT_NULL) {
                        /**
                         * It's an update request, and the source field is explicitly set to null,
                         * so we need to propagate this information to the inference fields metadata
                         * to overwrite any inference previously computed on the field.
                         * This ensures that the field is treated as intentionally cleared,
                         * preventing any unintended carryover of prior inference results.
                         */
                        if (incrementIndexingPressurePreInference(indexRequest, itemIndex) == false) {
                            break;
                        }

                        var slot = ensureResponseAccumulatorSlot(itemIndex);
                        slot.addOrUpdateResponse(
                            new ChunkedStringFieldInferenceResponse(field, sourceField, null, order++, 0, null, EMPTY_CHUNKED_INFERENCE)
                        );
                        continue;
                    }
                    if (valueObj == null || valueObj == EXPLICIT_NULL) {
                        if (indexRequest.isUpdateRequest() && useLegacyFormat) {
                            setInferenceResponseFailure(
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

                    if (serviceSettings == null) {
                        var serviceSettingsMap = modelRegistry.getMinimalServiceSettings(Set.of(inferenceId), false);
                        if (serviceSettingsMap.isEmpty()) {
                            setInferenceResponseFailure(
                                itemIndex,
                                new ResourceNotFoundException("Inference id [{}] not found for field [{}]", inferenceId, field)
                            );
                            break;
                        }
                        serviceSettings = serviceSettingsMap.get(inferenceId);
                        allowObjectValues = indexVersion.onOrAfter(IndexVersions.SEMANTIC_FIELD_TYPE)
                            && serviceSettings.taskType() == TaskType.EMBEDDING
                            && SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled();
                    }

                    final List<?> values;
                    try {
                        values = allowObjectValues
                            ? SemanticTextUtils.nodeObjectValues(field, valueObj)
                            : SemanticTextUtils.nodeStringValues(field, valueObj);
                    } catch (Exception exc) {
                        setInferenceResponseFailure(itemIndex, exc);
                        break;
                    }

                    List<FieldInferenceRequest> requests = itemRequests.computeIfAbsent(inferenceId, k -> new ArrayList<>());
                    inputLength += addInferenceRequestsForSourceFieldValues(
                        itemIndex,
                        indexRequest,
                        field,
                        sourceField,
                        chunkingSettings,
                        order,
                        values,
                        requests
                    );
                    order += values.size();
                }
            }

            if (hasInferenceResponseFailure(itemIndex)) {
                // Discard the staged requests. applyInferenceResponses will abort this item, so any inference work would be wasted.
                return 0;
            }

            // Merge the item's staged requests into the requests map
            itemRequests.forEach(
                (inferenceId, requests) -> requestsMap.computeIfAbsent(inferenceId, k -> new ArrayList<>()).addAll(requests)
            );
            return inputLength;
        }

        private long addInferenceRequestsForSourceFieldValues(
            int itemIndex,
            ExtendedIndexRequest indexRequest,
            String field,
            String sourceField,
            ChunkingSettings chunkingSettings,
            int startOrder,
            List<?> values,
            List<FieldInferenceRequest> requests
        ) {
            int order = startOrder;
            int offsetAdjustment = 0;
            int inputIndex = 0;
            long inputLength = 0;
            var slot = ensureResponseAccumulatorSlot(itemIndex);

            for (Object v : values) {
                if (incrementIndexingPressurePreInference(indexRequest, itemIndex) == false) {
                    break;
                }

                int inputLengthDelta = switch (v) {
                    case String s when s.isBlank() -> addBlankStringResponse(slot, field, sourceField, s, order);
                    case String s -> addChunkedStringRequest(
                        requests,
                        itemIndex,
                        field,
                        sourceField,
                        s,
                        order,
                        offsetAdjustment,
                        chunkingSettings
                    );
                    case InferenceString is -> addInferenceStringRequest(requests, itemIndex, field, sourceField, is, order, inputIndex);
                    default -> {
                        setInferenceResponseFailure(
                            itemIndex,
                            new IllegalStateException(
                                "Unexpected parsed inference input type ["
                                    + v.getClass().getName()
                                    + "] for field ["
                                    + field
                                    + "] from source field ["
                                    + sourceField
                                    + "]"
                            )
                        );
                        yield -1;
                    }
                };
                if (inputLengthDelta < 0) {
                    break;
                }

                if (v instanceof String s) {
                    // When using the inference metadata fields format, all the text input values are concatenated so that the
                    // chunk text offsets are expressed in the context of a single string. Calculate the offset adjustment
                    // to apply to account for this.
                    offsetAdjustment += s.length() + 1; // Add one for separator char length
                }

                inputLength += inputLengthDelta;
                order++;
                inputIndex++;
            }

            return inputLength;
        }

        private int addBlankStringResponse(
            FieldInferenceResponseAccumulator slot,
            String field,
            String sourceField,
            String input,
            int order
        ) {
            slot.addOrUpdateResponse(
                new ChunkedStringFieldInferenceResponse(field, sourceField, input, order, 0, null, EMPTY_CHUNKED_INFERENCE)
            );
            return 0;
        }

        private int addChunkedStringRequest(
            List<FieldInferenceRequest> requests,
            int itemIndex,
            String field,
            String sourceField,
            String input,
            int order,
            int offsetAdjustment,
            ChunkingSettings chunkingSettings
        ) {
            requests.add(
                new ChunkedStringFieldInferenceRequest(itemIndex, field, sourceField, input, order, offsetAdjustment, chunkingSettings)
            );
            return input.length();
        }

        private int addInferenceStringRequest(
            List<FieldInferenceRequest> requests,
            int itemIndex,
            String field,
            String sourceField,
            InferenceString input,
            int order,
            int sourceFieldInputIndex
        ) {
            requests.add(new InferenceStringFieldInferenceRequest(itemIndex, field, sourceField, input, order, sourceFieldInputIndex));
            return input.value().length();
        }

        private boolean incrementIndexingPressurePreInference(ExtendedIndexRequest indexRequest, int itemIndex) {
            boolean success = true;
            if (indexRequest.isIndexingPressureIncremented() == false) {
                try {
                    // Track operation count as one operation per document source update
                    coordinatingIndexingPressure.increment(1, indexRequest.getIndexRequest().indexSource().byteLength());
                    indexRequest.setIndexingPressureIncremented();
                } catch (EsRejectedExecutionException e) {
                    setInferenceResponseFailure(
                        itemIndex,
                        new InferenceException(
                            "Unable to insert inference results into document ["
                                + indexRequest.getIndexRequest().id()
                                + "] due to memory pressure. Please retry the bulk request with fewer documents or smaller document sizes.",
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
                acc = new FieldInferenceResponseAccumulator(id);
                inferenceResults.set(id, acc);
            }
            return acc;
        }

        private void setInferenceResponseFailure(int id, Exception failure) {
            var acc = ensureResponseAccumulatorSlot(id);
            acc.setFailure(failure);
        }

        private boolean hasInferenceResponseFailure(int itemIndex) {
            var acc = inferenceResults.get(itemIndex);
            return acc != null && acc.failure().get() != null;
        }

        /**
         * Applies the {@link FieldInferenceResponseAccumulator} to the provided {@link BulkItemRequest}.
         * If the response contains failures, the bulk item request is marked as failed for the downstream action.
         * Otherwise, the source of the request is augmented with the field inference results.
         */
        private void applyInferenceResponses(BulkItemRequest item, FieldInferenceResponseAccumulator response) throws IOException {
            Exception failure = response.failure().get();
            if (failure != null) {
                item.abort(item.index(), failure);
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
                Collections.sort(responses, Comparator.comparingInt(FieldInferenceResponse::fieldInputOrder));
                Map<String, List<SemanticTextField.Chunk>> chunkMap = new LinkedHashMap<>();
                for (var resp : responses) {
                    // Get the first non-null model from the response list
                    if (model == null) {
                        model = resp.model();
                    }

                    var lst = chunkMap.computeIfAbsent(resp.sourceField(), k -> new ArrayList<>());
                    lst.addAll(resp.toChunks(useLegacyFormat, indexRequest.getContentType()));
                }

                List<String> inputs = useLegacyFormat
                    ? responses.stream().filter(r -> r.sourceField().equals(fieldName)).map(FieldInferenceResponse::legacyInput).toList()
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
                        chunkMap
                    ),
                    indexRequest.getContentType()
                );
                inferenceFieldsMap.put(fieldName, result);
            }

            updateIndexSource(item, inferenceFieldsMap);
        }

        private void updateIndexSource(BulkItemRequest item, Map<String, Object> inferenceFieldsMap) throws IOException {
            IndexRequest indexRequest = getIndexRequestOrNull(item.request());
            IndexSource indexSource = indexRequest.indexSource();
            int originalSourceSize = indexSource.byteLength();
            BytesReference originalSource = indexSource.bytes();
            if (useLegacyFormat) {
                var newDocMap = indexSource.sourceAsMap();
                for (var entry : inferenceFieldsMap.entrySet()) {
                    XContentMapValues.insertValue(entry.getKey(), newDocMap, entry.getValue());
                }
                indexSource.source(newDocMap, indexSource.contentType());
            } else {
                try (XContentBuilder builder = XContentBuilder.builder(indexSource.contentType().xContent())) {
                    appendSourceAndInferenceMetadata(builder, indexSource.bytes(), indexSource.contentType(), inferenceFieldsMap);
                    indexSource.source(builder);
                }
            }
            long modifiedSourceSize = indexSource.byteLength();

            // Add the indexing pressure from the source modifications.
            // Don't increment operation count because we count one source update as one operation, and we already accounted for those
            // in addFieldInferenceRequests.
            try {
                coordinatingIndexingPressure.increment(0, modifiedSourceSize - originalSourceSize);
            } catch (EsRejectedExecutionException e) {
                indexSource.source(originalSource, indexSource.contentType());
                item.abort(
                    item.index(),
                    new InferenceException(
                        "Unable to insert inference results into document ["
                            + indexRequest.id()
                            + "] due to memory pressure. Please retry the bulk request with fewer documents or smaller document sizes.",
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

    private static class ExtendedIndexRequest {
        private final IndexRequest indexRequest;
        private final boolean isUpdateRequest;
        private boolean indexingPressureIncremented;

        private ExtendedIndexRequest(IndexRequest indexRequest, boolean isUpdateRequest) {
            this.indexRequest = indexRequest;
            this.isUpdateRequest = isUpdateRequest;
            this.indexingPressureIncremented = false;
        }

        private IndexRequest getIndexRequest() {
            return indexRequest;
        }

        private boolean isUpdateRequest() {
            return isUpdateRequest;
        }

        private boolean isIndexingPressureIncremented() {
            return indexingPressureIncremented;
        }

        private void setIndexingPressureIncremented() {
            this.indexingPressureIncremented = true;
        }
    }

    private static class EmptyChunkedInference implements ChunkedInference {
        @Override
        public Iterator<Chunk> chunksAsByteReference(XContent xcontent) {
            return Collections.emptyIterator();
        }
    }
}
