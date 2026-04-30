/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.inference.InferenceStringGroup.containsNonTextEntry;
import static org.elasticsearch.inference.InferenceStringGroup.indexContainingMultipleInferenceStrings;
import static org.elasticsearch.inference.TaskType.CHAT_COMPLETION;
import static org.elasticsearch.inference.TaskType.EMBEDDING;
import static org.elasticsearch.inference.TaskType.SPARSE_EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.resolveInferenceTimeout;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwUnsupportedEmbeddingOperation;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwUnsupportedReasoningUnifiedCompletionOperation;

public abstract class SenderService<M extends Model> implements InferenceService {

    protected static final Set<TaskType> COMPLETION_ONLY = EnumSet.of(TaskType.COMPLETION);

    /**
     * The task types that support chunking settings
     */
    protected static final EnumSet<TaskType> CHUNKING_TASK_TYPES = EnumSet.of(SPARSE_EMBEDDING, TEXT_EMBEDDING, EMBEDDING);

    private final Sender sender;
    private final ServiceComponents serviceComponents;
    private final ClusterService clusterService;
    protected final Map<TaskType, ModelCreator<? extends M>> modelCreators;

    public SenderService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        ClusterService clusterService,
        Map<TaskType, ModelCreator<? extends M>> modelCreators
    ) {
        Objects.requireNonNull(factory);
        sender = factory.createSender();
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.modelCreators = Objects.requireNonNull(modelCreators);
    }

    public Sender getSender() {
        return sender;
    }

    protected ServiceComponents getServiceComponents() {
        return serviceComponents;
    }

    @Override
    public void infer(
        Model model,
        @Nullable String query,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        List<String> input,
        boolean stream,
        Map<String, Object> taskSettings,
        InputType inputType,
        @Nullable TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        try {
            var resolvedInferenceTimeout = resolveInferenceTimeout(timeout, inputType, clusterService, model.getTaskType());
            var inferenceInput = createInput(this, model, input, inputType, query, returnDocuments, topN, stream);
            doInfer(model, inferenceInput, taskSettings, resolvedInferenceTimeout, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

            ChunkingSettings chunkingSettings = null;
            if (CHUNKING_TASK_TYPES.contains(taskType)) {
                chunkingSettings = ChunkingSettingsBuilder.fromMap(
                    removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS)
                );
            }

            migrateBetweenTaskAndServiceSettings(serviceSettingsMap, taskSettingsMap);

            var model = retrieveModelCreatorFromMapOrThrow(
                modelCreators,
                inferenceEntityId,
                taskType,
                name(),
                ConfigurationParseContext.REQUEST
            ).createFromMaps(
                inferenceEntityId,
                taskType,
                name(),
                serviceSettingsMap,
                taskSettingsMap,
                chunkingSettings,
                serviceSettingsMap,
                ConfigurationParseContext.REQUEST
            );

            throwIfNotEmptyMap(config, name());
            throwIfNotEmptyMap(serviceSettingsMap, name());
            if (usesParserForTaskSettings() == false) {
                throwIfNotEmptyMap(taskSettingsMap, name());
            }

            parsedModelListener.onResponse(model);
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    /**
     * Override as needed. Services that create the task settings using a parser do not remove entries from the task settings map, which
     * causes the existing validation that there are no unknown values left in the map to fail. Rather than explicitly checking that the
     * task settings map is empty, these services will throw an exception from the parser.
     * @return whether this service implements a parser for task settings
     */
    public boolean usesParserForTaskSettings() {
        return false;
    }

    public M parsePersistedConfig(UnparsedModel unparsedModel) {
        var config = unparsedModel.settings();
        var secrets = unparsedModel.secrets();
        var taskType = unparsedModel.taskType();

        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = secrets == null ? null : removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (CHUNKING_TASK_TYPES.contains(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        migrateBetweenTaskAndServiceSettings(serviceSettingsMap, taskSettingsMap);

        return retrieveModelCreatorFromMapOrThrow(
            modelCreators,
            unparsedModel.inferenceEntityId(),
            taskType,
            name(),
            ConfigurationParseContext.PERSISTENT
        ).createFromMaps(
            unparsedModel.inferenceEntityId(),
            taskType,
            name(),
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            secretSettingsMap,
            ConfigurationParseContext.PERSISTENT,
            unparsedModel.endpointMetadata()
        );
    }

    @Override
    public M buildModelFromConfigAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return retrieveModelCreatorFromMapOrThrow(
            modelCreators,
            config.getInferenceEntityId(),
            config.getTaskType(),
            config.getService(),
            ConfigurationParseContext.REQUEST
        ).createFromModelConfigurationsAndSecrets(config, secrets);
    }

    /**
     * Allows for implementations to perform migration for the cases where settings were moved between service and task settings.
     */
    protected void migrateBetweenTaskAndServiceSettings(Map<String, Object> serviceSettings, Map<String, Object> taskSettings) {}

    private static InferenceInputs createInput(
        SenderService<?> service,
        Model model,
        List<String> input,
        InputType inputType,
        @Nullable String query,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        boolean stream
    ) {
        return switch (model.getTaskType()) {
            case COMPLETION, CHAT_COMPLETION -> new ChatCompletionInput(input, stream);
            case RERANK -> {
                ValidationException validationException = new ValidationException();
                service.validateRerankParameters(returnDocuments, topN, validationException);

                if (query == null) {
                    validationException.addValidationError("Rerank task type requires a non-null query field");
                }

                validationException.throwIfValidationErrorsExist();
                yield new QueryAndDocsInputs(query, input, returnDocuments, topN, stream);
            }
            case TEXT_EMBEDDING, SPARSE_EMBEDDING -> {
                ValidationException validationException = new ValidationException();
                service.validateInputType(inputType, model, validationException);
                validationException.throwIfValidationErrorsExist();
                yield new EmbeddingsInput(input, inputType, stream);
            }
            default -> throw new ElasticsearchStatusException(
                Strings.format("Invalid task type received when determining input type: [%s]", model.getTaskType().toString()),
                RestStatus.BAD_REQUEST
            );
        };
    }

    @Override
    public void unifiedCompletionInfer(
        Model model,
        UnifiedCompletionRequest request,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        try {
            var resolvedInferenceTimeout = resolveInferenceTimeout(timeout, InputType.UNSPECIFIED, clusterService, CHAT_COMPLETION);
            if (supportsChatCompletionReasoning() == false && request.containsChatCompletionReasoning()) {
                throwUnsupportedReasoningUnifiedCompletionOperation(name());
            }
            doUnifiedCompletionInfer(model, new UnifiedChatInput(request, true), resolvedInferenceTimeout, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected boolean supportsChatCompletionReasoning() {
        return false;
    }

    @Override
    public void embeddingInfer(Model model, EmbeddingRequest request, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        try {
            var resolvedInferenceTimeout = resolveInferenceTimeout(timeout, request.inputType(), clusterService, model.getTaskType());
            if (supportsImageEmbeddingContent() == false && containsNonTextEntry(request.inputs())) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        Strings.format("The %s service does not support embedding with non-text inputs", name()),
                        RestStatus.BAD_REQUEST
                    )
                );
                return;
            }
            if (supportsMultipleItemsPerContent()) {
                doEmbeddingInfer(model, request, resolvedInferenceTimeout, listener);
            } else {
                var index = indexContainingMultipleInferenceStrings(request.inputs());
                if (index == null) {
                    doEmbeddingInfer(model, request, resolvedInferenceTimeout, listener);
                } else {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            Strings.format(
                                "Field [%1$s] must contain a single item for [%2$s] service. "
                                    + "[%1$s] object with multiple items found at $.%3$s.%1$s[%4$d]",
                                InferenceStringGroup.CONTENT_FIELD,
                                name(),
                                EmbeddingRequest.INPUT_FIELD,
                                index
                            ),
                            RestStatus.BAD_REQUEST
                        )
                    );
                }
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Override as necessary for services which support generating a single embedding vector from multiple inputs
     * @return true if the service supports sending embedding requests where multiple inputs are used to generate a single embedding vector
     */
    protected boolean supportsMultipleItemsPerContent() {
        return false;
    }

    /**
     * Override as necessary for services which support images in embedding inputs
     * @return true if the service supports images in embedding inputs
     */
    protected boolean supportsImageEmbeddingContent() {
        return false;
    }

    @Override
    public void chunkedInfer(
        Model model,
        @Nullable String query,
        List<ChunkInferenceInput> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        try {
            var resolvedInferenceTimeout = resolveInferenceTimeout(timeout, inputType, clusterService, model.getTaskType());
            ValidationException validationException = new ValidationException();
            validateInputType(inputType, model, validationException);
            validationException.throwIfValidationErrorsExist();
            if (supportsChunkedInfer()) {
                if (input.isEmpty()) {
                    listener.onResponse(List.of());
                } else {
                    if (supportsImageEmbeddingContent() == false
                        && containsNonTextEntry(input.stream().map(ChunkInferenceInput::input).toList())) {
                        listener.onFailure(
                            new ElasticsearchStatusException(
                                Strings.format("The %s service does not support embedding with non-text inputs", name()),
                                RestStatus.BAD_REQUEST
                            )
                        );
                        return;
                    }
                    // a non-null query is not supported and is dropped by all providers
                    doChunkedInfer(model, input, taskSettings, inputType, resolvedInferenceTimeout, listener);
                }
            } else {
                listener.onFailure(
                    new UnsupportedOperationException(Strings.format("%s service does not support chunked inference", name()))
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected abstract void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    );

    protected abstract void validateInputType(InputType inputType, Model model, ValidationException validationException);

    protected void validateRerankParameters(Boolean returnDocuments, Integer topN, ValidationException validationException) {}

    protected abstract void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    );

    protected void doEmbeddingInfer(
        Model model,
        EmbeddingRequest request,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        throwUnsupportedEmbeddingOperation(model.getConfigurations().getService());
    }

    protected abstract void doChunkedInfer(
        Model model,
        List<ChunkInferenceInput> inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    );

    protected boolean supportsChunkedInfer() {
        return true;
    }

    @Override
    public void start(Model model, @Nullable TimeValue timeout, ActionListener<Boolean> listener) {
        listener.onResponse(Boolean.TRUE);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(sender);
    }

    /**
     * Retrieves a {@link ModelCreator} from the provided map based on the task type, or throws an exception if not found.
     * @param modelCreators the map of task types to model creators
     * @param inferenceId the inference entity ID
     * @param taskType the task type
     * @param service the service name
     * @param context the configuration parse context
     * @param <C> the type of {@link ModelCreator}
     * @return the retrieved {@link ModelCreator}
     * @throws ElasticsearchStatusException if no {@link ModelCreator} is found for the given task type
     */
    protected static <C> C retrieveModelCreatorFromMapOrThrow(
        Map<TaskType, C> modelCreators,
        String inferenceId,
        TaskType taskType,
        String service,
        ConfigurationParseContext context
    ) {
        C modelCreator = modelCreators.get(taskType);
        if (modelCreator == null) {
            throw createInvalidTaskTypeException(inferenceId, service, taskType, context);
        }
        return modelCreator;
    }
}
