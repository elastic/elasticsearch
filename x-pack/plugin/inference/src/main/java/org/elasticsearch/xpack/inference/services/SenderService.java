/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
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

import static org.elasticsearch.inference.InferenceStringGroup.indexContainingMultipleInferenceStrings;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwUnsupportedEmbeddingOperation;

public abstract class SenderService implements InferenceService {
    protected static final Set<TaskType> COMPLETION_ONLY = EnumSet.of(TaskType.COMPLETION);
    private final Sender sender;
    private final ServiceComponents serviceComponents;
    private final ClusterService clusterService;

    public SenderService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
        Objects.requireNonNull(factory);
        sender = factory.createSender();
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
        this.clusterService = Objects.requireNonNull(clusterService);
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
        SubscribableListener.newForked(this::init).<InferenceServiceResults>andThen((inferListener) -> {
            var resolvedInferenceTimeout = ServiceUtils.resolveInferenceTimeout(timeout, inputType, clusterService);
            var inferenceInput = createInput(this, model, input, inputType, query, returnDocuments, topN, stream);
            doInfer(model, inferenceInput, taskSettings, resolvedInferenceTimeout, inferListener);
        }).addListener(listener);
    }

    private static InferenceInputs createInput(
        SenderService service,
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

                if (validationException.validationErrors().isEmpty() == false) {
                    throw validationException;
                }
                yield new QueryAndDocsInputs(query, input, returnDocuments, topN, stream);
            }
            case TEXT_EMBEDDING, SPARSE_EMBEDDING -> {
                ValidationException validationException = new ValidationException();
                service.validateInputType(inputType, model, validationException);
                if (validationException.validationErrors().isEmpty() == false) {
                    throw validationException;
                }
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
        SubscribableListener.newForked(this::init).<InferenceServiceResults>andThen((completionInferListener) -> {
            doUnifiedCompletionInfer(model, new UnifiedChatInput(request, true), timeout, completionInferListener);
        }).addListener(listener);
    }

    @Override
    public void embeddingInfer(Model model, EmbeddingRequest request, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        SubscribableListener.newForked(this::init).<InferenceServiceResults>andThen((embeddingInferListener) -> {
            if (supportsMultipleItemsPerContent()) {
                doEmbeddingInfer(model, request, timeout, embeddingInferListener);
            } else {
                var index = indexContainingMultipleInferenceStrings(request.inputs());
                if (index == null) {
                    doEmbeddingInfer(model, request, timeout, embeddingInferListener);
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
        }).addListener(listener);
    }

    /**
     * Override as necessary for services which support generating a single embedding vector from multiple inputs
     * @return true if the service supports sending embedding requests where multiple inputs are used to generate a single embedding vector
     */
    protected boolean supportsMultipleItemsPerContent() {
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
        SubscribableListener.newForked(this::init).<List<ChunkedInference>>andThen((chunkedInferListener) -> {
            ValidationException validationException = new ValidationException();
            validateInputType(inputType, model, validationException);
            if (validationException.validationErrors().isEmpty() == false) {
                throw validationException;
            }
            if (supportsChunkedInfer()) {
                if (input.isEmpty()) {
                    chunkedInferListener.onResponse(List.of());
                } else {
                    // a non-null query is not supported and is dropped by all providers
                    doChunkedInfer(model, input, taskSettings, inputType, timeout, chunkedInferListener);
                }
            } else {
                chunkedInferListener.onFailure(
                    new UnsupportedOperationException(Strings.format("%s service does not support chunked inference", name()))
                );
            }
        }).addListener(listener);
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

    public void start(Model model, ActionListener<Boolean> listener) {
        SubscribableListener.newForked(this::init)
            .<Boolean>andThen((doStartListener) -> doStart(model, doStartListener))
            .addListener(listener);
    }

    @Override
    public void start(Model model, @Nullable TimeValue unused, ActionListener<Boolean> listener) {
        start(model, listener);
    }

    protected void doStart(Model model, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    private void init(ActionListener<Void> listener) {
        sender.startAsynchronously(listener);
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
