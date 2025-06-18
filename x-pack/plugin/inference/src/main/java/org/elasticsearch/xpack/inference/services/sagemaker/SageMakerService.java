/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModelBuilder;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemas;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails;

public class SageMakerService implements InferenceService {
    public static final String NAME = "amazon_sagemaker";
    private static final String DISPLAY_NAME = "Amazon SageMaker";
    private static final List<String> ALIASES = List.of("sagemaker", "amazonsagemaker");
    private static final int DEFAULT_BATCH_SIZE = 256;
    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.THIRTY_SECONDS;
    private final SageMakerModelBuilder modelBuilder;
    private final SageMakerClient client;
    private final SageMakerSchemas schemas;
    private final ThreadPool threadPool;
    private final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration;

    public SageMakerService(
        SageMakerModelBuilder modelBuilder,
        SageMakerClient client,
        SageMakerSchemas schemas,
        ThreadPool threadPool,
        CheckedSupplier<Map<String, SettingsConfiguration>, RuntimeException> configurationMap
    ) {
        this.modelBuilder = modelBuilder;
        this.client = client;
        this.schemas = schemas;
        this.threadPool = threadPool;
        this.configuration = new LazyInitializable<>(
            () -> new InferenceServiceConfiguration.Builder().setService(NAME)
                .setName(DISPLAY_NAME)
                .setTaskTypes(supportedTaskTypes())
                .setConfigurations(configurationMap.get())
                .build()
        );
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public List<String> aliases() {
        return ALIASES;
    }

    @Override
    public void parseRequestConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        ActionListener<Model> parsedModelListener
    ) {
        ActionListener.completeWith(parsedModelListener, () -> modelBuilder.fromRequest(modelId, taskType, NAME, config));
    }

    @Override
    public Model parsePersistedConfigWithSecrets(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        return modelBuilder.fromStorage(modelId, taskType, NAME, config, secrets);
    }

    @Override
    public Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        return modelBuilder.fromStorage(modelId, taskType, NAME, config, null);
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        return configuration.getOrCompute();
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return schemas.supportedTaskTypes();
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return schemas.supportedStreamingTasks();
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
        if (model instanceof SageMakerModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var inferenceRequest = new SageMakerInferenceRequest(query, returnDocuments, topN, input, stream, inputType);

        try {
            var sageMakerModel = ((SageMakerModel) model).override(taskSettings);
            var regionAndSecrets = regionAndSecrets(sageMakerModel);

            if (stream) {
                var schema = schemas.streamSchemaFor(sageMakerModel);
                var request = schema.streamRequest(sageMakerModel, inferenceRequest);
                client.invokeStream(
                    regionAndSecrets,
                    request,
                    timeout != null ? timeout : DEFAULT_TIMEOUT,
                    ActionListener.wrap(
                        response -> listener.onResponse(schema.streamResponse(sageMakerModel, response)),
                        e -> listener.onFailure(schema.error(sageMakerModel, e))
                    )
                );
            } else {
                var schema = schemas.schemaFor(sageMakerModel);
                var request = schema.request(sageMakerModel, inferenceRequest);
                client.invoke(
                    regionAndSecrets,
                    request,
                    timeout != null ? timeout : DEFAULT_TIMEOUT,
                    ActionListener.wrap(
                        response -> listener.onResponse(schema.response(sageMakerModel, response, threadPool.getThreadContext())),
                        e -> listener.onFailure(schema.error(sageMakerModel, e))
                    )
                );
            }
        } catch (Exception e) {
            listener.onFailure(internalFailure(model, e));
        }
    }

    private SageMakerClient.RegionAndSecrets regionAndSecrets(SageMakerModel model) {
        var secrets = model.awsSecretSettings();
        if (secrets.isEmpty()) {
            assert false : "Cannot invoke a model without secrets";
            throw new ElasticsearchStatusException(
                format("Attempting to infer using a model without API keys, inference id [%s]", model.getInferenceEntityId()),
                RestStatus.INTERNAL_SERVER_ERROR
            );
        }
        return new SageMakerClient.RegionAndSecrets(model.region(), secrets.get());
    }

    private static ElasticsearchStatusException internalFailure(Model model, Exception cause) {
        if (cause instanceof ElasticsearchStatusException ese) {
            return ese;
        } else {
            return new ElasticsearchStatusException(
                "Failed to call SageMaker for inference id [{}].",
                RestStatus.INTERNAL_SERVER_ERROR,
                cause,
                model.getInferenceEntityId()
            );
        }
    }

    @Override
    public void unifiedCompletionInfer(
        Model model,
        UnifiedCompletionRequest request,
        @Nullable TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof SageMakerModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        try {
            var sageMakerModel = (SageMakerModel) model;
            var regionAndSecrets = regionAndSecrets(sageMakerModel);
            var schema = schemas.streamSchemaFor(sageMakerModel);
            var sagemakerRequest = schema.chatCompletionStreamRequest(sageMakerModel, request);
            client.invokeStream(
                regionAndSecrets,
                sagemakerRequest,
                timeout != null ? timeout : DEFAULT_TIMEOUT,
                ActionListener.wrap(
                    response -> listener.onResponse(schema.chatCompletionStreamResponse(sageMakerModel, response)),
                    e -> listener.onFailure(schema.chatCompletionError(sageMakerModel, e))
                )
            );
        } catch (Exception e) {
            listener.onFailure(internalFailure(model, e));
        }
    }

    @Override
    public void chunkedInfer(
        Model model,
        String query,
        List<ChunkInferenceInput> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        @Nullable TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        if (model instanceof SageMakerModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }
        try {
            var sageMakerModel = ((SageMakerModel) model).override(taskSettings);
            var batchedRequests = new EmbeddingRequestChunker<>(
                input,
                sageMakerModel.batchSize().orElse(DEFAULT_BATCH_SIZE),
                sageMakerModel.getConfigurations().getChunkingSettings()
            ).batchRequestsWithListeners(listener);

            var subscribableListener = SubscribableListener.newSucceeded(null);
            for (var request : batchedRequests) {
                subscribableListener = subscribableListener.andThen(
                    threadPool.executor(UTILITY_THREAD_POOL_NAME),
                    threadPool.getThreadContext(),
                    (l, ignored) -> infer(
                        sageMakerModel,
                        query,
                        null,  // no return docs while chunking?
                        null, // no topN while chunking?
                        request.batch().inputs().get(),
                        false, // we never stream when chunking
                        null, // since we pass sageMakerModel as the model, we already overwrote the model with the task settings
                        inputType,
                        timeout,
                        ActionListener.runAfter(request.listener(), () -> l.onResponse(null))
                    )
                );
            }
            // if there were any errors trying to create the SubscribableListener chain, then forward that to the listener
            // otherwise, BatchRequestAndListener will handle forwarding errors from the infer method
            subscribableListener.addListener(
                ActionListener.noop().delegateResponse((l, e) -> listener.onFailure(internalFailure(model, e)))
            );
        } catch (Exception e) {
            listener.onFailure(internalFailure(model, e));
        }
    }

    @Override
    public void start(Model model, TimeValue timeout, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof SageMakerModel sageMakerModel) {
            return modelBuilder.updateModelWithEmbeddingDetails(sageMakerModel, embeddingSize);
        }

        throw invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_SAGEMAKER;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
