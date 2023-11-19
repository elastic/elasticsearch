/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.action.openai.OpenAiActionCreator;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.throwIfNotEmptyMap;

public class OpenAiService implements InferenceService {
    public static final String NAME = "openai";

    private final SetOnce<HttpRequestSenderFactory> factory;
    private final SetOnce<ServiceComponents> serviceComponents;
    private final AtomicReference<Sender> sender = new AtomicReference<>();

    public OpenAiService(SetOnce<HttpRequestSenderFactory> factory, SetOnce<ServiceComponents> serviceComponents) {
        this.factory = Objects.requireNonNull(factory);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public OpenAiModel parseRequestConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);

        OpenAiModel model = createModel(
            modelId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            serviceSettingsMap,
            TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME)
        );

        throwIfNotEmptyMap(config, NAME);
        throwIfNotEmptyMap(serviceSettingsMap, NAME);
        throwIfNotEmptyMap(taskSettingsMap, NAME);

        return model;
    }

    private OpenAiModel createModel(
        String modelId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings,
        String failureMessage
    ) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new OpenAiEmbeddingsModel(modelId, taskType, NAME, serviceSettings, taskSettings, secretSettings);
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        };
    }

    @Override
    public OpenAiModel parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config, Map<String, Object> secrets) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrThrowIfNull(secrets, ModelSecrets.SECRET_SETTINGS);

        OpenAiModel model = createModel(
            modelId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            secretSettingsMap,
            format("Failed to parse stored model [%s] for [%s] service, please delete and add the service again", modelId, NAME)
        );

        throwIfNotEmptyMap(config, NAME);
        throwIfNotEmptyMap(secrets, NAME);
        throwIfNotEmptyMap(serviceSettingsMap, NAME);
        throwIfNotEmptyMap(taskSettingsMap, NAME);
        throwIfNotEmptyMap(secretSettingsMap, NAME);

        return model;
    }

    @Override
    public void infer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        ActionListener<List<? extends InferenceResults>> listener
    ) {
        init();

        if (model instanceof OpenAiModel == false) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    format(
                        "The internal model was invalid, please delete the service [%s] with id [%s] and add it again.",
                        model.getConfigurations().getService(),
                        model.getConfigurations().getModelId()
                    ),
                    RestStatus.INTERNAL_SERVER_ERROR
                )
            );
            return;
        }

        OpenAiModel openAiModel = (OpenAiModel) model;
        var actionCreator = new OpenAiActionCreator(sender.get(), serviceComponents.get());

        var action = openAiModel.accept(actionCreator, taskSettings);
        action.execute(input, listener);
    }

    @Override
    public void start(Model model, ActionListener<Boolean> listener) {
        init();
        listener.onResponse(true);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(sender.get());
    }

    private void init() {
        sender.updateAndGet(current -> Objects.requireNonNullElseGet(current, () -> factory.get().createSender(name())));
        sender.get().start();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_OPENAI_ADDED;
    }
}
