/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.xpack.inference.external.action.huggingface.HuggingFaceElserAction;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.services.MapParsingUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.throwIfNotEmptyMap;

public class HuggingFaceElserService implements InferenceService {
    public static final String NAME = "hugging_face_elser";

    private final SetOnce<HttpRequestSenderFactory> factory;
    private final SetOnce<ServiceComponents> serviceComponents;
    private final AtomicReference<Sender> sender = new AtomicReference<>();

    public HuggingFaceElserService(SetOnce<HttpRequestSenderFactory> factory, SetOnce<ServiceComponents> serviceComponents) {
        this.factory = Objects.requireNonNull(factory);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HuggingFaceElserModel parseRequestConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);

        HuggingFaceElserServiceSettings serviceSettings = HuggingFaceElserServiceSettings.fromMap(serviceSettingsMap);
        HuggingFaceElserSecretSettings secretSettings = HuggingFaceElserSecretSettings.fromMap(serviceSettingsMap);

        throwIfNotEmptyMap(config, NAME);
        throwIfNotEmptyMap(serviceSettingsMap, NAME);

        return new HuggingFaceElserModel(modelId, taskType, NAME, serviceSettings, secretSettings);
    }

    @Override
    public HuggingFaceElserModel parsePersistedConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrThrowIfNull(secrets, ModelSecrets.SECRET_SETTINGS);

        HuggingFaceElserServiceSettings serviceSettings = HuggingFaceElserServiceSettings.fromMap(serviceSettingsMap);
        HuggingFaceElserSecretSettings secretSettings = HuggingFaceElserSecretSettings.fromMap(secretSettingsMap);

        return new HuggingFaceElserModel(modelId, taskType, NAME, serviceSettings, secretSettings);
    }

    @Override
    public void infer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        ActionListener<List<? extends InferenceResults>> listener
    ) {
        if (model.getConfigurations().getTaskType() != TaskType.SPARSE_EMBEDDING) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), NAME),
                    RestStatus.BAD_REQUEST
                )
            );
            return;
        }

        if (model instanceof HuggingFaceElserModel == false) {
            listener.onFailure(new ElasticsearchException("The internal model was invalid"));
            return;
        }

        init();

        HuggingFaceElserModel huggingFaceElserModel = (HuggingFaceElserModel) model;
        HuggingFaceElserAction action = new HuggingFaceElserAction(sender.get(), huggingFaceElserModel, serviceComponents.get());

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
        return TransportVersions.ML_INFERENCE_TASK_SETTINGS_OPTIONAL_ADDED;
    }
}
