/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class DeepSeekService extends SenderService {
    private static final String NAME = "deepseek";
    private static final String CHAT_COMPLETION_ERROR_PREFIX = "deepseek chat completions";
    private static final String COMPLETION_ERROR_PREFIX = "deepseek completions";
    private static final String SERVICE_NAME = "DeepSeek";
    // The task types exposed via the _inference/_services API
    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES_FOR_SERVICES_API = EnumSet.of(
        TaskType.COMPLETION,
        TaskType.CHAT_COMPLETION
    );
    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES_FOR_STREAMING = EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);

    public DeepSeekService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        doInfer(model, inputs, timeout, COMPLETION_ERROR_PREFIX, listener);
    }

    private void doInfer(
        Model model,
        InferenceInputs inputs,
        TimeValue timeout,
        String errorPrefix,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof DeepSeekChatCompletionModel deepSeekModel) {
            var requestCreator = new DeepSeekRequestManager(deepSeekModel, getServiceComponents().threadPool());
            var errorMessage = constructFailedToSendRequestMessage(errorPrefix);
            var action = new SenderExecutableAction(getSender(), requestCreator, errorMessage);
            action.execute(inputs, timeout, listener);
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {}

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        doInfer(model, inputs, timeout, CHAT_COMPLETION_ERROR_PREFIX, listener);
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        EmbeddingsInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        listener.onFailure(new UnsupportedOperationException(Strings.format("The %s service only supports unified completion", NAME)));
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void parseRequestConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        ActionListener<Model> parsedModelListener
    ) {
        ActionListener.completeWith(parsedModelListener, () -> {
            var serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            try {
                return DeepSeekChatCompletionModel.createFromNewInput(modelId, taskType, NAME, serviceSettingsMap);
            } finally {
                throwIfNotEmptyMap(serviceSettingsMap, NAME);
            }
        });
    }

    @Override
    public Model parsePersistedConfigWithSecrets(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        var serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        var secretSettingsMap = removeFromMapOrThrowIfNull(secrets, ModelSecrets.SECRET_SETTINGS);
        return DeepSeekChatCompletionModel.readFromStorage(modelId, taskType, NAME, serviceSettingsMap, secretSettingsMap);
    }

    @Override
    public Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        var serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        return DeepSeekChatCompletionModel.readFromStorage(modelId, taskType, NAME, serviceSettingsMap, null);
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        return Configuration.get();
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return SUPPORTED_TASK_TYPES_FOR_SERVICES_API;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_DEEPSEEK;
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return SUPPORTED_TASK_TYPES_FOR_STREAMING;
    }

    private static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES_FOR_SERVICES_API).setDescription(
                        "The name of the model to use for the inference task."
                    )
                        .setLabel("Model ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.putAll(
                    DefaultSecretSettings.toSettingsConfigurationWithDescription(
                        "The DeepSeek API authentication key. For more details about generating DeepSeek API keys, "
                            + "refer to https://api-docs.deepseek.com.",
                        SUPPORTED_TASK_TYPES_FOR_SERVICES_API
                    )
                );

                configurationMap.put(
                    URL,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES_FOR_SERVICES_API).setDefaultValue(
                        "https://api.deepseek.com/chat/completions"
                    )
                        .setDescription("The URL endpoint to use for the requests.")
                        .setLabel("URL")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                return new InferenceServiceConfiguration.Builder().setService(NAME)
                    .setName(SERVICE_NAME)
                    .setTaskTypes(SUPPORTED_TASK_TYPES_FOR_SERVICES_API)
                    .setConfigurations(configurationMap)
                    .build();
            }
        );
    }
}
