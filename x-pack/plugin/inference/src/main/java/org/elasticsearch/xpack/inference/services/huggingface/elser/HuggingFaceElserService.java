/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.MapParsingUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.throwIfNotEmptyMap;

public class HuggingFaceElserService implements InferenceService {
    public static final String NAME = "hugging_face_elser";

    // TODO add the http client here
    public HuggingFaceElserService() {

    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HuggingFaceElserModel parseRequestConfig(String modelId, TaskType taskType, Map<String, Object> config) {
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
    public void infer(Model model, String input, Map<String, Object> taskSettings, ActionListener<InferenceResults> listener) {

    }

    @Override
    public void start(Model model, ActionListener<Boolean> listener) {

    }
}
