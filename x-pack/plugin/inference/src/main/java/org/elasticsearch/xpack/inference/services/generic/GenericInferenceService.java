/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.generic;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class GenericInferenceService extends SenderService {

    public GenericInferenceService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    public void parseRequestConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        ActionListener<Model> parsedModelListener
    ) {
        throw new UnsupportedOperationException("Parsing request config is not supported by the generic service");
    }

    @Override
    public Model parsePersistedConfigWithSecrets(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        throw new UnsupportedOperationException("Parsing persisted config with secrets is not supported by the generic service");
    }

    @Override
    public Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        throw new UnsupportedOperationException("Parsing persisted config is not supported by the generic service");
    }

    @Override
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        throw new UnsupportedOperationException("Inference is not supported by the generic service");
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        throw new UnsupportedOperationException("Unified completion inference is not supported by the generic service");
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        DocumentsOnlyInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        throw new UnsupportedOperationException("Chunked inference is not supported by the generic service");
    }

    @Override
    public String name() {
        // TODO: Return the name of the service from the parsed service schema
        return "generic";
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        // TODO: Identify a way to build a configuration for the generic service from the service schema
        return null;
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        // TODO: Add task types as we add support for them
        return null;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // TODO: Add a minimal version when we are ready to launch the generic service
        return null;
    }
}
