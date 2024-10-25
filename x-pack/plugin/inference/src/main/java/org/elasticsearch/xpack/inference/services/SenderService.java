/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class SenderService implements InferenceService {
    protected static final Set<TaskType> COMPLETION_ONLY = Set.of(TaskType.COMPLETION);
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public SenderService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        Objects.requireNonNull(factory);
        sender = factory.createSender();
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    protected Sender getSender() {
        return sender;
    }

    protected ServiceComponents getServiceComponents() {
        return serviceComponents;
    }

    @Override
    public void infer(
        Model model,
        @Nullable String query,
        List<String> input,
        boolean stream,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        init();
        if (query != null) {
            doInfer(model, new QueryAndDocsInputs(query, input, stream), taskSettings, inputType, timeout, listener);
        } else {
            doInfer(model, new DocumentsOnlyInput(input, stream), taskSettings, inputType, timeout, listener);
        }
    }

    @Override
    public void chunkedInfer(
        Model model,
        @Nullable String query,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        init();
        // a non-null query is not supported and is dropped by all providers
        doChunkedInfer(model, new DocumentsOnlyInput(input), taskSettings, inputType, chunkingOptions, timeout, listener);
    }

    protected abstract void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    );

    protected abstract void doChunkedInfer(
        Model model,
        DocumentsOnlyInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    );

    @Override
    public void start(Model model, ActionListener<Boolean> listener) {
        init();

        doStart(model, listener);
    }

    protected void doStart(Model model, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    private void init() {
        sender.start();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(sender);
    }
}
