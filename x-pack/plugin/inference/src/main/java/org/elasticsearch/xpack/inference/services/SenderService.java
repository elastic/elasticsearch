/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
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

public abstract class SenderService implements InferenceService {
    protected static final Set<TaskType> COMPLETION_ONLY = EnumSet.of(TaskType.COMPLETION, TaskType.ANY);
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
        var inferenceInput = createInput(model, input, query, stream);
        doInfer(model, inferenceInput, taskSettings, inputType, timeout, listener);
    }

    private static InferenceInputs createInput(Model model, List<String> input, @Nullable String query, boolean stream) {
        return switch (model.getTaskType()) {
            case COMPLETION -> new ChatCompletionInput(input, stream);
            case RERANK -> new QueryAndDocsInputs(query, input, stream);
            case TEXT_EMBEDDING, SPARSE_EMBEDDING -> new DocumentsOnlyInput(input, stream);
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
        init();
        doUnifiedCompletionInfer(model, new UnifiedChatInput(request, true), timeout, listener);
    }

    @Override
    public void chunkedInfer(
        Model model,
        @Nullable String query,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        init();
        // a non-null query is not supported and is dropped by all providers
        doChunkedInfer(model, new DocumentsOnlyInput(input), taskSettings, inputType, timeout, listener);
    }

    protected abstract void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    );

    protected abstract void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    );

    protected abstract void doChunkedInfer(
        Model model,
        DocumentsOnlyInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    );

    public void start(Model model, ActionListener<Boolean> listener) {
        init();
        doStart(model, listener);
    }

    @Override
    public void start(Model model, @Nullable TimeValue unused, ActionListener<Boolean> listener) {
        start(model, listener);
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
