/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public abstract class SenderService implements InferenceService {
    private final SetOnce<HttpRequestSenderFactory> factory;
    private final SetOnce<ServiceComponents> serviceComponents;
    private final AtomicReference<Sender> sender = new AtomicReference<>();

    public SenderService(SetOnce<HttpRequestSenderFactory> factory, SetOnce<ServiceComponents> serviceComponents) {
        this.factory = Objects.requireNonNull(factory);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    protected Sender getSender() {
        return sender.get();
    }

    protected ServiceComponents getServiceComponents() {
        return serviceComponents.get();
    }

    @Override
    public void infer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ActionListener<InferenceServiceResults> listener
    ) {
        init();

        doInfer(model, input, taskSettings, inputType, listener);
    }

    @Override
    public void chunkedInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        init();
        doChunkedInfer(model, input, taskSettings, inputType, chunkingOptions, listener);
    }

    protected abstract void doInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ActionListener<InferenceServiceResults> listener
    );

    protected abstract void doChunkedInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
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
        sender.updateAndGet(current -> Objects.requireNonNullElseGet(current, () -> factory.get().createSender(name())));
        sender.get().start();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(sender.get());
    }
}
