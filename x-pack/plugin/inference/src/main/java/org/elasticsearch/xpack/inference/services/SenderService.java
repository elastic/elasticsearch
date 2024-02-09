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
import org.elasticsearch.xpack.inference.external.http.sender.BaseRequestManagerFactory;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManagerFactory;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public abstract class SenderService implements InferenceService {
    private final SetOnce<HttpRequestSender.HttpRequestSenderFactory> httpRequestSenderFactory;
    private final SetOnce<ServiceComponents> serviceComponents;
    private final AtomicReference<Sender> sender = new AtomicReference<>();
    private final RequestManagerFactory requestManagerFactory;

    public SenderService(
        SetOnce<HttpRequestSender.HttpRequestSenderFactory> httpRequestSenderFactory,
        SetOnce<ServiceComponents> serviceComponents
    ) {
        this(httpRequestSenderFactory, serviceComponents, new BaseRequestManagerFactory());
    }

    public SenderService(
        SetOnce<HttpRequestSender.HttpRequestSenderFactory> httpRequestSenderFactory,
        SetOnce<ServiceComponents> serviceComponents,
        RequestManagerFactory requestManagerFactory
    ) {
        this.httpRequestSenderFactory = Objects.requireNonNull(httpRequestSenderFactory);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
        this.requestManagerFactory = Objects.requireNonNull(requestManagerFactory);
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
        ActionListener<ChunkedInferenceServiceResults> listener
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
        ActionListener<ChunkedInferenceServiceResults> listener
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
        sender.updateAndGet(
            current -> Objects.requireNonNullElseGet(
                current,
                () -> httpRequestSenderFactory.get().createSender(name(), requestManagerFactory)
            )
        );
        sender.get().start();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(sender.get());
    }
}
