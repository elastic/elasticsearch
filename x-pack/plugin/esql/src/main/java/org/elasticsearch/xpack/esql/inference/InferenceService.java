/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class InferenceService {

    private InferenceSettings inferenceSettings;

    private final Client client;
    private final ThreadPool threadPool;
    private final InferenceResolver.Factory inferenceResolverFactory;

    /**
     * Creates a new inference service with the given client.
     *
     * @param client the Elasticsearch client for inference operations
     * @param clusterService used to read and update inference settings
     */
    public InferenceService(Client client, ClusterService clusterService) {
        this(client, clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(this::updateInferenceSettings, InferenceSettings.getSettings());

    }

    private InferenceService(Client client, Settings settings) {
        this.client = client;
        this.threadPool = client.threadPool();
        this.inferenceResolverFactory = InferenceResolver.factory(client);
        updateInferenceSettings(settings);
    }

    /**
     * Returns the settings for ES|QL inference features.
     *
     * @return the inference settings
     */
    public InferenceSettings inferenceSettings() {
        return inferenceSettings;
    }

    private void updateInferenceSettings(Settings settings) {
        inferenceSettings = new InferenceSettings(settings);
    }

    /**
     * Creates an inference resolver for resolving inference IDs in logical plans.
     *
     * @param functionRegistry the function registry to resolve functions
     *
     * @return a new inference resolver instance
     */
    public InferenceResolver inferenceResolver(EsqlFunctionRegistry functionRegistry) {
        return inferenceResolverFactory.create(functionRegistry);
    }

    /**
     * Executes an inference request.
     *
     * @param request  the inference request to execute
     * @param listener the listener to notify upon completion
     */
    public void executeInference(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
        executeAsyncWithOrigin(client, INFERENCE_ORIGIN, InferenceAction.INSTANCE, request, listener);
    }

    public ThreadPool threadPool() {
        return threadPool;
    }

    public ThreadContext threadContext() {
        return threadPool.getThreadContext();
    }
}
