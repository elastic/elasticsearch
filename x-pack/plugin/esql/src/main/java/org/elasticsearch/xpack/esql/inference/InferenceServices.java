/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;

/**
 * Provides centralized access to inference-related services for ESQL execution.
 * <p>
 * This service aggregates factories and utilities needed for inference operations,
 * including inference runners for executing individual and bulk inference requests,
 * and inference resolvers for collecting inference IDs from logical plans.
 * </p>
 */
public class InferenceServices {
    private final Client client;
    private final ThreadPool threadPool;
    private final InferenceRunner.Factory inferenceRunnerFactory;

    /**
     * Creates a new instance of the inference services with the specified client and thread pool.
     *
     * @param client     The client for interacting with the Elasticsearch cluster
     * @param threadPool The thread pool for executing inference operations
     */
    public InferenceServices(Client client, ThreadPool threadPool) {
        this.client = client;
        this.threadPool = threadPool;
        this.inferenceRunnerFactory = InferenceRunner.factory(client, threadPool);
    }

    /**
     * Creates an inference runner with the specified execution configuration using the default configuration.
     *
     * @return A configured inference runner capable of executing inference requests
     */
    public InferenceRunner inferenceRunner() {
        return inferenceRunner(InferenceExecutionConfig.DEFAULT);
    }

    /**
     * Creates an inference runner with the specified execution configuration.
     *
     * @param inferenceExecutionConfig Configuration specifying concurrency limits and execution parameters
     * @return A configured inference runner capable of executing inference requests
     */
    public InferenceRunner inferenceRunner(InferenceExecutionConfig inferenceExecutionConfig) {
        return inferenceRunnerFactory.create(inferenceExecutionConfig);
    }

    /**
     * Returns the inference runner factory for creating multiple configured runners.
     *
     * @return The factory for creating inference runners
     */
    public InferenceRunner.Factory inferenceRunnerFactory() {
        return inferenceRunnerFactory;
    }

    /**
     * Creates an inference resolver for collecting inference IDs from logical plans.
     *
     * @param functionRegistry The function registry for resolving inference functions
     * @return A resolver capable of extracting inference IDs from plans and expressions
     */
    public InferenceResolver inferenceResolver(EsqlFunctionRegistry functionRegistry) {
        return new InferenceResolver(functionRegistry, client);
    }

    public ThreadPool threadPool() {
        return threadPool;
    }
}
