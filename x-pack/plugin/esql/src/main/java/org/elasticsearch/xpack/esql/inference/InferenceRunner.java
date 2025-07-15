/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

import java.util.List;

/**
 * Interface for executing individual and bulk inference requests with concurrency control.
 * <p>
 * Implementations provide throttling and resource management to ensure efficient execution
 * of inference operations while respecting system resource limits.
 * </p>
 */
public interface InferenceRunner {

    /**
     * Executes a single inference request asynchronously.
     *
     * @param request  The inference request to execute
     * @param listener Callback listener for the inference response or error
     */
    void execute(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener);

    /**
     * Executes multiple inference requests in bulk with coordinated processing.
     * <p>
     * This method provides efficient batch processing of inference requests while maintaining
     * ordering and providing consolidated results or error handling.
     * </p>
     *
     * @param requests An iterator over the inference requests to be executed in bulk
     * @param listener Callback listener for the list of inference responses or error
     */
    void executeBulk(BulkInferenceRequestIterator requests, ActionListener<List<InferenceAction.Response>> listener);

    public static Factory factory(Client client, ThreadPool threadPool) {
        return new ThrottledInferenceRunner.Factory(client, threadPool);
    }

    interface Factory {
        /**
         * Creates a new inference runner with the specified execution configuration.
         *
         * @param inferenceExecutionConfig Configuration defining concurrency limits and execution parameters
         * @return A configured inference runner implementation
         */
        InferenceRunner create(InferenceExecutionConfig inferenceExecutionConfig);

    }

}
