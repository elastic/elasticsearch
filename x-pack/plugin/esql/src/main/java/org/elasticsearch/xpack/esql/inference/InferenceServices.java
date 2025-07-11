/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutionConfig;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutor;

public class InferenceServices {
    private final InferenceResolver inferenceResolver;
    private final BulkInferenceExecutor.Factory bulkInferenceExecutorFactory;

    public InferenceServices(Client client, ThreadPool threadPool) {
        this.inferenceResolver = new InferenceResolver(client, threadPool);
        this.bulkInferenceExecutorFactory = new BulkInferenceExecutor.Factory(client, threadPool);
    }

    public BulkInferenceExecutor bulkInferenceExecutor(BulkInferenceExecutionConfig bulkExecutionConfig) {
        return bulkInferenceExecutorFactory.create(bulkExecutionConfig);
    }

    public BulkInferenceExecutor.Factory bulkInferenceExecutorFactory() {
        return bulkInferenceExecutorFactory;
    }

    public InferenceResolver inferenceResolver() {
        return inferenceResolver;
    }
}
