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
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutionConfig;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutor;

public class InferenceServices {
    private final Client client;
    private final BulkInferenceExecutor.Factory bulkInferenceExecutorFactory;

    public InferenceServices(Client client, ThreadPool threadPool) {
        this.client = client;
        this.bulkInferenceExecutorFactory = new BulkInferenceExecutor.Factory(client, threadPool);
    }

    public BulkInferenceExecutor bulkInferenceExecutor(BulkInferenceExecutionConfig bulkExecutionConfig) {
        return bulkInferenceExecutorFactory.create(bulkExecutionConfig);
    }

    public BulkInferenceExecutor.Factory bulkInferenceExecutorFactory() {
        return bulkInferenceExecutorFactory;
    }

    public InferenceResolver inferenceResolver(EsqlFunctionRegistry functionRegistry) {
        return new InferenceResolver(functionRegistry, client);
    }
}
