/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import java.util.concurrent.ExecutorService;

public class InferenceExecutionContext {
    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 10;
    private final InferenceRunner inferenceRunner;
    private final ExecutorService executorService;
    private final int maxConcurrentRequests;

    private InferenceExecutionContext(InferenceRunner inferenceRunner, ExecutorService executorService, int maxConcurrentRequests) {
        this.inferenceRunner = inferenceRunner;
        this.executorService = executorService;
        this.maxConcurrentRequests = maxConcurrentRequests;
    }

    public InferenceRunner inferenceRunner() {
        return inferenceRunner;
    }

    public ExecutorService executorService() {
        return executorService;
    }

    public int maxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public static class Builder {
        private final InferenceRunner inferenceRunner;
        private final ExecutorService executorService;
        private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;

        Builder(InferenceRunner inferenceRunner, ExecutorService executorService) {
            this.inferenceRunner = inferenceRunner;
            this.executorService = executorService;
        }

        public InferenceExecutionContext build() {
            return new InferenceExecutionContext(inferenceRunner, executorService, maxConcurrentRequests);
        }

        public Builder setMaxConcurrentRequests(int maxConcurrentRequests) {
            this.maxConcurrentRequests = maxConcurrentRequests;
            return this;
        }
    }
}
