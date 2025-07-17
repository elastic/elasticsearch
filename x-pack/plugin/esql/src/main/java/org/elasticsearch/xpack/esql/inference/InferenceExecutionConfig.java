/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

/**
 * Configuration record for inference execution parameters.
 * <p>
 * This record defines the concurrency and resource limits for inference operations,
 * including the number of worker threads and the maximum number of outstanding requests
 * that can be queued or processed simultaneously.
 * </p>
 *
 * @param workers                The number of worker threads for inference execution
 * @param maxOutstandingRequests The maximum number of concurrent inference requests allowed
 */
public record InferenceExecutionConfig(int workers, int maxOutstandingRequests) {

    /**
     * Default number of worker threads for inference execution.
     */
    public static final int DEFAULT_WORKERS = 10;

    /** Default maximum number of outstanding inference requests. */
    public static final int DEFAULT_MAX_OUTSTANDING_REQUESTS = 50;

    /**
     * Default configuration instance using standard values for most use cases.
     */
    public static final InferenceExecutionConfig DEFAULT = new InferenceExecutionConfig(DEFAULT_WORKERS, DEFAULT_MAX_OUTSTANDING_REQUESTS);

    public InferenceExecutionConfig {
        if (workers <= 0) throw new IllegalArgumentException("workers must be positive");
        if (maxOutstandingRequests <= 0) throw new IllegalArgumentException("maxOutstandingRequests must be positive");
    }
}
