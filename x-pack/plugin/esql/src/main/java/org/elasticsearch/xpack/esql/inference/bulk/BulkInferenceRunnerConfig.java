/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

/**
 * Configuration record for inference execution parameters.
 * <p>
 * This record defines the concurrency and resource limits for inference operations,
 * including the number of worker threads and the maximum number of outstanding requests
 * that can be queued or processed simultaneously.
 * </p>
 *
 * @param maxOutstandingBulkRequests The maximum number of concurrent bulk inference requests allowed
 * @param maxOutstandingRequests     The maximum number of concurrent inference requests allowed
 */
public record BulkInferenceRunnerConfig(int maxOutstandingRequests, int maxOutstandingBulkRequests) {

    /**
     * Default number of worker threads for inference execution.
     */
    public static final int DEFAULT_MAX_OUTSTANDING_BULK_REQUESTS = 10;

    /** Default maximum number of outstanding inference requests. */
    public static final int DEFAULT_MAX_OUTSTANDING_REQUESTS = 50;

    /**
     * Default configuration instance using standard values for most use cases.
     */
    public static final BulkInferenceRunnerConfig DEFAULT = new BulkInferenceRunnerConfig(
        DEFAULT_MAX_OUTSTANDING_REQUESTS,
        DEFAULT_MAX_OUTSTANDING_BULK_REQUESTS
    );

    public BulkInferenceRunnerConfig {
        if (maxOutstandingRequests <= 0) throw new IllegalArgumentException("maxOutstandingRequests must be positive");
        if (maxOutstandingBulkRequests <= 0) throw new IllegalArgumentException("maxOutstandingBulkRequests must be positive");

        if (maxOutstandingBulkRequests > maxOutstandingRequests) {
            maxOutstandingBulkRequests = maxOutstandingRequests;
        }
    }
}
