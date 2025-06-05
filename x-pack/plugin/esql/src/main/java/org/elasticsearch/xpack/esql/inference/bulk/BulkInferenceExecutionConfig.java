/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

public record BulkInferenceExecutionConfig(int workers, int maxOutstandingRequests) {
    public static final int DEFAULT_WORKERS = 10;
    public static final int DEFAULT_MAX_OUTSTANDING_REQUESTS = 50;

    public static final BulkInferenceExecutionConfig DEFAULT = new BulkInferenceExecutionConfig(
        DEFAULT_WORKERS,
        DEFAULT_MAX_OUTSTANDING_REQUESTS
    );
}
