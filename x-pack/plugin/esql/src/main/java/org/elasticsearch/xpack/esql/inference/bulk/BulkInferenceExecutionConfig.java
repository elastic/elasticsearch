/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.core.TimeValue;

import java.util.concurrent.TimeUnit;

public record BulkInferenceExecutionConfig(TimeValue inferenceTimeout, int workers) {
    public static final TimeValue DEFAULT_INFERENCE_TIMEOUT = new TimeValue(10, TimeUnit.SECONDS);
    public static final int DEFAULT_WORKERS = 10;

    public static final BulkInferenceExecutionConfig DEFAULT = new BulkInferenceExecutionConfig(DEFAULT_INFERENCE_TIMEOUT, DEFAULT_WORKERS);
}
