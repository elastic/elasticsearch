/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

/**
 * Encapsulates parameters needed by evaluation.
 */
public final class EvaluationParameters {

    /**
     * Maximum number of buckets allowed in any single search request.
     */
    private final int maxBuckets;

    public EvaluationParameters(int maxBuckets) {
        this.maxBuckets = maxBuckets;
    }

    public int getMaxBuckets() {
        return maxBuckets;
    }
}
