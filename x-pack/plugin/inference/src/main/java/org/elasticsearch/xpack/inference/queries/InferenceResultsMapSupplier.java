/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.inference.InferenceResults;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

class InferenceResultsMapSupplier implements Supplier<Map<FullyQualifiedInferenceId, InferenceResults>> {
    private volatile Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap = null;

    void set(Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap) {
        this.inferenceResultsMap = inferenceResultsMap;
    }

    @Override
    public Map<FullyQualifiedInferenceId, InferenceResults> get() {
        return inferenceResultsMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceResultsMapSupplier that = (InferenceResultsMapSupplier) o;
        return Objects.equals(inferenceResultsMap, that.inferenceResultsMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inferenceResultsMap);
    }
}
