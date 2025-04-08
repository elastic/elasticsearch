/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class InferenceResolution {

    public static final InferenceResolution EMPTY = new InferenceResolution.Builder().build();

    public static InferenceResolution.Builder builder() {
        return new Builder();
    }

    private final Map<String, ResolvedInference> resolvedInferences;
    private final Map<String, String> errors;

    private InferenceResolution(Map<String, ResolvedInference> resolvedInferences, Map<String, String> errors) {
        this.resolvedInferences = Collections.unmodifiableMap(resolvedInferences);
        this.errors = Collections.unmodifiableMap(errors);
    }

    public ResolvedInference getResolvedInference(String inferenceId) {
        return resolvedInferences.get(inferenceId);
    }

    public Collection<ResolvedInference> resolvedInferences() {
        return resolvedInferences.values();
    }

    public boolean hasError() {
        return errors.isEmpty() == false;
    }

    public String getError(String inferenceId) {
        final String error = errors.get(inferenceId);
        if (error != null) {
            return error;
        } else {
            return "unresolved inference [" + inferenceId + "]";
        }
    }

    public static class Builder {

        private final Map<String, ResolvedInference> resolvedInferences;
        private final Map<String, String> errors;

        private Builder() {
            this.resolvedInferences = ConcurrentCollections.newConcurrentMap();
            this.errors = ConcurrentCollections.newConcurrentMap();
        }

        public Builder withResolvedInference(ResolvedInference resolvedInference) {
            resolvedInferences.putIfAbsent(resolvedInference.inferenceId(), resolvedInference);
            return this;
        }

        public Builder withError(String inferenceId, String reason) {
            errors.putIfAbsent(inferenceId, reason);
            return this;
        }

        public InferenceResolution build() {
            return new InferenceResolution(resolvedInferences, errors);
        }
    }
}
