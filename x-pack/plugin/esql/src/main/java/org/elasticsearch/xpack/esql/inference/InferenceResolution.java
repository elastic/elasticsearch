/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Collection;
import java.util.Map;

public class InferenceResolution {

    public static final InferenceResolution EMPTY = new InferenceResolution();

    private final Map<String, ResolvedInference> resolvedInferences = ConcurrentCollections.newConcurrentMap();

    private final Map<String, String> errors = ConcurrentCollections.newConcurrentMap();

    public ResolvedInference getResolvedInference(String inferenceId) {
        return resolvedInferences.get(inferenceId);
    }

    public Collection<ResolvedInference> resolvedInferences() {
        return resolvedInferences.values();
    }

    public String getError(String inferenceId) {
        final String error = errors.get(inferenceId);
        if (error != null) {
            return error;
        } else {
            assert false : "unresolved inference [" + inferenceId + "]";
            return "unresolved inference [" + inferenceId + "]";
        }
    }

    public void addResolvedInference(ResolvedInference resolvedInference) {
        resolvedInferences.putIfAbsent(resolvedInference.inferenceId(), resolvedInference);
    }

    public void addError(String inferenceId, String reason) {
        errors.putIfAbsent(inferenceId, reason);
    }
}
