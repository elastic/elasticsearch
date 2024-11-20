/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

public class UnifiedCompletionInputs extends InferenceInputs {
    public static UnifiedCompletionInputs of(InferenceInputs inferenceInputs) {
        return InferenceInputs.abc(inferenceInputs, UnifiedCompletionInputs.class);
    }

    private final Object parameters;
    private final boolean stream;

    public UnifiedCompletionInputs(Object parameters) {
        super();
        this.parameters = parameters;
        // TODO retrieve this from the parameters eventually
        this.stream = true;
    }

    public Object parameters() {
        return parameters;
    }

    public boolean stream() {
        return stream;
    }
}
