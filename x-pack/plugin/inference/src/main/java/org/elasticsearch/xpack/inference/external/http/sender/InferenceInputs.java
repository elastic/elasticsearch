/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.common.Strings;

public abstract class InferenceInputs {
    private final boolean stream;

    public InferenceInputs(boolean stream) {
        this.stream = stream;
    }

    public static IllegalArgumentException createUnsupportedTypeException(InferenceInputs inferenceInputs, Class<?> clazz) {
        return new IllegalArgumentException(
            Strings.format("Unable to convert inference inputs type: [%s] to [%s]", inferenceInputs.getClass(), clazz)
        );
    }

    public <T extends InferenceInputs> T castTo(Class<T> clazz) {
        if (clazz.isInstance(this) == false) {
            throw createUnsupportedTypeException(this, clazz);
        }

        return clazz.cast(this);
    }

    public boolean stream() {
        return stream;
    }

    public abstract boolean isSingleInput();
}
