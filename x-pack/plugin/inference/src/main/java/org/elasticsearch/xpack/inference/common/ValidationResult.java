/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * Represents the result of validating a settings object.
 * @param <T> The type of the result if validation is successful. This will be null if validation failed or is undefined.
 */
public final class ValidationResult<T> {

    private static final ValidationResult<?> UNDEFINED_RESULT = new ValidationResult<>(null, ValidationState.UNDEFINED);
    private static final ValidationResult<?> FAILED_RESULT = new ValidationResult<>(null, ValidationState.FAILED);

    public enum ValidationState {
        SUCCESS,
        FAILED,
        UNDEFINED
    }

    public static <T> ValidationResult<T> success(T result) {
        return new ValidationResult<>(Objects.requireNonNull(result), ValidationState.SUCCESS);
    }

    public static <T> ValidationResult<T> failed() {
        @SuppressWarnings("unchecked")
        var failed = (ValidationResult<T>) FAILED_RESULT;
        return failed;
    }

    public static <T> ValidationResult<T> undefined() {
        @SuppressWarnings("unchecked")
        var undefined = (ValidationResult<T>) UNDEFINED_RESULT;
        return undefined;
    }

    private final T result;
    private final ValidationState state;

    private ValidationResult(@Nullable T result, ValidationState state) {
        this.result = result;
        this.state = state;
    }

    public T result() {
        return result;
    }

    public boolean isSuccess() {
        return state == ValidationState.SUCCESS;
    }

    public boolean isFailed() {
        return state == ValidationState.FAILED;
    }

    public boolean isUndefined() {
        return state == ValidationState.UNDEFINED;
    }
}
