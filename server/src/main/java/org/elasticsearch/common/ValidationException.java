/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates an accumulation of validation errors
 */
public class ValidationException extends IllegalArgumentException {
    private final List<String> validationErrors = new ArrayList<>();

    public ValidationException() {
        super("validation failed");
    }

    public ValidationException(Throwable cause) {
        super(cause);
    }

    /**
     * Add a new validation error to the accumulating validation errors
     * @param error the error to add
     */
    public final ValidationException addValidationError(String error) {
        validationErrors.add(error);
        return this;
    }

    /**
     * Add a sequence of validation errors to the accumulating validation errors
     * @param errors the errors to add
     */
    public final ValidationException addValidationErrors(Iterable<String> errors) {
        for (String error : errors) {
            validationErrors.add(error);
        }
        return this;
    }

    /**
     * Returns the validation errors accumulated
     */
    public final List<String> validationErrors() {
        return validationErrors;
    }

    @Override
    public final String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Validation Failed: ");
        int index = 0;
        for (String error : validationErrors) {
            sb.append(++index).append(": ").append(error).append(";");
        }
        return sb.toString();
    }
}
