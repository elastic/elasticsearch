/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates an accumulation of validation errors. This exception allows multiple validation
 * errors to be collected and reported together, making it easier to provide comprehensive
 * feedback about what went wrong during validation.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * ValidationException validationException = new ValidationException();
 *
 * if (name == null || name.isEmpty()) {
 *     validationException.addValidationError("name cannot be null or empty");
 * }
 * if (age < 0) {
 *     validationException.addValidationError("age must be positive");
 * }
 *
 * // Throw if any validation errors were found
 * validationException.throwIfValidationErrorsExist();
 *
 * // Or manually check
 * if (!validationException.validationErrors().isEmpty()) {
 *     throw validationException;
 * }
 * }</pre>
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

    /**
     * Throws this exception if any validation errors have been accumulated.
     * This is a convenience method that allows for cleaner validation code.
     *
     * @throws ValidationException if there are any validation errors
     */
    public final void throwIfValidationErrorsExist() {
        if (validationErrors().isEmpty() == false) {
            throw this;
        }
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
