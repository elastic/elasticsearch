/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client;

import org.elasticsearch.common.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Encapsulates an accumulation of validation errors
 */
public class ValidationException extends IllegalArgumentException {

    /**
     * Creates {@link ValidationException} instance initialized with given error messages.
     * @param error the errors to add
     * @return {@link ValidationException} instance
     */
    public static ValidationException withError(String... error) {
        return withErrors(Arrays.asList(error));
    }

    /**
     * Creates {@link ValidationException} instance initialized with given error messages.
     * @param errors the list of errors to add
     * @return {@link ValidationException} instance
     */
    public static ValidationException withErrors(List<String> errors) {
        ValidationException e = new ValidationException();
        for (String error : errors) {
            e.addValidationError(error);
        }
        return e;
    }

    private final List<String> validationErrors = new ArrayList<>();

    /**
     * Add a new validation error to the accumulating validation errors
     * @param error the error to add
     */
    public void addValidationError(final String error) {
        validationErrors.add(error);
    }

    /**
     * Adds validation errors from an existing {@link ValidationException} to
     * the accumulating validation errors
     * @param exception the {@link ValidationException} to add errors from
     */
    public final void addValidationErrors(final @Nullable ValidationException exception) {
        if (exception != null) {
            for (String error : exception.validationErrors()) {
                addValidationError(error);
            }
        }
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
