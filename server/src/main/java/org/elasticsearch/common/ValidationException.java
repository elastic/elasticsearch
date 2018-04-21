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

    /**
     * Add a new validation error to the accumulating validation errors
     * @param error the error to add
     */
    public final void addValidationError(String error) {
        validationErrors.add(error);
    }

    /**
     * Add a sequence of validation errors to the accumulating validation errors
     * @param errors the errors to add
     */
    public final void addValidationErrors(Iterable<String> errors) {
        for (String error : errors) {
            validationErrors.add(error);
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
