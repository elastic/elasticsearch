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

package org.elasticsearch.index.query;

import java.util.ArrayList;
import java.util.List;

/**
 * This exception can be used to indicate various reasons why validation of a query has failed.
 */
public class QueryValidationException extends IllegalArgumentException {

    private final List<String> validationErrors = new ArrayList<>();

    public QueryValidationException(String error) {
        super("query validation failed");
        validationErrors.add(error);
    }

    public void addValidationError(String error) {
        validationErrors.add(error);
    }

    public void addValidationErrors(Iterable<String> errors) {
        for (String error : errors) {
            validationErrors.add(error);
        }
    }

    public List<String> validationErrors() {
        return validationErrors;
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Validation Failed: ");
        int index = 0;
        for (String error : validationErrors) {
            sb.append(++index).append(": ").append(error).append(";");
        }
        return sb.toString();
    }

    /**
     * Helper method than can be used to add error messages to an existing {@link QueryValidationException}.
     * When passing {@code null} as the initial exception, a new exception is created.
     * @param validationError the error message to add to an initial exception
     * @param validationException an initial exception. Can be {@code null}, in which case a new exception is created.
     * @return a {@link QueryValidationException} with added validation error message
     */
    public static QueryValidationException addValidationError(String validationError, QueryValidationException validationException) {
        if (validationException == null) {
            validationException = new QueryValidationException(validationError);
        } else {
            validationException.addValidationError(validationError);
        }
        return validationException;
    }
}
