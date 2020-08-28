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

import org.elasticsearch.common.ValidationException;

import java.util.List;

/**
 * This exception can be used to indicate various reasons why validation of a query has failed.
 */
public class QueryValidationException extends ValidationException {

    /**
     * Helper method than can be used to add error messages to an existing {@link QueryValidationException}.
     * When passing {@code null} as the initial exception, a new exception is created.
     *
     * @param queryId the query that caused the error
     * @param validationError the error message to add to an initial exception
     * @param validationException an initial exception. Can be {@code null}, in which case a new exception is created.
     * @return a {@link QueryValidationException} with added validation error message
     */
    public static QueryValidationException addValidationError(String queryId, String validationError,
                                                                    QueryValidationException validationException) {
        if (validationException == null) {
            validationException = new QueryValidationException();
        }
        validationException.addValidationError("[" + queryId + "] " + validationError);
        return validationException;
    }

    /**
     * Helper method than can be used to add error messages to an existing {@link QueryValidationException}.
     * When passing {@code null} as the initial exception, a new exception is created.
     * @param validationErrors the error messages to add to an initial exception
     * @param validationException an initial exception. Can be {@code null}, in which case a new exception is created.
     * @return a {@link QueryValidationException} with added validation error message
     */
    public static QueryValidationException addValidationErrors(List<String> validationErrors,
                                                                    QueryValidationException validationException) {
        if (validationException == null) {
            validationException = new QueryValidationException();
        }
        validationException.addValidationErrors(validationErrors);
        return validationException;
    }
}
