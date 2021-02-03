/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
