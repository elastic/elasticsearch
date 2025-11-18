/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class ErrorResponse {

    // Denotes an error object that was not found
    public static final ErrorResponse UNDEFINED_ERROR = new ErrorResponse(false);

    private final String errorMessage;
    private final boolean errorStructureFound;

    public ErrorResponse(String errorMessage) {
        this.errorMessage = Objects.requireNonNull(errorMessage);
        this.errorStructureFound = true;
    }

    protected ErrorResponse(boolean errorStructureFound) {
        this.errorMessage = "";
        this.errorStructureFound = errorStructureFound;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean errorStructureFound() {
        return errorStructureFound;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ErrorResponse that = (ErrorResponse) o;
        return errorStructureFound == that.errorStructureFound && Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(errorMessage, errorStructureFound);
    }

    /**
     * Creates an ErrorResponse from the given HttpResult.
     * Attempts to read the body as a UTF-8 string and constructs an ErrorResponse.
     * If reading fails, returns a generic UNDEFINED_ERROR.
     *
     * @param response the HttpResult containing the error response
     * @return an ErrorResponse instance
     */
    public static ErrorResponse fromResponse(HttpResult response) {
        try {
            String errorMessage = new String(response.body(), StandardCharsets.UTF_8);
            return new ErrorResponse(errorMessage);
        } catch (Exception e) {
            // swallow the error
        }

        return ErrorResponse.UNDEFINED_ERROR;
    }

    /**
     * Parses a string response into an ErrorResponse.
     * If the string is not blank, creates a new ErrorResponse with the string as the error message.
     * If the string is blank, returns UNDEFINED_ERROR.
     *
     * @param response the error response as a string
     * @return an ErrorResponse instance
     */
    public static ErrorResponse fromString(String response) {
        if (Strings.isNullOrBlank(response) == false) {
            return new ErrorResponse(response);
        } else {
            return ErrorResponse.UNDEFINED_ERROR;
        }
    }
}
