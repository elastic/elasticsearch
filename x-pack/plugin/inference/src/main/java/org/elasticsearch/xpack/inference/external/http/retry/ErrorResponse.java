/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

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
}
