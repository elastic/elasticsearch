/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.service.windows;

/**
 * Exception thrown when a Windows service control operation fails.
 */
public class WindowsServiceException extends Exception {

    private final int errorCode;

    public WindowsServiceException(String message, int errorCode) {
        super(message + " (error code: " + errorCode + ")");
        this.errorCode = errorCode;
    }

    public WindowsServiceException(String message) {
        super(message);
        this.errorCode = 0;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
