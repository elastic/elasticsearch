/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.runtime;

/**
 * Exception thrown when runtime evaluator generation or loading fails.
 * <p>
 * This exception wraps underlying errors (VerifyError, ClassFormatError, etc.)
 * to provide better error messages and prevent node crashes. When this exception
 * is thrown, the query should fail gracefully with an informative error message
 * rather than crashing the node.
 * <p>
 * Common causes:
 * <ul>
 *   <li>Bug in bytecode generation (VerifyError, ClassFormatError)</li>
 *   <li>Missing dependencies (NoClassDefFoundError, ClassNotFoundException)</li>
 *   <li>Invalid method annotations (IllegalArgumentException)</li>
 *   <li>Unsupported data types or parameter combinations</li>
 * </ul>
 */
public class RuntimeEvaluatorException extends RuntimeException {

    public RuntimeEvaluatorException(String message) {
        super(message);
    }

    public RuntimeEvaluatorException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Returns a user-friendly error message suitable for query error responses.
     */
    public String getUserMessage() {
        String msg = getMessage();
        Throwable cause = getCause();
        if (cause != null) {
            if (cause instanceof VerifyError || cause instanceof ClassFormatError) {
                return "Internal error: failed to generate evaluator for function. " + msg;
            } else if (cause instanceof NoClassDefFoundError || cause instanceof ClassNotFoundException) {
                return "Missing dependency for function. " + msg;
            }
        }
        return "Failed to evaluate function: " + msg;
    }
}
