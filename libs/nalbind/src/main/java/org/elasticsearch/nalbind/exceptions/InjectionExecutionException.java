/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.exceptions;

/**
 * Indicates a bug in the injector; usually an invalid plan. These should never be thrown if the injector is working correctly.
 */
public class InjectionExecutionException extends IllegalStateException {
    public InjectionExecutionException(String s) {
        super(s);
    }

    public InjectionExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
