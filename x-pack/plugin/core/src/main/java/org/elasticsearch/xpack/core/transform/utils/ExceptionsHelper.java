/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.utils;

/**
 * Collection of methods to aid in creating and checking for exceptions.
 */
public class ExceptionsHelper {
    /**
     * A more REST-friendly Object.requireNonNull()
     */
    public static <T> T requireNonNull(T obj, String paramName) {
        if (obj == null) {
            throw new IllegalArgumentException("[" + paramName + "] must not be null.");
        }
        return obj;
    }

    /**
     * @see org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper#findSearchExceptionRootCause(Throwable)
     */
    public static Throwable findSearchExceptionRootCause(Throwable t) {
        return org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper.findSearchExceptionRootCause(t);
    }
}
