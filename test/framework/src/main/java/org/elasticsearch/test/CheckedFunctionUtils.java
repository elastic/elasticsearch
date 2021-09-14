/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.CheckedFunction;

import static org.mockito.Matchers.any;

/**
 * Test utilities for working with {@link CheckedFunction}s and {@link CheckedSupplier}s.
 */
public class CheckedFunctionUtils {

    /**
     * Returns a Mockito matcher for any argument that is an {@link CheckedFunction}.
     * @param <T> the function input type that the caller expects. Do not specify this, it will be inferred
     * @param <R> the function output type that the caller expects. Do not specify this, it will be inferred
     * @param <E> the function exception type that the caller expects. Do not specify this, it will be inferred
     * @return a checked function matcher
     */
    @SuppressWarnings("unchecked")
    public static <T, R, E extends Exception> CheckedFunction<T, R, E> anyCheckedFunction() {
        return any(CheckedFunction.class);
    }

    /**
     * Returns a Mockito matcher for any argument that is an {@link CheckedSupplier}.
     * @param <R> the supplier output type that the caller expects. Do not specify this, it will be inferred
     * @param <E> the supplier exception type that the caller expects. Do not specify this, it will be inferred
     * @return a checked supplier matcher
     */
    @SuppressWarnings("unchecked")
    public static <R, E extends Exception> CheckedSupplier<R, E> anyCheckedSupplier() {
        return any(CheckedSupplier.class);
    }
}
