/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules.function;

import java.io.Serializable;

/**
 * Functional interface for functions with four parameters that return a value.
 * <p>
 * This interface extends {@link Serializable} to support lambda serialization
 * for method reference resolution, and {@link VarargCallAdapter} to allow
 * conversion to variable-argument form.
 *
 * @param <R> the return type of the function
 * @param <A> the type of the first parameter
 * @param <B> the type of the second parameter
 * @param <C> the type of the third parameter
 * @param <D> the type of the fourth parameter
 */
public interface Call4<R, A, B, C, D> extends Serializable, VarargCallAdapter<R> {
    /**
     * Invokes the function with four arguments.
     *
     * @param arg0 the first argument
     * @param arg1 the second argument
     * @param arg2 the third argument
     * @param arg3 the fourth argument
     * @return the result of the function call
     * @throws Exception if the function call fails
     */
    R call(A arg0, B arg1, C arg2, D arg3) throws Exception;

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    default VarargCall<R> asVarargCall() {
        return args -> call((A) args[0], (B) args[1], (C) args[2], (D) args[3]);
    }
}
