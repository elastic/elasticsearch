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
 * Functional interface for functions with no parameters that return a value.
 * <p>
 * This interface extends {@link Serializable} to support lambda serialization
 * for method reference resolution, and {@link VarargCallAdapter} to allow
 * conversion to variable-argument form.
 *
 * @param <R> the return type of the function
 */
public interface Call0<R> extends Serializable, VarargCallAdapter<R> {
    /**
     * Invokes the function with no arguments.
     *
     * @return the result of the function call
     * @throws Exception if the function call fails
     */
    R call() throws Exception;

    /**
     * {@inheritDoc}
     */
    @Override
    default VarargCall<R> asVarargCall() {
        return args -> call();
    }
}
