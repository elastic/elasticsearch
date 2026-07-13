/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules.function;

/**
 * Functional interface for calling functions with variable arguments.
 * <p>
 * This interface is used internally by the entitlement system to represent
 * method calls with varying numbers of arguments in a uniform way.
 *
 * @param <R> the return type of the function
 */
public interface VarargCall<R> {
    /**
     * Invokes the function with the specified arguments.
     *
     * @param args the arguments to pass to the function
     * @return the result of the function call
     * @throws Exception if the function call fails
     */
    R call(Object... args) throws Exception;
}
