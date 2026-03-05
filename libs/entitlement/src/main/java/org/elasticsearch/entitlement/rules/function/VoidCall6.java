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
 * Functional interface for functions with six parameters that return void.
 * <p>
 * This interface extends {@link Serializable} to support lambda serialization
 * for method reference resolution.
 *
 * @param <A> the type of the first parameter
 * @param <B> the type of the second parameter
 * @param <C> the type of the third parameter
 * @param <D> the type of the fourth parameter
 * @param <E> the type of the fifth parameter
 * @param <F> the type of the sixth parameter
 */
public interface VoidCall6<A, B, C, D, E, F> extends Serializable {
    /**
     * Invokes the function with six arguments.
     *
     * @param arg0 the first argument
     * @param arg1 the second argument
     * @param arg2 the third argument
     * @param arg3 the fourth argument
     * @param arg4 the fifth argument
     * @param arg5 the sixth argument
     * @throws Exception if the function call fails
     */
    void call(A arg0, B arg1, C arg2, D arg3, E arg4, F arg5) throws Exception;
}
