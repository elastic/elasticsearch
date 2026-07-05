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
 * Interface for converting fixed-arity function interfaces to variable-argument form.
 * <p>
 * This adapter interface allows the various {@code Call} and {@code VoidCall} interfaces
 * with fixed numbers of parameters to be converted into a uniform {@link VarargCall}
 * representation for internal processing.
 *
 * @param <R> the return type of the function
 */
public interface VarargCallAdapter<R> {
    /**
     * Converts this fixed-arity function to a variable-argument function.
     *
     * @return a {@link VarargCall} equivalent of this function
     */
    VarargCall<R> asVarargCall();
}
