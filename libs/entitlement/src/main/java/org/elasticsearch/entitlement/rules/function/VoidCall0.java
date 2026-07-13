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
 * Functional interface for functions with no parameters that return void.
 * <p>
 * This interface extends {@link Serializable} to support lambda serialization
 * for method reference resolution.
 */
@FunctionalInterface
public interface VoidCall0 extends Serializable {
    /**
     * Invokes the function with no arguments.
     *
     * @throws Exception if the function call fails
     */
    void call() throws Exception;
}
