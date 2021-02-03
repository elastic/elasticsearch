/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

/**
 * A {@link java.util.function.BiFunction}-like interface which allows throwing checked exceptions.
 */
@FunctionalInterface
public interface CheckedBiFunction<T, U, R, E extends Exception> {
    R apply(T t, U u) throws E;
}
