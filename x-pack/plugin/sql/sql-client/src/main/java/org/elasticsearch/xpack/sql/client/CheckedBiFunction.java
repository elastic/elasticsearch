/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.client;

import java.util.function.BiFunction;

/**
 * A {@link BiFunction}-like interface which allows throwing checked exceptions.
 * Elasticsearch has one of these but we don't depend on Elasticsearch.
 */
@FunctionalInterface
public interface CheckedBiFunction<T, U, R, E extends Exception> {
    R apply(T t, U u) throws E;
}
