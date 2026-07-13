/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.core;

/**
 * NB: Light-clone from Core/Common library to keep JDBC driver independent.
 *
 * A {@link java.util.function.BiFunction}-like interface which allows throwing checked exceptions.
 */
@FunctionalInterface
public interface CheckedBiFunction<T, U, R, E extends Exception> {
    R apply(T t, U u) throws E;
}
