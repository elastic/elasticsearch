/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.core;

import java.util.function.Consumer;

/**
 * NB: Light-clone from Core library to keep JDBC driver independent.
 *
 * A {@link Consumer}-like interface which allows throwing checked exceptions.
 */
@FunctionalInterface
public interface CheckedConsumer<T, E extends Exception> {
    void accept(T t) throws E;
}
