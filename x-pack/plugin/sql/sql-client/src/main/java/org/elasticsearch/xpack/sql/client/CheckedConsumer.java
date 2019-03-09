/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client;

import java.util.function.Consumer;

/**
 * A {@link Consumer}-like interface which allows throwing checked exceptions.
 * Elasticsearch has one of these but we don't depend on Elasticsearch.
 */
@FunctionalInterface
public interface CheckedConsumer<T, E extends Exception> {
    void accept(T t) throws E;
}
