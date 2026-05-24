/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logstashbridge.core;

import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

/**
 * A stable interface on top of {@link CheckedBiFunction}.
 * @param <T> type of lhs parameter
 * @param <U> type of rhs parameter
 * @param <R> type of return value
 * @param <E> type of anticipated exception
 */
@FunctionalInterface
public interface CheckedBiFunctionBridge<T, U, R, E extends Exception> extends StableBridgeAPI<CheckedBiFunction<T, U, R, E>> {
    R apply(T t, U u) throws E;

    @Override
    default CheckedBiFunction<T, U, R, E> toInternal() {
        return this::apply;
    }
}
