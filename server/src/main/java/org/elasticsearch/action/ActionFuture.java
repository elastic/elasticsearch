/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.core.TimeValue;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * An extension to {@link Future} allowing for simplified "get" operations.
 *
 *
 */
public interface ActionFuture<T> extends Future<T> {

    /**
     * Similar to {@link #get()}, just catching the {@link InterruptedException} and throwing
     * an {@link IllegalStateException} instead. Also catches
     * {@link java.util.concurrent.ExecutionException} and throws the actual cause instead.
     */
    T actionGet();

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just catching the {@link InterruptedException} and throwing
     * an {@link IllegalStateException} instead. Also catches
     * {@link java.util.concurrent.ExecutionException} and throws the actual cause instead.
     */
    T actionGet(String timeout);

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just catching the {@link InterruptedException} and throwing
     * an {@link IllegalStateException} instead. Also catches
     * {@link java.util.concurrent.ExecutionException} and throws the actual cause instead.
     *
     * @param timeoutMillis Timeout in millis
     */
    T actionGet(long timeoutMillis);

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just catching the {@link InterruptedException} and throwing
     * an {@link IllegalStateException} instead. Also catches
     * {@link java.util.concurrent.ExecutionException} and throws the actual cause instead.
     */
    T actionGet(long timeout, TimeUnit unit);

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just catching the {@link InterruptedException} and throwing
     * an {@link IllegalStateException} instead. Also catches
     * {@link java.util.concurrent.ExecutionException} and throws the actual cause instead.
     */
    T actionGet(TimeValue timeout);
}
