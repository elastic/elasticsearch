/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;

/**
 * A utility class for resolving/setting values in the {@link ThreadContext} in a typesafe way
 * @see ThreadContext#getTransient(String)
 * @see ThreadContext#putTransient(String, Object)
 */
public final class ThreadContextTransient<T> {

    private final String key;
    private final Class<T> type;

    private ThreadContextTransient(String key, Class<T> type) {
        this.key = key;
        this.type = type;
    }

    /**
     * @return The key/name of the transient header
     */
    public String getKey() {
        return key;
    }

    /**
     * @return {@code true} if the thread context contains a non-null value for this {@link #getKey() key}
     */
    public boolean exists(ThreadContext threadContext) {
        return threadContext.getTransient(key) != null;
    }

    /**
     * @return The current value for this {@link #getKey() key}. May be {@code null}.
     */
    @Nullable
    public T get(ThreadContext threadContext) {
        final Object val = threadContext.getTransient(key);
        if (val == null) {
            return null;
        }
        if (this.type.isInstance(val)) {
            return this.type.cast(val);
        } else {
            final String message = Strings.format(
                "Found object of type [%s] as transient [%s] in thread-context, but expected it to be [%s]",
                val.getClass(),
                key,
                type
            );
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

    /**
     * @return The current value for this {@link #getKey() key}. May not be {@code null}
     * @throws IllegalStateException if the thread context does not contain a value (or contains {@code null}).
     */
    public T require(ThreadContext threadContext) {
        final T value = get(threadContext);
        if (value == null) {
            throw new IllegalStateException("Cannot find value for [" + key + "] in thread-context");
        }
        return value;
    }

    /**
     * Set the value for the this {@link #getKey() key}.
     * Because transient headers cannot be overwritten, this method will throw an exception if a value already exists
     * @see ThreadContext#putTransient(String, Object)
     */
    public void set(ThreadContext threadContext, T value) {
        threadContext.putTransient(this.key, value);
    }

    /**
     * Set the value for the this {@link #getKey() key}, if and only if there is no current value
     * @return {@code true} if the value was set, {@code false} otherwise
     */
    public boolean setIfEmpty(ThreadContext threadContext, T value) {
        if (exists(threadContext) == false) {
            set(threadContext, value);
            return true;
        } else {
            return false;
        }
    }

    public static <T> ThreadContextTransient<T> transientValue(String key, Class<T> type) {
        return new ThreadContextTransient<>(key, type);
    }
}
