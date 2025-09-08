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

/**
 * A utility class for resolving/setting values in the {@link ThreadContext} in a typesafe way
 */
public final class ThreadContextValue<T> {

    private final String key;
    private final Class<T> type;

    private ThreadContextValue(String key, Class<T> type) {
        this.key = key;
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public boolean exists(ThreadContext threadContext) {
        return threadContext.getTransient(key) != null;
    }

    public T get(ThreadContext threadContext) {
        final Object val = threadContext.getTransient(key);
        if (val == null) {
            return null;
        }
        if (this.type.isInstance(val)) {
            return this.type.cast(val);
        } else {
            final String message = Strings.format(
                "Found object [%s] as transient [%s] in thread-context; expected to be [%s] but is [%s]",
                val,
                key,
                type,
                val.getClass()
            );
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

    public T require(ThreadContext threadContext) {
        final T value = get(threadContext);
        if (value == null) {
            throw new IllegalStateException("Cannot find value for [" + key + "] in thread-context");
        }
        return value;
    }

    public void set(ThreadContext threadContext, T value) {
        threadContext.putTransient(this.key, value);
    }

    public void setIfEmpty(ThreadContext threadContext, T value) {
        if (exists(threadContext) == false) {
            set(threadContext, value);
        }
    }

    public static <T> ThreadContextValue<T> transientValue(String key, Class<T> type) {
        return new ThreadContextValue<>(key, type);
    }
}
