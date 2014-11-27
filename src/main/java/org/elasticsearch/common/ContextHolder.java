/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import com.carrotsearch.hppc.ObjectObjectAssociativeContainer;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;

/**
 *
 */
public class ContextHolder {

    private ObjectObjectOpenHashMap<Object, Object> context;

    /**
     * Attaches the given value to the context.
     *
     * @return  The previous value that was associated with the given key in the context, or
     *          {@code null} if there was none.
     */
    @SuppressWarnings("unchecked")
    public final synchronized <V> V putInContext(Object key, Object value) {
        if (context == null) {
            context = new ObjectObjectOpenHashMap<>(2);
        }
        return (V) context.put(key, value);
    }

    /**
     * Attaches the given values to the context
     */
    public final synchronized void putAllInContext(ObjectObjectAssociativeContainer<Object, Object> map) {
        if (map == null) {
            return;
        }
        if (context == null) {
            context = new ObjectObjectOpenHashMap<>(map);
        } else {
            context.putAll(map);
        }
    }

    /**
     * @return  The context value that is associated with the given key
     *
     * @see     #putInContext(Object, Object)
     */
    @SuppressWarnings("unchecked")
    public final synchronized <V> V getFromContext(Object key) {
        return context != null ? (V) context.get(key) : null;
    }

    /**
     * @param defaultValue  The default value that should be returned for the given key, if no
     *                      value is currently associated with it.
     *
     * @return  The value that is associated with the given key in the context
     *
     * @see     #putInContext(Object, Object)
     */
    @SuppressWarnings("unchecked")
    public final synchronized <V> V getFromContext(Object key, V defaultValue) {
        V value = getFromContext(key);
        return value == null ? defaultValue : value;
    }

    /**
     * Checks if the context contains an entry with the given key
     */
    public final synchronized boolean hasInContext(Object key) {
        return context != null && context.containsKey(key);
    }

    /**
     * @return  The number of values attached in the context.
     */
    public final synchronized int contextSize() {
        return context != null ? context.size() : 0;
    }

    /**
     * Checks if the context is empty.
     */
    public final synchronized boolean isContextEmpty() {
        return context == null || context.isEmpty();
    }

    /**
     * @return  A safe immutable copy of the current context.
     */
    public synchronized ImmutableOpenMap<Object, Object> getContext() {
        return context != null ? ImmutableOpenMap.copyOf(context) : ImmutableOpenMap.of();
    }

    /**
     * Copies the context from the given context holder to this context holder. Any shared keys between
     * the two context will be overridden by the given context holder.
     */
    public synchronized void copyContextFrom(ContextHolder other) {
        synchronized (other) {
            if (other.context == null) {
                return;
            }
            if (context == null) {
                context = new ObjectObjectOpenHashMap<>(other.context);
            } else {
                context.putAll(other.context);
            }
        }
    }
}
