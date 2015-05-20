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
import com.carrotsearch.hppc.ObjectObjectHashMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ContextAndHeaderHolder implements HasContextAndHeaders {

    private ObjectObjectHashMap<Object, Object> context;
    protected Map<String, Object> headers;

    @SuppressWarnings("unchecked")
    @Override
    public final synchronized <V> V putInContext(Object key, Object value) {
        if (context == null) {
            context = new ObjectObjectHashMap<>(2);
        }
        return (V) context.put(key, value);
    }

    @Override
    public final synchronized void putAllInContext(ObjectObjectAssociativeContainer<Object, Object> map) {
        if (map == null) {
            return;
        }
        if (context == null) {
            context = new ObjectObjectHashMap<>(map);
        } else {
            context.putAll(map);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public final synchronized <V> V getFromContext(Object key) {
        return context != null ? (V) context.get(key) : null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final synchronized <V> V getFromContext(Object key, V defaultValue) {
        V value = getFromContext(key);
        return value == null ? defaultValue : value;
    }

    @Override
    public final synchronized boolean hasInContext(Object key) {
        return context != null && context.containsKey(key);
    }

    @Override
    public final synchronized int contextSize() {
        return context != null ? context.size() : 0;
    }

    @Override
    public final synchronized boolean isContextEmpty() {
        return context == null || context.isEmpty();
    }

    @Override
    public synchronized ImmutableOpenMap<Object, Object> getContext() {
        return context != null ? ImmutableOpenMap.copyOf(context) : ImmutableOpenMap.of();
    }

    @Override
    public synchronized void copyContextFrom(HasContext other) {
        if (other == null) {
            return;
        }

        synchronized (other) {
            ImmutableOpenMap<Object, Object> otherContext = other.getContext();
            if (otherContext == null) {
                return;
            }
            if (context == null) {
                ObjectObjectHashMap<Object, Object> map = new ObjectObjectHashMap<>(other.getContext().size());
                map.putAll(otherContext);
                this.context = map;
            } else {
                context.putAll(otherContext);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void putHeader(String key, Object value) {
        if (headers == null) {
            headers = new HashMap<>();
        }
        headers.put(key, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <V> V getHeader(String key) {
        return headers != null ? (V) headers.get(key) : null;
    }

    @Override
    public final boolean hasHeader(String key) {
        return headers != null && headers.containsKey(key);
    }

    @Override
    public Set<String> getHeaders() {
        return headers != null ? headers.keySet() : Collections.<String>emptySet();
    }

    @Override
    public void copyHeadersFrom(HasHeaders from) {
        if (from != null && from.getHeaders() != null && !from.getHeaders().isEmpty()) {
            for (String headerName : from.getHeaders()) {
                putHeader(headerName, from.getHeader(headerName));
            }
        }
    }

    @Override
    public void copyContextAndHeadersFrom(HasContextAndHeaders other) {
        copyContextFrom(other);
        copyHeadersFrom(other);
    }
}
