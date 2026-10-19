/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The {@code params} map of a script with allocation tracking on. A read of {@code _source} makes the server parse the whole
 * document into maps and lists, and no allowlist method sees that. This wrapper charges the parse the first time the parsed
 * map comes back. The server keeps one parsed map per document, so later reads in the same execution return the same object
 * and are not charged again. Every other key goes straight through.
 */
public final class ChargedParams implements Map<String, Object> {

    private static final String SOURCE = "_source";

    private final PainlessScript script;
    private final Map<String, Object> delegate;
    /** The parsed source map this execution has already paid for. */
    private Object charged;

    public ChargedParams(PainlessScript script, Map<String, Object> delegate) {
        this.script = script;
        this.delegate = delegate;
    }

    @Override
    public Object get(Object key) {
        Object value = delegate.get(key);
        if (value != charged && value instanceof Map<?, ?> source && SOURCE.equals(key)) {
            charged = value;
            script.$checkAllocBytes(AllocationEstimators.parsedSourceBytes(source));
        }
        return value;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate.containsValue(value);
    }

    @Override
    public Object put(String key, Object value) {
        return delegate.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return delegate.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        delegate.putAll(m);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Set<String> keySet() {
        return delegate.keySet();
    }

    @Override
    public Collection<Object> values() {
        return delegate.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return delegate.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
