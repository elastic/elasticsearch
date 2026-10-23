/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.lookup.Source;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * DynamicMap is used to wrap a Map for a script parameter. A set of
 * functions is provided for the overridden values where the function
 * is applied to the existing value when one exists for the
 * corresponding key.
 */
public final class DynamicMap implements Map<String, Object> {

    /**
     * A function whose value is built from stored bytes, and that can say how many without building it.
     */
    public interface StoredFunction extends Function<Object, Object> {
        /** The number of stored bytes {@link #apply} would build the value from, or -1 if not known. Does not build it. */
        long storedLength(Object value);
    }

    /**
     * The {@code _source} entry the script contexts share. The stored value is the document behind a supplier. Reading it
     * parses the document. Asking for the stored length loads the document but does not parse it.
     */
    public static final StoredFunction SOURCE_FUNCTION = new StoredFunction() {
        @Override
        @SuppressWarnings("unchecked")
        public Object apply(Object value) {
            return ((Supplier<Source>) value).get().source();
        }

        @Override
        public long storedLength(Object value) {
            if (value instanceof Supplier<?> supplier && supplier.get() instanceof Source source) {
                BytesReference bytes = source.internalSourceRef();
                return bytes == null ? -1 : bytes.length();
            }
            return -1;
        }
    };

    private final Map<String, Object> delegate;

    private final Map<String, Function<Object, Object>> functions;

    public DynamicMap(Map<String, Object> delegate, Map<String, Function<Object, Object>> functions) {
        this.delegate = delegate;
        this.functions = functions;
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
    public boolean containsKey(final Object key) {
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return delegate.containsValue(value);
    }

    @Override
    public Object get(final Object key) {
        Object value = delegate.get(key);
        Function<Object, Object> function = functions.get(key);
        if (function != null) {
            value = function.apply(value);
        }
        return value;
    }

    /**
     * The number of stored bytes {@link #get} would build the value under {@code key} from, or -1 when the map cannot say
     * without building it.
     */
    public long storedLength(String key) {
        return functions.get(key) instanceof StoredFunction stored ? stored.storedLength(delegate.get(key)) : -1;
    }

    @Override
    public Object put(final String key, final Object value) {
        return delegate.put(key, value);
    }

    @Override
    public Object remove(final Object key) {
        return delegate.remove(key);
    }

    @Override
    public void putAll(final Map<? extends String, ?> m) {
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
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        int count = 0;
        for (Map.Entry<String, Object> entry : entrySet()) {
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            if (++count < size()) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
