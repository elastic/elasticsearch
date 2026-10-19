/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.script.DynamicMap;
import org.elasticsearch.search.lookup.Source;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The {@code params} map of a script with allocation tracking on. A read of {@code _source} makes the server load the stored
 * document and parse it into maps and lists, and no allowlist method sees that. This wrapper charges it in two steps. Before
 * the parse it charges the loaded bytes and the least the parse can cost, so a document too big for the budget stops the
 * script before the parse runs. After the parse it walks the tree and charges whatever the floor missed. The server keeps one
 * document per doc id, so later reads in the same execution get the same objects back and are not charged again. Every other
 * key goes straight through.
 */
public final class ChargedParams implements Map<String, Object> {

    private static final String SOURCE = "_source";

    private final PainlessScript script;
    private final Map<String, Object> delegate;
    /** The loaded document this execution has already paid the floor for, and that floor. */
    private Object loaded;
    private long floor;
    /** The parsed source map this execution has already paid for. */
    private Object charged;

    public ChargedParams(PainlessScript script, Map<String, Object> delegate) {
        this.script = script;
        this.delegate = delegate;
    }

    @Override
    public Object get(Object key) {
        if (SOURCE.equals(key)) {
            chargeFloor();
        }
        Object value = delegate.get(key);
        if (value != charged && value instanceof Map<?, ?> source && SOURCE.equals(key)) {
            charged = value;
            script.$checkAllocBytes(Math.max(0L, AllocationEstimators.parsedSourceBytes(source) - floor));
            floor = 0L;
        }
        return value;
    }

    /**
     * Charges the loaded bytes and the parse floor, once per document. The server's params map keeps the document behind a
     * supplier and parses it in {@code get}, so this looks at the raw value first. Stored and synthetic source both hand over
     * bytes, so asking for the length does not parse. A plain map has no raw value and pays only after the parse.
     */
    private void chargeFloor() {
        if (delegate instanceof DynamicMap dynamic && dynamic.getRaw(SOURCE) instanceof Supplier<?> supplier) {
            Object document = supplier.get();
            if (document != loaded && document instanceof Source source) {
                loaded = document;
                BytesReference bytes = source.internalSourceRef();
                long length = bytes == null ? 0L : bytes.length();
                floor = AllocationEstimators.parseFloorBytes(length);
                script.$checkAllocBytes(AllocSizes.addSat(length, floor));
            }
        }
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
