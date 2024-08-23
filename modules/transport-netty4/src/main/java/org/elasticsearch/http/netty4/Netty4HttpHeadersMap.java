/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.HttpHeaders;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A wrapper of {@link HttpHeaders} that implements a map to prevent copying unnecessarily. This class does not support modifications and
 * due to the underlying implementation, it performs case insensitive lookups of key to values.
 * <p>
 * It is important to note that this implementation does have some downsides in that each invocation of the {@link #values()} and
 * {@link #entrySet()} methods will perform a copy of the values in the HttpHeaders rather than returning a view of the underlying values.
 */
class Netty4HttpHeadersMap implements Map<String, List<String>> {

    private final HttpHeaders httpHeaders;

    Netty4HttpHeadersMap(HttpHeaders httpHeaders) {
        this.httpHeaders = httpHeaders;
    }

    @Override
    public int size() {
        return httpHeaders.size();
    }

    @Override
    public boolean isEmpty() {
        return httpHeaders.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return key instanceof String && httpHeaders.contains((String) key);
    }

    @Override
    public boolean containsValue(Object value) {
        return value instanceof List && httpHeaders.names().stream().map(httpHeaders::getAll).anyMatch(value::equals);
    }

    @Override
    public List<String> get(Object key) {
        return key instanceof String ? httpHeaders.getAll((String) key) : null;
    }

    @Override
    public List<String> put(String key, List<String> value) {
        throw new UnsupportedOperationException("modifications are not supported");
    }

    @Override
    public List<String> remove(Object key) {
        throw new UnsupportedOperationException("modifications are not supported");
    }

    @Override
    public void putAll(Map<? extends String, ? extends List<String>> m) {
        throw new UnsupportedOperationException("modifications are not supported");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("modifications are not supported");
    }

    @Override
    public Set<String> keySet() {
        return httpHeaders.names();
    }

    @Override
    public Collection<List<String>> values() {
        return httpHeaders.names().stream().map(k -> Collections.unmodifiableList(httpHeaders.getAll(k))).toList();
    }

    @Override
    public Set<Entry<String, List<String>>> entrySet() {
        return httpHeaders.names()
                          .stream()
                          .map(k -> new AbstractMap.SimpleImmutableEntry<>(k, httpHeaders.getAll(k)))
                          .collect(Collectors.toSet());
    }
}
