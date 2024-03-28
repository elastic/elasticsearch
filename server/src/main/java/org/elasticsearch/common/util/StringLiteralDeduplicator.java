/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Map;

/**
 * A cache in front of Java's string interning. This method assumes that it is only called with strings that are already part of the
 * JVM's string pool so that interning them does not grow the pool. Calling it with strings not in the interned string pool is not
 * advisable as its performance may deteriorate to slower than outright calls to {@link String#intern()}.
 */
public final class StringLiteralDeduplicator {

    private static final int DEFAULT_MAX_SIZE = 1000;

    private final Map<String, String> map = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private final int maxSize;

    public StringLiteralDeduplicator() {
        this(DEFAULT_MAX_SIZE);
    }

    public StringLiteralDeduplicator(int maxSize) {
        this.maxSize = maxSize;
    }

    public String deduplicate(String string) {
        final String res = map.get(string);
        if (res != null) {
            return res;
        }
        final String interned = string.intern();
        if (map.size() > maxSize) {
            map.clear();
        }
        map.put(interned, interned);
        return interned;
    }
}
