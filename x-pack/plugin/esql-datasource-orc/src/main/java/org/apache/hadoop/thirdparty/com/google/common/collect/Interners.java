/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.apache.hadoop.thirdparty.com.google.common.collect;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Minimal stub replacing the {@code hadoop-shaded-guava} jar (3.5 MB).
 * <p>
 * The original class lives in Google Guava ({@code com.google.common.collect.Interners},
 * Apache License 2.0) and is shaded by the Hadoop project into the
 * {@code org.apache.hadoop.thirdparty} package namespace. The static initializer of
 * {@code org.apache.hadoop.util.StringInterner} (in {@code hadoop-common}) calls
 * {@code Interners.newStrongInterner()} to create the singleton interner used during
 * Hadoop {@code Configuration} XML parsing.
 * <p>
 * Guava's implementation uses its internal {@code MapMakerInternalMap}; this stub
 * uses {@link ConcurrentHashMap} instead -- a completely independent implementation
 * with identical semantics. No code was copied from Guava.
 */
public final class Interners {

    private Interners() {}

    @SuppressWarnings("unchecked")
    public static <E> Interner<E> newStrongInterner() {
        return new StrongInterner<>();
    }

    private static final class StrongInterner<E> implements Interner<E> {
        private final ConcurrentHashMap<E, E> map = new ConcurrentHashMap<>();

        @Override
        public E intern(E sample) {
            E existing = map.putIfAbsent(sample, sample);
            return existing != null ? existing : sample;
        }
    }
}
