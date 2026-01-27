/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.storage;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Very small cache for fake datasets keyed by storage URI. Intended for tests only.
 */
public class FakeLanceDatasetRegistry {
    private static final Map<String, FakeLanceDataset> CACHE = new ConcurrentHashMap<>();

    public static FakeLanceDataset get(String uri, int dims, Supplier<FakeLanceDataset> loader) throws IOException {
        try {
            return CACHE.computeIfAbsent(uri, u -> {
                try {
                    return loader.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            // Unwrap nested exceptions to find IOException
            Throwable cause = e.getCause();
            while (cause != null) {
                if (cause instanceof IOException io) {
                    throw io;
                }
                cause = cause.getCause();
            }
            throw e;
        }
    }

    public static void clear() {
        CACHE.clear();
    }
}
