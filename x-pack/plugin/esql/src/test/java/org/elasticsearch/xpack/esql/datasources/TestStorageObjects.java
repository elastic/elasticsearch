/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * Package-private fixtures for decorator-style {@link StorageObject} tests. Per AGENTS.md
 * "real classes over mocks" — these provide deterministic, dependency-free delegate
 * implementations for tests that exercise the decorator's contract (delegation, lifecycle,
 * counter merging) without simulating I/O behaviour.
 */
final class TestStorageObjects {
    private TestStorageObjects() {}

    /**
     * Delegate fixture whose only meaningful method is {@link StorageObject#metrics}; every
     * other SPI call throws {@link UnsupportedOperationException} so an unexpected invocation
     * surfaces loudly rather than silently returning a default.
     */
    static StorageObject metricsOnly(StorageObjectMetrics snapshot) {
        return new StorageObject() {
            @Override
            public StorageObjectMetrics metrics() {
                return snapshot;
            }

            @Override
            public InputStream newStream() {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long length() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Instant lastModified() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean exists() {
                throw new UnsupportedOperationException();
            }

            @Override
            public StoragePath path() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int readBytes(long position, ByteBuffer target) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
