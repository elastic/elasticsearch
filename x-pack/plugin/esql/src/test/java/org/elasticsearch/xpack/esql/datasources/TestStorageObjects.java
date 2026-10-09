/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;

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
            public StorageIdentity storageIdentity() {
                return AbstractTestStorageObject.NOOP;
            }

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

    /**
     * Native-async leaf: {@code startReadBytesAsync} queues the GET on a helper thread and
     * returns. Used to stack QBSO over CLSO the way S3/HTTP behave.
     */
    static StorageObject nativeAsync(byte[] data) {
        return new AbstractTestStorageObject() {
            @Override
            public Releasable startReadBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                Thread runner = new Thread(
                    () -> { listener.onResponse(new DirectReadBuffer(ByteBuffer.wrap(data), () -> {})); },
                    "native-async-get"
                );
                runner.setDaemon(true);
                runner.start();
                return () -> {};
            }

            @Override
            public boolean supportsNativeAsync() {
                return true;
            }

            @Override
            public boolean readBytesAsyncReleasesExecutor() {
                return true;
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
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.EPOCH;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("s3://bucket/key");
            }

            @Override
            public int readBytes(long position, ByteBuffer target) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
