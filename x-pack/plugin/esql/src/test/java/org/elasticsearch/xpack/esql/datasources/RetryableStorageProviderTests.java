/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RetryableStorageProviderTests extends ESTestCase {

    public void testNewObjectWrapsWithRetry() throws IOException {
        AtomicInteger streamCalls = new AtomicInteger();
        StorageObject inner = stubObject("s3://bucket/file.csv", () -> {
            if (streamCalls.incrementAndGet() < 2) {
                throw new SocketTimeoutException("timeout");
            }
            return new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8));
        });
        StorageProvider delegate = stubProvider(inner);
        RetryableStorageProvider provider = new RetryableStorageProvider(delegate, new RetryPolicy(3, 1, 10));

        StorageObject obj = provider.newObject(StoragePath.of("s3://bucket/file.csv"));
        InputStream stream = obj.newStream();
        assertEquals("data", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
        assertEquals(2, streamCalls.get());
    }

    public void testNewObjectWithLengthWrapsWithRetry() throws IOException {
        AtomicInteger streamCalls = new AtomicInteger();
        StorageObject inner = stubObject("s3://bucket/file.csv", () -> {
            if (streamCalls.incrementAndGet() < 2) {
                throw new SocketTimeoutException("timeout");
            }
            return new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8));
        });
        StorageProvider delegate = stubProvider(inner);
        RetryableStorageProvider provider = new RetryableStorageProvider(delegate, new RetryPolicy(3, 1, 10));

        StorageObject obj = provider.newObject(StoragePath.of("s3://bucket/file.csv"), 100);
        InputStream stream = obj.newStream();
        assertEquals("data", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    }

    public void testListObjectsRetriesOnTransientFailure() throws IOException {
        AtomicInteger calls = new AtomicInteger();
        StorageProvider delegate = new StubStorageProvider() {
            @Override
            public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
                if (calls.incrementAndGet() < 2) {
                    throw new SocketTimeoutException("timeout");
                }
                return new StorageIterator() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public StorageEntry next() {
                        return null;
                    }

                    @Override
                    public void close() {}
                };
            }
        };
        RetryableStorageProvider provider = new RetryableStorageProvider(delegate, new RetryPolicy(3, 1, 10));

        StorageIterator iter = provider.listObjects(StoragePath.of("s3://bucket/prefix"), true);
        assertFalse(iter.hasNext());
        assertEquals(2, calls.get());
    }

    public void testExistsRetriesOnTransientFailure() throws IOException {
        AtomicInteger calls = new AtomicInteger();
        StorageProvider delegate = new StubStorageProvider() {
            @Override
            public boolean exists(StoragePath path) throws IOException {
                if (calls.incrementAndGet() < 2) {
                    throw new SocketTimeoutException("timeout");
                }
                return true;
            }
        };
        RetryableStorageProvider provider = new RetryableStorageProvider(delegate, new RetryPolicy(3, 1, 10));

        assertTrue(provider.exists(StoragePath.of("s3://bucket/file.csv")));
        assertEquals(2, calls.get());
    }

    public void testSupportedSchemesDelegates() {
        StorageProvider delegate = new StubStorageProvider() {
            @Override
            public List<String> supportedSchemes() {
                return List.of("s3", "gs");
            }
        };
        RetryableStorageProvider provider = new RetryableStorageProvider(delegate, RetryPolicy.DEFAULT);
        assertEquals(List.of("s3", "gs"), provider.supportedSchemes());
    }

    public void testReadBytesAsyncRetriesOnTransientFailure() throws Exception {
        AtomicInteger asyncCalls = new AtomicInteger();
        StorageObject inner = new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public long length() {
                return 4;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("s3://bucket/file.csv");
            }

            @Override
            public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
                if (asyncCalls.incrementAndGet() < 3) {
                    listener.onFailure(new SocketTimeoutException("timeout"));
                } else {
                    listener.onResponse(ByteBuffer.wrap("async-data".getBytes(StandardCharsets.UTF_8)));
                }
            }
        };
        StorageProvider delegate = stubProvider(inner);
        RetryableStorageProvider provider = new RetryableStorageProvider(delegate, new RetryPolicy(3, 1, 10));

        StorageObject obj = provider.newObject(StoragePath.of("s3://bucket/file.csv"));
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            PlainActionFuture<ByteBuffer> future = new PlainActionFuture<>();
            obj.readBytesAsync(0, 10, exec, future);
            ByteBuffer result = future.actionGet();
            byte[] bytes = new byte[result.remaining()];
            result.get(bytes);
            assertEquals("async-data", new String(bytes, StandardCharsets.UTF_8));
            assertEquals(3, asyncCalls.get());
        } finally {
            exec.shutdown();
            exec.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testCloseClosesDelegate() throws IOException {
        AtomicInteger closeCalls = new AtomicInteger();
        StorageProvider delegate = new StubStorageProvider() {
            @Override
            public void close() {
                closeCalls.incrementAndGet();
            }
        };
        RetryableStorageProvider provider = new RetryableStorageProvider(delegate, RetryPolicy.DEFAULT);
        provider.close();
        assertEquals(1, closeCalls.get());
    }

    private static StorageObject stubObject(String location, IOStreamSupplier streamSupplier) {
        StoragePath path = StoragePath.of(location);
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return streamSupplier.get();
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                return streamSupplier.get();
            }

            @Override
            public long length() {
                return 100;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return path;
            }
        };
    }

    private static StorageProvider stubProvider(StorageObject object) {
        return new StubStorageProvider() {
            @Override
            public StorageObject newObject(StoragePath path) {
                return object;
            }

            @Override
            public StorageObject newObject(StoragePath path, long length) {
                return object;
            }

            @Override
            public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
                return object;
            }
        };
    }

    @FunctionalInterface
    interface IOStreamSupplier {
        InputStream get() throws IOException;
    }

    private abstract static class StubStorageProvider implements StorageProvider {
        @Override
        public StorageObject newObject(StoragePath path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(StoragePath path) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of();
        }

        @Override
        public void close() {}
    }
}
