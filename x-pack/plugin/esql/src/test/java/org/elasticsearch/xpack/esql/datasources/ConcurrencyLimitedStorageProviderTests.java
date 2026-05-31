/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConcurrencyLimitedStorageProviderTests extends ESTestCase {

    public void testListObjectsAcquiresPermit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(5);
        StorageProvider delegate = mock(StorageProvider.class);
        StorageIterator emptyIterator = new StorageIterator() {
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
        when(delegate.listObjects(any(), anyBoolean())).thenReturn(emptyIterator);

        ConcurrencyLimitedStorageProvider provider = new ConcurrencyLimitedStorageProvider(delegate, limiter);
        assertEquals(5, limiter.availablePermits());

        try (StorageIterator iter = provider.listObjects(StoragePath.of("s3://bucket/prefix"), true)) {
            assertEquals(4, limiter.availablePermits());
        }
        assertEquals(5, limiter.availablePermits());
    }

    public void testExistsAcquiresAndReleasesPermit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageProvider delegate = mock(StorageProvider.class);
        when(delegate.exists(any())).thenReturn(true);

        ConcurrencyLimitedStorageProvider provider = new ConcurrencyLimitedStorageProvider(delegate, limiter);
        assertEquals(3, limiter.availablePermits());

        boolean result = provider.exists(StoragePath.of("s3://bucket/key"));
        assertTrue(result);
        assertEquals(3, limiter.availablePermits());
    }

    public void testExistsReleasesPermitOnException() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageProvider delegate = mock(StorageProvider.class);
        when(delegate.exists(any())).thenThrow(new IOException("network error"));

        ConcurrencyLimitedStorageProvider provider = new ConcurrencyLimitedStorageProvider(delegate, limiter);
        expectThrows(IOException.class, () -> provider.exists(StoragePath.of("s3://bucket/key")));
        assertEquals(3, limiter.availablePermits());
    }

    public void testNewObjectReturnsConcurrencyLimitedObject() {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(5);
        StorageProvider delegate = mock(StorageProvider.class);
        StorageObject mockObj = mock(StorageObject.class);
        when(delegate.newObject(any(StoragePath.class))).thenReturn(mockObj);

        ConcurrencyLimitedStorageProvider provider = new ConcurrencyLimitedStorageProvider(delegate, limiter);
        StorageObject obj = provider.newObject(StoragePath.of("s3://bucket/key"));
        assertNotNull(obj);
        assertTrue(obj instanceof ConcurrencyLimitedStorageObject);
    }
}
