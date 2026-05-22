/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryBudgetedStorageObjectTests extends ESTestCase {

    public void testStreamCloseReleasesBudget() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(3, 60_000L, null);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.newStream()).thenReturn(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
        when(delegate.path()).thenReturn(StoragePath.of("s3://bucket/key"));

        QueryBudgetedStorageObject obj = new QueryBudgetedStorageObject(delegate, budget);
        assertEquals(0, budget.inFlight());

        InputStream stream = obj.newStream();
        assertEquals(1, budget.inFlight());

        stream.close();
        assertEquals(0, budget.inFlight());
    }

    public void testStreamDoubleCloseIsSafe() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(3, 60_000L, null);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.newStream()).thenReturn(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));

        QueryBudgetedStorageObject obj = new QueryBudgetedStorageObject(delegate, budget);
        InputStream stream = obj.newStream();
        stream.close();
        stream.close();
        assertEquals(0, budget.inFlight());
    }

    public void testReadBytesReleasesOnException() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(3, 60_000L, null);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.readBytes(anyLong(), any(ByteBuffer.class))).thenThrow(new IOException("read error"));

        QueryBudgetedStorageObject obj = new QueryBudgetedStorageObject(delegate, budget);
        expectThrows(IOException.class, () -> obj.readBytes(0, ByteBuffer.allocate(10)));
        assertEquals(0, budget.inFlight());
    }

    public void testLengthReleasesBudget() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(3, 60_000L, null);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.length()).thenReturn(42L);

        QueryBudgetedStorageObject obj = new QueryBudgetedStorageObject(delegate, budget);
        assertEquals(42L, obj.length());
        assertEquals(0, budget.inFlight());
    }

    @SuppressWarnings("unchecked")
    public void testAsyncReadReleasesBudgetOnSuccess() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(3, 60_000L, null);
        StorageObject delegate = mock(StorageObject.class);
        ByteBuffer result = ByteBuffer.wrap("data".getBytes(StandardCharsets.UTF_8));
        doAnswer(inv -> {
            ActionListener<ByteBuffer> listener = inv.getArgument(3);
            listener.onResponse(result);
            return null;
        }).when(delegate).readBytesAsync(anyLong(), anyLong(), any(), any(ActionListener.class));

        QueryBudgetedStorageObject obj = new QueryBudgetedStorageObject(delegate, budget);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ByteBuffer> response = new AtomicReference<>();

        obj.readBytesAsync(0, 4, Runnable::run, ActionListener.wrap(r -> {
            response.set(r);
            latch.countDown();
        }, e -> latch.countDown()));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertSame(result, response.get());
        assertEquals(0, budget.inFlight());
    }

    @SuppressWarnings("unchecked")
    public void testAsyncReadReleasesBudgetOnFailure() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(3, 60_000L, null);
        StorageObject delegate = mock(StorageObject.class);
        doAnswer(inv -> {
            ActionListener<ByteBuffer> listener = inv.getArgument(3);
            listener.onFailure(new IOException("async error"));
            return null;
        }).when(delegate).readBytesAsync(anyLong(), anyLong(), any(), any(ActionListener.class));

        QueryBudgetedStorageObject obj = new QueryBudgetedStorageObject(delegate, budget);
        CountDownLatch latch = new CountDownLatch(1);

        obj.readBytesAsync(0, 4, Runnable::run, ActionListener.wrap(r -> latch.countDown(), e -> latch.countDown()));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(0, budget.inFlight());
    }

    public void testNewStreamReleasesOnDelegateException() throws Exception {
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(3, 60_000L, null);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.newStream()).thenThrow(new IOException("connection refused"));

        QueryBudgetedStorageObject obj = new QueryBudgetedStorageObject(delegate, budget);
        expectThrows(IOException.class, obj::newStream);
        assertEquals(0, budget.inFlight());
    }
}
