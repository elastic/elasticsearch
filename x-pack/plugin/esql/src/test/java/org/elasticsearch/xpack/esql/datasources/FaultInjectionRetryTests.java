/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests that the retry policy correctly handles transient fault patterns
 * matching what {@code FaultInjectingS3HttpHandler} produces: a configurable
 * number of failures followed by success.
 */
public class FaultInjectionRetryTests extends ESTestCase {

    public void testTransientFaultsRecoverWithinRetryBudget() throws IOException {
        int faultCount = between(1, 2);
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/data.parquet");

        String result = policy.execute(() -> {
            if (calls.incrementAndGet() <= faultCount) {
                throw new IOException("503 Service Unavailable");
            }
            return "data";
        }, "GET_OBJECT", path);

        assertEquals("data", result);
        assertEquals(faultCount + 1, calls.get());
    }

    public void testPersistentFaultsExhaustRetryBudget() {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/data.parquet");

        IOException ex = expectThrows(IOException.class, () -> policy.execute(() -> {
            calls.incrementAndGet();
            throw new IOException("503 Service Unavailable");
        }, "GET_OBJECT", path));

        assertTrue(ex.getMessage().contains("503"));
        assertEquals(4, calls.get());
    }

    public void testConnectionResetIsRetried() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/data.parquet");

        String result = policy.execute(() -> {
            if (calls.incrementAndGet() == 1) {
                throw new SocketException("Connection reset");
            }
            return "recovered";
        }, "GET_OBJECT", path);

        assertEquals("recovered", result);
        assertEquals(2, calls.get());
    }

    public void testNonRetryableFaultFailsImmediately() {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/secret.parquet");

        IOException ex = expectThrows(IOException.class, () -> policy.execute(() -> {
            calls.incrementAndGet();
            throw new IOException("403 Access Denied");
        }, "GET_OBJECT", path));

        assertTrue(ex.getMessage().contains("403"));
        assertEquals(1, calls.get());
    }

    public void testTimeoutIsRetried() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/data.parquet");

        String result = policy.execute(() -> {
            if (calls.incrementAndGet() <= 2) {
                throw new SocketTimeoutException("Read timed out");
            }
            return "ok";
        }, "GET_OBJECT", path);

        assertEquals("ok", result);
        assertEquals(3, calls.get());
    }

    public void testCountdownFaultPattern() throws IOException {
        int totalFaults = between(1, 3);
        RetryPolicy policy = new RetryPolicy(totalFaults + 1, 1, 10);
        AtomicInteger faultCounter = new AtomicInteger(totalFaults);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/data.parquet");

        String result = policy.execute(() -> {
            calls.incrementAndGet();
            if (faultCounter.decrementAndGet() >= 0) {
                throw new IOException("503 Service Unavailable");
            }
            return "success";
        }, "GET_OBJECT", path);

        assertEquals("success", result);
        assertEquals(totalFaults + 1, calls.get());
    }

    public void testSuccessfulCallReturnsImmediately() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/metadata.json");

        String result = policy.execute(() -> {
            calls.incrementAndGet();
            return "metadata";
        }, "GET_OBJECT", path);

        assertEquals("metadata", result);
        assertEquals(1, calls.get());
    }

    public void testRetryPolicyWithZeroRetriesNeverRetries() {
        RetryPolicy policy = RetryPolicy.NONE;
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/data.parquet");

        IOException ex = expectThrows(IOException.class, () -> policy.execute(() -> {
            calls.incrementAndGet();
            throw new IOException("503 Service Unavailable");
        }, "GET_OBJECT", path));

        assertTrue(ex.getMessage().contains("503"));
        assertEquals(1, calls.get());
    }
}
