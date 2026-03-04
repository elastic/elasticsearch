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
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class RetryPolicyTests extends ESTestCase {

    public void testNoRetryPolicyNeverRetries() {
        RetryPolicy policy = RetryPolicy.NONE;
        assertFalse(policy.isRetryable(new SocketTimeoutException("timeout")));
        assertEquals(0, policy.maxRetries());
    }

    public void testSocketTimeoutIsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertTrue(policy.isRetryable(new SocketTimeoutException("Read timed out")));
    }

    public void testConnectExceptionIsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertTrue(policy.isRetryable(new ConnectException("Connection refused")));
    }

    public void testConnectionResetIsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertTrue(policy.isRetryable(new SocketException("Connection reset")));
    }

    public void testSocketExceptionWithoutResetIsNotRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertFalse(policy.isRetryable(new SocketException("Broken pipe")));
    }

    public void testHttp429IsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertTrue(policy.isRetryable(new IOException("Status code: 429")));
        assertTrue(policy.isRetryable(new IOException("Too Many Requests")));
    }

    public void testHttp503IsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertTrue(policy.isRetryable(new IOException("Status code: 503")));
        assertTrue(policy.isRetryable(new IOException("Service Unavailable")));
    }

    public void testSlowDownIsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertTrue(policy.isRetryable(new IOException("SlowDown")));
        assertTrue(policy.isRetryable(new IOException("Reduce your request rate")));
    }

    public void testNonTransientErrorIsNotRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertFalse(policy.isRetryable(new IOException("Access Denied")));
        assertFalse(policy.isRetryable(new IOException("NoSuchKey")));
        assertFalse(policy.isRetryable(new SecurityException("forbidden")));
    }

    public void testNullMessageIsNotRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertFalse(policy.isRetryable(new IOException((String) null)));
    }

    public void testWrappedTransientErrorIsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertTrue(policy.isRetryable(new RuntimeException("wrapper", new SocketTimeoutException("timeout"))));
        assertTrue(policy.isRetryable(new IOException("outer", new IOException("503 Service Unavailable"))));
    }

    public void testWrappedNonTransientErrorIsNotRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertFalse(policy.isRetryable(new RuntimeException("wrapper", new IOException("Access Denied"))));
    }

    public void testDelayGrowsExponentially() {
        RetryPolicy policy = new RetryPolicy(5, 100, 10000);
        long d0 = policy.delayMillis(0);
        long d1 = policy.delayMillis(1);
        long d2 = policy.delayMillis(2);
        assertTrue("delay should grow: d0=" + d0 + " d1=" + d1, d1 > d0 / 2);
        assertTrue("delay should grow: d1=" + d1 + " d2=" + d2, d2 > d1 / 2);
    }

    public void testDelayIsCappedAtMax() {
        RetryPolicy policy = new RetryPolicy(10, 100, 500);
        for (int i = 0; i < 10; i++) {
            long delay = policy.delayMillis(i);
            assertTrue("delay " + delay + " exceeds max+jitter", delay <= 500 + 500 / 4 + 1);
        }
    }

    public void testExecuteSucceedsOnFirstAttempt() throws IOException {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/key");

        String result = policy.execute(() -> {
            calls.incrementAndGet();
            return "ok";
        }, "test", path);

        assertEquals("ok", result);
        assertEquals(1, calls.get());
    }

    public void testExecuteRetriesOnTransientFailure() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/key");

        String result = policy.execute(() -> {
            if (calls.incrementAndGet() < 3) {
                throw new SocketTimeoutException("timeout");
            }
            return "ok";
        }, "test", path);

        assertEquals("ok", result);
        assertEquals(3, calls.get());
    }

    public void testExecuteThrowsAfterMaxRetries() {
        RetryPolicy policy = new RetryPolicy(2, 1, 10);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/key");

        IOException ex = expectThrows(IOException.class, () -> policy.execute(() -> {
            calls.incrementAndGet();
            throw new SocketTimeoutException("timeout");
        }, "test", path));

        assertEquals("timeout", ex.getMessage());
        assertEquals(3, calls.get());
    }

    public void testExecuteDoesNotRetryNonTransientError() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/key");

        IOException ex = expectThrows(IOException.class, () -> policy.execute(() -> {
            calls.incrementAndGet();
            throw new IOException("Access Denied");
        }, "test", path));

        assertEquals("Access Denied", ex.getMessage());
        assertEquals(1, calls.get());
    }
}
