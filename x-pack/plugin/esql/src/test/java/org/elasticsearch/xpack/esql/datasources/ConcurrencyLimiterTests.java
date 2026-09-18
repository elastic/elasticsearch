/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrencyLimiterTests extends ESTestCase {

    public void testAcquireAndRelease() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(5, false));
        assertEquals(5, limiter.availablePermits());
        limiter.acquire();
        assertEquals(4, limiter.availablePermits());
        limiter.release();
        assertEquals(5, limiter.availablePermits());
    }

    public void testBlocksWhenExhausted() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false), 60_000L);
        limiter.acquire();
        assertEquals(0, limiter.availablePermits());

        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        Thread blocker = new Thread(() -> {
            started.countDown();
            try {
                limiter.acquire();
                acquired.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        blocker.start();
        started.await(5, TimeUnit.SECONDS);

        Thread.sleep(100);
        assertFalse(acquired.get());

        limiter.release();
        blocker.join(5000);
        assertTrue(acquired.get());
    }

    public void testTimeoutThrows() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false), 50L);
        limiter.acquire();

        expectThrows(TimeoutException.class, limiter::acquire);

        limiter.release();
    }

    public void testTimeoutMessageWhenSettingCannotRaiseLimit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("gs", new ExternalSourceSettings.BlobStoreConcurrency(1, false), 50L);
        limiter.acquire();
        TimeoutException thrown = expectThrows(TimeoutException.class, limiter::acquire);
        assertEquals(
            "Timed out waiting for a concurrency permit for [gs] after [50]ms (max permits [1]). "
                + "[esql.external.max_concurrent_requests] cannot raise this node's limit.",
            thrown.getMessage()
        );
        limiter.release();
    }

    public void testTimeoutMessageWhenSettingCanRaiseLimit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, true), 50L);
        limiter.acquire();
        TimeoutException thrown = expectThrows(TimeoutException.class, limiter::acquire);
        assertEquals(
            "Timed out waiting for a concurrency permit for [s3] after [50]ms (max permits [1]). "
                + "Raise [esql.external.max_concurrent_requests] in the node's configuration and restart the node "
                + "to increase the limit.",
            thrown.getMessage()
        );
        limiter.release();
    }

    public void testDisabledWithZeroPermits() throws Exception {
        ConcurrencyLimiter limiter = ConcurrencyLimiter.UNLIMITED;
        assertFalse(limiter.isEnabled());
        assertEquals(Integer.MAX_VALUE, limiter.availablePermits());
        limiter.acquire();
        limiter.release();
    }

    public void testUnlimitedSingleton() throws Exception {
        assertFalse(ConcurrencyLimiter.UNLIMITED.isEnabled());
        assertEquals(0, ConcurrencyLimiter.UNLIMITED.maxPermits());
        ConcurrencyLimiter.UNLIMITED.acquire();
        ConcurrencyLimiter.UNLIMITED.release();
    }

    public void testMaxPermits() {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(42, false));
        assertTrue(limiter.isEnabled());
        assertEquals(42, limiter.maxPermits());
    }
}
