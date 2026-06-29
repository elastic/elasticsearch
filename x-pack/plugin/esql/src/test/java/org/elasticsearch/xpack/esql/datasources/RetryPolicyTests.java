/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

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

    public void testConnectExceptionWithDnsFailureIsNotRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        ConnectException ce = new ConnectException("Connection refused");
        ce.initCause(new UnknownHostException("no-such-bucket.s3.amazonaws.com"));
        assertFalse(policy.isRetryable(ce));
    }

    public void testConnectExceptionWithNestedDnsFailureIsNotRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        ConnectException ce = new ConnectException("Connection refused");
        IOException wrapper = new IOException("resolve failed");
        wrapper.initCause(new UnknownHostException("no-such-host.example.com"));
        ce.initCause(wrapper);
        assertFalse(policy.isRetryable(ce));
    }

    public void testConnectionResetIsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertTrue(policy.isRetryable(new SocketException("Connection reset")));
    }

    public void testAnySocketExceptionIsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        // Connection reset and broken pipe are both transport faults; on a read either can be re-opened and
        // resumed, so both are retryable by type — no message inspection.
        assertTrue(policy.isRetryable(new SocketException("Connection reset")));
        assertTrue(policy.isRetryable(new SocketException("Broken pipe")));
    }

    public void testTransientStorageMarkerIsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        // Providers classify transient transport faults and retryable server responses (500 / 503 / 429) by
        // type and status code, then raise this typed marker; the retry layer reacts to the type, not text.
        assertTrue(policy.isRetryable(new ExternalUnavailableException("transient transport fault", (Throwable) null)));
        assertTrue(policy.isRetryable(new ExternalUnavailableException(true, "throttled")));
    }

    public void testWrappedTransientMarkerIsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertTrue(policy.isRetryable(new RuntimeException("wrapper", new ExternalUnavailableException("transient", (Throwable) null))));
    }

    public void testNonTransientErrorIsNotRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        // A bare throwable with no transport type and no typed marker is a real error regardless of message —
        // a 503 that was not classified into the typed marker by a provider is not retried here.
        assertFalse(policy.isRetryable(new IOException("Access Denied")));
        assertFalse(policy.isRetryable(new IOException("NoSuchKey")));
        assertFalse(policy.isRetryable(new IOException("Service Unavailable")));
        assertFalse(policy.isRetryable(new SecurityException("forbidden")));
    }

    public void testNullMessageIsNotRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertFalse(policy.isRetryable(new IOException((String) null)));
    }

    public void testWrappedTransientTransportErrorIsRetryable() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertTrue(policy.isRetryable(new RuntimeException("wrapper", new SocketTimeoutException("timeout"))));
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

    public void testExecuteDoesNotRetryDnsFailure() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/key");

        ConnectException ce = new ConnectException("Connection refused");
        ce.initCause(new UnknownHostException("no-such-host.example.com"));

        IOException ex = expectThrows(IOException.class, () -> policy.execute(() -> {
            calls.incrementAndGet();
            throw ce;
        }, "test", path));

        assertEquals("Connection refused", ex.getMessage());
        assertEquals(1, calls.get());
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

    // --- Total duration budget tests ---

    public void testWithTotalDurationBudgetPreservesRetryParameters() {
        RetryPolicy base = new RetryPolicy(5, 100, 2000);
        RetryPolicy budgeted = base.withTotalDurationBudget(10_000);

        assertEquals(5, budgeted.maxRetries());
        assertEquals(10_000, budgeted.maxTotalDurationMs());
    }

    public void testDefaultPolicyHasNoBudget() {
        assertEquals(RetryPolicy.NO_BUDGET, RetryPolicy.DEFAULT.maxTotalDurationMs());
    }

    public void testNonePolicyHasNoBudget() {
        assertEquals(RetryPolicy.NO_BUDGET, RetryPolicy.NONE.maxTotalDurationMs());
    }

    public void testExecuteAbortsWhenBudgetExceeded() {
        RetryPolicy policy = new RetryPolicy(10, 500, 5000, 100);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/key");

        IOException ex = expectThrows(IOException.class, () -> policy.execute(() -> {
            calls.incrementAndGet();
            throw new SocketTimeoutException("timeout");
        }, "test", path));

        assertEquals("timeout", ex.getMessage());
        assertTrue("should abort on first failure when delay exceeds budget, got " + calls.get(), calls.get() <= 2);
    }

    public void testExecuteSucceedsWithinBudget() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10, 60_000);
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

    // --- Throttle-specific retry budget tests ---

    public void testThrottlingErrorGetsHigherRetryBudget() throws IOException {
        RetryPolicy policy = new RetryPolicy(2, 1, 10, 8, 1, 10, RetryPolicy.NO_BUDGET, null);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/key");

        String result = policy.execute(() -> {
            if (calls.incrementAndGet() <= 6) {
                throw new ExternalUnavailableException(true, "throttled");
            }
            return "ok";
        }, "test", path);

        assertEquals("ok", result);
        assertEquals(7, calls.get());
    }

    public void testNonThrottleErrorUsesStandardBudget() {
        RetryPolicy policy = new RetryPolicy(2, 1, 10, 8, 1, 10, RetryPolicy.NO_BUDGET, null);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/key");

        IOException ex = expectThrows(IOException.class, () -> policy.execute(() -> {
            calls.incrementAndGet();
            throw new SocketTimeoutException("timeout");
        }, "test", path));

        assertEquals("timeout", ex.getMessage());
        assertEquals(3, calls.get());
    }

    public void testThrottlingDelayIsLongerThanStandardDelay() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        long standardDelay = policy.delayMillis(0, false);
        long throttleDelay = policy.delayMillis(0, true);
        assertTrue(
            "throttle delay [" + throttleDelay + "] should be >= standard delay [" + standardDelay + "]",
            throttleDelay >= standardDelay
        );
    }

    public void testIsThrottlingErrorClassification() {
        // Throttling is decided by the provider (from the HTTP status) and flagged on the typed marker; it is
        // no longer inferred from message text. The throttling marker is recognized through the cause chain.
        assertTrue(RetryPolicy.isThrottlingError(new ExternalUnavailableException(true, "throttled")));
        assertTrue(RetryPolicy.isThrottlingError(new RuntimeException("wrapper", new ExternalUnavailableException(true, "throttled"))));

        // A plain transient marker is retryable but not throttling.
        assertFalse(RetryPolicy.isThrottlingError(new ExternalUnavailableException(false, "transient transport")));
        assertFalse(RetryPolicy.isThrottlingError(new SocketTimeoutException("timeout")));
        assertFalse(RetryPolicy.isThrottlingError(new ConnectException("refused")));
        assertFalse(RetryPolicy.isThrottlingError(new IOException("Service Unavailable")));
        assertFalse(RetryPolicy.isThrottlingError(new IOException((String) null)));
    }

    public void testAdaptiveBackoffScalesDelay() {
        AtomicLong clock = new AtomicLong(0);
        AdaptiveBackoff backoff = new AdaptiveBackoff(AdaptiveBackoff.MAX_MULTIPLIER, clock::get);
        backoff.onThrottled();
        backoff.onThrottled();

        RetryPolicy policy = RetryPolicy.DEFAULT.withAdaptiveBackoff(backoff);
        long normalDelay = RetryPolicy.DEFAULT.delayMillis(0, true);
        long adaptiveDelay = policy.delayMillis(0, true);
        assertTrue("adaptive delay [" + adaptiveDelay + "] should be > normal delay [" + normalDelay + "]", adaptiveDelay > normalDelay);
    }

    public void testDecideDoesNotRampAdaptiveBackoffWhenGivingUp() {
        // The onThrottled() feed sits AFTER the give-up + time-budget checks in decide(), so abandoning a
        // throttle must not ramp the cross-request multiplier. Pins that ordering.
        AtomicLong clock = new AtomicLong(0);
        AdaptiveBackoff backoff = new AdaptiveBackoff(AdaptiveBackoff.MAX_MULTIPLIER, clock::get);
        RetryPolicy policy = RetryPolicy.DEFAULT.withAdaptiveBackoff(backoff);
        ExternalUnavailableException throttle = new ExternalUnavailableException(true, (Throwable) null, "throttled (HTTP 503)");

        // Budget exhausted (attempt == throttleMaxRetries) -> GIVE_UP, and the backoff must stay at baseline.
        RetryPolicy.RetryDecision giveUp = policy.decide(throttle, policy.throttleMaxRetries(), System.nanoTime());
        assertFalse("an exhausted-budget throttle must give up", giveUp.retry());
        assertEquals("giving up must not ramp the adaptive backoff", 1, backoff.currentMultiplier());

        // Positive control: a within-budget throttle commits to a retry and DOES ramp the backoff.
        RetryPolicy.RetryDecision retry = policy.decide(throttle, 0, System.nanoTime());
        assertTrue("a within-budget throttle must retry", retry.retry());
        assertEquals("a committed retry ramps the adaptive backoff", 2, backoff.currentMultiplier());
    }

    public void testThrottleMaxRetriesAccessor() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
        assertEquals(RetryPolicy.DEFAULT_THROTTLE_MAX_RETRIES, policy.throttleMaxRetries());
    }

    public void testWithThrottleConfig() {
        RetryPolicy policy = RetryPolicy.DEFAULT.withThrottleConfig(20, 1000, 60_000);
        assertEquals(20, policy.throttleMaxRetries());
        assertEquals(RetryPolicy.DEFAULT_MAX_RETRIES, policy.maxRetries());
    }

    // --- Cancellation-aware backoff tests ---

    public void testExecuteAbortsBackoffWhenCancelled() {
        // A high throttle budget with long delays would otherwise keep this thread sleeping for a long time;
        // under an active cancellation scope the first scheduled retry must abort instead of sleeping.
        RetryPolicy policy = new RetryPolicy(0, 0, 0, 10, 30_000, 30_000, RetryPolicy.NO_BUDGET, null);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/key");

        TaskCancelledException ex = expectThrows(
            TaskCancelledException.class,
            () -> StorageRetryCancellation.runWithCancellation(() -> true, () -> policy.execute(() -> {
                calls.incrementAndGet();
                throw new ExternalUnavailableException(true, "throttled");
            }, "test", path))
        );

        assertNotNull(ex.getMessage());
        // Only the initial attempt ran; cancellation aborted before the first backoff sleep.
        assertEquals(1, calls.get());
    }

    public void testExecuteDoesNotAbortWhenNotCancelled() throws IOException {
        // With the cancellation supplier reporting false, behavior is unchanged: retries proceed to success.
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        AtomicInteger calls = new AtomicInteger();
        StoragePath path = StoragePath.of("s3://bucket/key");

        String result = StorageRetryCancellation.callWithCancellation(() -> false, () -> policy.execute(() -> {
            if (calls.incrementAndGet() < 3) {
                throw new SocketTimeoutException("timeout");
            }
            return "ok";
        }, "test", path));

        assertEquals("ok", result);
        assertEquals(3, calls.get());
    }

    public void testExecuteAbortsSleepWhenCancelledDuringBackoff() {
        // Long throttle delays so a non-cancellable sleep would block this thread for ~30s. The signal is
        // NOT cancelled at the pre-sleep check nor at the sleep start, then flips true during the sleep.
        RetryPolicy policy = new RetryPolicy(0, 0, 0, 10, 30_000, 30_000, RetryPolicy.NO_BUDGET, null);
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger polls = new AtomicInteger();
        // poll 1 = execute() pre-sleep check, poll 2 = sleep start, poll 3+ = in-sleep -> cancelled.
        BooleanSupplier cancel = () -> polls.incrementAndGet() > 2;
        StoragePath path = StoragePath.of("s3://bucket/key");

        long startNanos = System.nanoTime();
        expectThrows(TaskCancelledException.class, () -> StorageRetryCancellation.callWithCancellation(cancel, () -> policy.execute(() -> {
            calls.incrementAndGet();
            throw new ExternalUnavailableException(true, "throttled");
        }, "test", path)));
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertEquals("only the initial attempt ran", 1, calls.get());
        assertThat("a cancel during the backoff must abort, not wait out the full delay", elapsedMs, lessThan(5_000L));
    }

    public void testExecuteAbortsSleepWhenCancelledFromAnotherThread() throws Exception {
        RetryPolicy policy = new RetryPolicy(0, 0, 0, 10, 30_000, 30_000, RetryPolicy.NO_BUDGET, null);
        StoragePath path = StoragePath.of("s3://bucket/key");
        AtomicBoolean cancelled = new AtomicBoolean(false);
        CountDownLatch sleeping = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();

        Thread worker = new Thread(() -> {
            try {
                StorageRetryCancellation.runWithCancellation(cancelled::get, () -> policy.execute(() -> {
                    // Signal that we have entered the operation (which fails and parks in backoff next).
                    sleeping.countDown();
                    throw new ExternalUnavailableException(true, "throttled");
                }, "test", path));
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "retry-policy-cancellation-test");

        long startNanos = System.nanoTime();
        worker.start();
        assertTrue("worker did not enter the operation", sleeping.await(5, TimeUnit.SECONDS));
        cancelled.set(true);
        worker.join(TimeUnit.SECONDS.toMillis(15));
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertFalse("worker should have aborted the backoff and finished", worker.isAlive());
        assertThat(thrown.get(), instanceOf(TaskCancelledException.class));
        assertThat("a cross-thread cancel must not wait out the full throttle delay", elapsedMs, lessThan(15_000L));
    }

    public void testExecuteIgnoresCancellationOutsideScope() throws IOException {
        // No ambient scope is active, so a cancelled-looking environment cannot affect the retry loop.
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
}
