/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class TransformRetryableStartUpListenerTests extends ESTestCase {
    /**
     * When the action succeeds on the first try
     * Then we invoked the retryListener with "false" and then invoked the actionListener's onResponse.
     */
    public void testFirstRunPasses() {
        var retryResult = new AtomicReference<Boolean>();
        var responseResult = new AtomicInteger(0);
        var context = mock(TransformContext.class);

        var listener = new TransformRetryableStartUpListener<>(
            "transformId",
            immediatelyReturn(),
            responseListener(responseResult),
            retryListener(retryResult),
            () -> true,
            context
        );

        callThreeTimes("transformId", listener);

        // assert only 1 success and no retries
        assertEquals("Response Listener should only be called once.", 1, responseResult.get());
        assertNotNull("Retry Listener should be called.", retryResult.get());
        assertFalse("Retries should not be scheduled.", retryResult.get());
        verify(context, only()).resetStartUpFailureCount();
    }

    private Consumer<ActionListener<Void>> immediatelyReturn() {
        return l -> l.onResponse(null);
    }

    private ActionListener<Void> responseListener(AtomicInteger result) {
        return ActionListener.wrap(r -> {
            if (result.compareAndSet(0, 1) == false) {
                fail("Response Listener should only be called at most once for every test.");
            }
        }, e -> {
            if (result.compareAndSet(0, -1) == false) {
                fail("Response Listener should only be called at most once for every test.");
            }
        });
    }

    private ActionListener<Boolean> retryListener(AtomicReference<Boolean> result) {
        return ActionListener.wrap(result::set, e -> fail("Retry Listener is never expected to fail."));
    }

    private void callThreeTimes(String transformId, TransformRetryableStartUpListener<?> listener) {
        listener.triggered(event(transformId));
        listener.triggered(event(transformId));
        listener.triggered(event(transformId));
    }

    private TransformScheduler.Event event(String transformId) {
        return new TransformScheduler.Event(transformId, System.currentTimeMillis(), System.currentTimeMillis());
    }

    /**
     * When the action fails once then succeeds on the second try
     * Then we invoked the retryListener with "true" and then invoked the actionListener's onResponse.
     */
    public void testFirstRunFails() {
        var retryResult = new AtomicReference<Boolean>();
        var responseResult = new AtomicInteger(0);
        var context = mock(TransformContext.class);

        var listener = new TransformRetryableStartUpListener<>(
            "transformId",
            failOnceThen(immediatelyReturn()),
            responseListener(responseResult),
            retryListener(retryResult),
            () -> true,
            context
        );

        callThreeTimes("transformId", listener);

        // assert only 1 retry and 1 success
        assertEquals("Response Listener should only be called once.", 1, responseResult.get());
        assertNotNull("Retry Listener should be called.", retryResult.get());
        assertTrue("Retries should be scheduled.", retryResult.get());
        verify(context, times(1)).incrementAndGetStartUpFailureCount(any(IllegalStateException.class));
        verify(context, times(1)).resetStartUpFailureCount();
    }

    private Consumer<ActionListener<Void>> failOnceThen(Consumer<ActionListener<Void>> followup) {
        var firstRun = new AtomicBoolean(true);
        return l -> {
            if (firstRun.compareAndSet(true, false)) {
                l.onFailure(new IllegalStateException("first call fails"));
            } else {
                followup.accept(l);
            }
        };
    }

    /**
     * When the TransformRetryableStartUpListener is never invoked
     * Then there should be no failures to report
     */
    public void testUnusedRetryableIsNotReported() {
        var context = mock(TransformContext.class);

        new TransformRetryableStartUpListener<>(
            "transformId",
            failOnceThen(immediatelyReturn()),
            responseListener(),
            retryListener(),
            () -> true,
            context
        );

        verifyNoInteractions(context);
    }

    private ActionListener<Boolean> retryListener() {
        return retryListener(new AtomicReference<>());
    }

    private ActionListener<Void> responseListener() {
        return responseListener(new AtomicInteger());
    }

    /**
     * Given one transformId
     * When we receive an event for another transformId
     * Then we should not take any action
     */
    public void testWrongTransformIdIsIgnored() {
        var correctTransformId = "transformId";
        var incorrectTransformId = "someOtherTransformId";
        var retryResult = new AtomicReference<Boolean>();
        var responseResult = new AtomicInteger(0);
        var context = mock(TransformContext.class);

        var listener = new TransformRetryableStartUpListener<>(
            correctTransformId,
            failOnceThen(immediatelyReturn()),
            responseListener(responseResult),
            retryListener(retryResult),
            () -> true,
            context
        );

        listener.triggered(event(incorrectTransformId));

        assertEquals("Response Listener should never be called once.", 0, responseResult.get());
        assertNull("Retry Listener should not be called.", retryResult.get());
        verifyNoInteractions(context);
    }

    /**
     * Given an action that always fails
     * When shouldRetry returns true and then false
     * Then we should call the actionListener's onFailure handler
     */
    public void testCancelRetries() {
        var retryResult = new AtomicReference<Boolean>();
        var responseResult = new AtomicInteger(0);
        var context = mock(TransformContext.class);
        var runTwice = new AtomicBoolean(true);

        var listener = new TransformRetryableStartUpListener<>(
            "transformId",
            alwaysFail(),
            responseListener(responseResult),
            retryListener(retryResult),
            () -> runTwice.compareAndSet(true, false),
            context
        );

        callThreeTimes("transformId", listener);

        // assert only 1 retry and 1 failure
        assertEquals("Response Listener should only be called once.", -1, responseResult.get());
        assertNotNull("Retry Listener should be called.", retryResult.get());
        assertTrue("Retries should be scheduled.", retryResult.get());
        verify(context, times(1)).incrementAndGetStartUpFailureCount(any(IllegalStateException.class));
        verify(context, times(1)).resetStartUpFailureCount();
    }

    private Consumer<ActionListener<Void>> alwaysFail() {
        return l -> l.onFailure(new IllegalStateException("always fail"));
    }

    /**
     * Given an action that always fails
     * When shouldRetry returns false
     * Then we should call the actionListener's onFailure handler and the retryListener with "false"
     */
    public void testCancelRetryImmediately() {
        var retryResult = new AtomicReference<Boolean>();
        var responseResult = new AtomicInteger(0);
        var context = mock(TransformContext.class);

        var listener = new TransformRetryableStartUpListener<>(
            "transformId",
            alwaysFail(),
            responseListener(responseResult),
            retryListener(retryResult),
            () -> false,
            context
        );

        callThreeTimes("transformId", listener);

        // assert no retries and 1 failure
        assertEquals("Response Listener should only be called once.", -1, responseResult.get());
        assertNotNull("Retry Listener should be called.", retryResult.get());
        assertFalse("Retries should not be scheduled.", retryResult.get());
        verify(context, only()).resetStartUpFailureCount();
    }

    /**
     * Given triggered has been called
     * When we call trigger a second time
     * And the first call has not finished
     * Then we should not take any action
     *
     * Given the first call has finished
     * When we call trigger a third time
     * Then we should successfully call the action
     */
    public void testRunOneAtATime() {
        var retryResult = new AtomicReference<Boolean>();
        var responseResult = new AtomicInteger(0);
        var context = mock(TransformContext.class);

        var savedListener = new AtomicReference<ActionListener<Void>>();
        Consumer<ActionListener<Void>> action = l -> {
            if (savedListener.compareAndSet(null, l) == false) {
                fail("Action should only be called once.");
            }
        };

        var listener = new TransformRetryableStartUpListener<>(
            "transformId",
            action,
            responseListener(responseResult),
            retryListener(retryResult),
            () -> true,
            context
        );

        callThreeTimes("transformId", listener);

        // verify the action has been called
        assertNotNull(savedListener.get());

        // assert the listener has not been called yet
        assertEquals("Response Listener should never be called once.", 0, responseResult.get());
        assertNull("Retry Listener should not be called.", retryResult.get());
        verifyNoInteractions(context);

        savedListener.get().onFailure(new IllegalStateException("first call fails"));

        // assert only 1 retry and 0 success
        assertEquals("Response Listener should only be called once.", 0, responseResult.get());
        assertNotNull("Retry Listener should be called.", retryResult.get());
        assertTrue("Retries should be scheduled.", retryResult.get());
        verify(context, times(1)).incrementAndGetStartUpFailureCount(any(IllegalStateException.class));
        verify(context, never()).resetStartUpFailureCount();

        // rerun and succeed
        savedListener.set(null);
        callThreeTimes("transformId", listener);
        savedListener.get().onResponse(null);

        // assert only 1 retry and 1 failure
        assertEquals("Response Listener should only be called once.", 1, responseResult.get());
        assertNotNull("Retry Listener should be called.", retryResult.get());
        assertTrue("Retries should be scheduled.", retryResult.get());
        verify(context, times(1)).incrementAndGetStartUpFailureCount(any(IllegalStateException.class));
        verify(context, times(1)).resetStartUpFailureCount();
    }
}
