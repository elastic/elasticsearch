/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransformRetryableActionsTests extends ESTestCase {
    public void testFirstRunPasses() {
        var retryResult = new AtomicReference<Boolean>();
        var retryListener = ActionListener.wrap(retryResult::set, e -> fail("Retry Listener is never expected to fail."));
        var responseResult = new AtomicBoolean(false);
        var responseListener = ActionListener.<Void>wrap(r -> responseResult.set(true), e -> {});

        Consumer<ActionListener<Void>> immediatelyReturn = callOnResponse();

        transformRetryableActions().register("transformId", "name", immediatelyReturn, responseListener, retryListener, runTwice()).run();

        assertNotNull("Retry Listener should be called.", retryResult.get());
        assertFalse("Retries should not be scheduled.", retryResult.get());
        assertTrue("Response Listener should be called.", responseResult.get());
    }

    private Consumer<ActionListener<Void>> callOnResponse() {
        return l -> l.onResponse(null);
    }

    private TransformRetryableActions transformRetryableActions() {
        return transformRetryableActions(mock(Scheduler.ScheduledCancellable.class));
    }

    private Supplier<Boolean> runTwice() {
        var tests = new AtomicBoolean(true);
        return () -> tests.compareAndSet(true, false);
    }

    public void testFirstRunFails() {
        var retryResult = new AtomicReference<Boolean>();
        var retryListener = ActionListener.wrap(retryResult::set, e -> fail("Retry Listener is never expected to fail."));
        var responseResult = new AtomicBoolean(false);
        var responseListener = ActionListener.<Void>wrap(r -> responseResult.set(true), e -> {});

        var failOnceThenReturn = failOnceThen(callOnResponse());
        transformRetryableActions().register("transformId", "name", failOnceThenReturn, responseListener, retryListener, runTwice()).run();

        assertNotNull("Retry Listener should be called.", retryResult.get());
        assertTrue("Retries should be scheduled.", retryResult.get());
        assertTrue("Response Listener should be called.", responseResult.get());
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

    public void testRegisteredActionsShouldDedupe() {
        var retryableActions = transformRetryableActions();

        var run1 = retryableActions.register("id1", "name", callOnResponse(), responseListener(), retryListener(), runTwice());
        var run2 = retryableActions.register("id1", "name", callOnResponse(), responseListener(), retryListener(), runTwice());

        assertSame("When we register two actions under the same transform id and name, then we return the same object.", run1, run2);
    }

    private ActionListener<Boolean> retryListener() {
        return ActionListener.<Boolean>noop().delegateResponse((l,e) -> fail(e, "Retry Listener is never expected to fail."));
    }

    private ActionListener<Void> responseListener() {
        return ActionListener.noop();
    }

    public void testDifferentNamedActionsShouldNotDedupe() {
        var retryableActions = transformRetryableActions();

        var run1 = retryableActions.register("id1", "name", callOnResponse(), responseListener(), retryListener(), runTwice());
        var run2 = retryableActions.register("id2", "name", callOnResponse(), responseListener(), retryListener(), runTwice());

        assertNotSame("When we register two actions under a different transform id and name, then we return new objects.", run1, run2);
    }

    public void testRetryableIsReported() {
        var transformId = "id1";
        var name = "testRetryableIsReported";
        var retryableActions = transformRetryableActions();

        var testRan = new AtomicBoolean(false);
        var failOnceThenTest = failOnceThen(l -> {
            // verify that we have one value recorded
            var retryDescriptions = retryableActions.retries(transformId);
            assertThat(retryDescriptions.size(), equalTo(1));
            var testDescription = retryDescriptions.get(0);
            assertThat(testDescription.message(), containsString("Retrying Transform action due to error"));
            assertThat(testDescription.retryCount(), equalTo(1));
            testRan.set(true);
            l.onResponse(null);
        });

        retryableActions.register(transformId, name, failOnceThenTest, responseListener(), retryListener(), runTwice()).run();
        assertTrue("Listener should have been called by the registered runnable.", testRan.get());
        assertTrue("When onResponse is called, the retryable action unregisters itself.", retryableActions.retries(transformId).isEmpty());
    }

    public void testUnusedRetryableIsNotReported() {
        var retryableActions = transformRetryableActions();

        retryableActions.register("id1", "name", callOnResponse(), responseListener(), retryListener(), runTwice());

        assertTrue(retryableActions.retries("id1").isEmpty());
    }

    public void testStopAllCallsFailureHandler() {
        var scheduledAction = mock(Scheduler.ScheduledCancellable.class);
        var retryableActions = transformRetryableActions(scheduledAction);

        var failOnceThenTest = failOnceThen(l -> {
            // do nothing
        });

        retryableActions.register("id1", "name", failOnceThenTest, responseListener(), retryListener(), runTwice()).run();
        assertFalse("Action should exist for StopAll to cancel it.", retryableActions.retries("id1").isEmpty());

        retryableActions.stopAll("id1");

        assertTrue("StopAll deregisters the action.", retryableActions.retries("id1").isEmpty());
        verify(scheduledAction, atLeastOnce()).cancel();
    }

    private TransformRetryableActions transformRetryableActions(Scheduler.ScheduledCancellable scheduledAction) {
        var threadpool = mock(ThreadPool.class);
        when(threadpool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadpool.schedule(any(), any(), any())).thenAnswer(answer -> {
            answer.getArgument(0, Runnable.class).run();
            return scheduledAction;
        });

        var auditor = mock(TransformAuditor.class);

        return new TransformRetryableActions(threadpool, auditor);
    }

    public void testStopIdempotency() {
        var retryableActions = transformRetryableActions();

        retryableActions.register("id1", "name", failOnceThen(l -> {}), responseListener(), retryListener(), runTwice()).run();
        retryableActions.register("id2", "name", failOnceThen(l -> {}), responseListener(), retryListener(), runTwice()).run();

        assertFalse(retryableActions.retries("id1").isEmpty());
        assertFalse(retryableActions.retries("id2").isEmpty());
        try {
            retryableActions.stopAll("id1");
            retryableActions.stopAll("id1");
            retryableActions.stopAll("id1");
        } catch (Exception e) {
            fail(e, "No exception should be thrown.");
        }
        assertTrue(retryableActions.retries("id1").isEmpty());
        assertFalse(retryableActions.retries("id2").isEmpty());
    }
}
