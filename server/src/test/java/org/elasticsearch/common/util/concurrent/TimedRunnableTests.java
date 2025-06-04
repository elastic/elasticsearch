/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public final class TimedRunnableTests extends ESTestCase {

    public void testTimedRunnableDelegatesToAbstractRunnable() {
        final boolean isForceExecution = randomBoolean();
        final AtomicBoolean onAfter = new AtomicBoolean();
        final AtomicReference<Exception> onRejection = new AtomicReference<>();
        final AtomicBoolean doRun = new AtomicBoolean();

        final AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return isForceExecution;
            }

            @Override
            public void onAfter() {
                onAfter.set(true);
            }

            @Override
            public void onRejection(final Exception e) {
                onRejection.set(e);
            }

            @Override
            public void onFailure(final Exception e) {}

            @Override
            protected void doRun() throws Exception {
                doRun.set(true);
            }
        };

        final TimedRunnable timedRunnable = new TimedRunnable(runnable);

        assertThat(timedRunnable.isForceExecution(), equalTo(isForceExecution));

        final Exception rejection = new RejectedExecutionException();
        timedRunnable.onRejection(rejection);
        assertThat(onRejection.get(), equalTo(rejection));

        timedRunnable.run();
        assertTrue(doRun.get());
        assertTrue(onAfter.get());
    }

    public void testTimedRunnableDelegatesRunInFailureCase() {
        final AtomicBoolean onAfter = new AtomicBoolean();
        final AtomicReference<Exception> onFailure = new AtomicReference<>();
        final AtomicBoolean doRun = new AtomicBoolean();

        final Exception exception = new Exception();

        final AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onAfter() {
                onAfter.set(true);
            }

            @Override
            public void onFailure(final Exception e) {
                onFailure.set(e);
            }

            @Override
            protected void doRun() throws Exception {
                doRun.set(true);
                throw exception;
            }
        };

        final TimedRunnable timedRunnable = new TimedRunnable(runnable);
        timedRunnable.run();
        assertTrue(doRun.get());
        assertThat(onFailure.get(), equalTo(exception));
        assertTrue(onAfter.get());
    }

    public void testTimedRunnableRethrowsExceptionWhenNotAbstractRunnable() {
        final AtomicBoolean hasRun = new AtomicBoolean();
        final RuntimeException exception = new RuntimeException();

        final Runnable runnable = () -> {
            hasRun.set(true);
            throw exception;
        };

        final TimedRunnable timedRunnable = new TimedRunnable(runnable);
        final RuntimeException thrown = expectThrows(RuntimeException.class, () -> timedRunnable.run());
        assertTrue(hasRun.get());
        assertSame(exception, thrown);
    }

    public void testTimedRunnableRethrowsRejectionWhenNotAbstractRunnable() {
        final AtomicBoolean hasRun = new AtomicBoolean();
        final RuntimeException exception = new RuntimeException();

        final Runnable runnable = () -> {
            hasRun.set(true);
            throw new AssertionError("should not run");
        };

        final TimedRunnable timedRunnable = new TimedRunnable(runnable);
        final RuntimeException thrown = expectThrows(RuntimeException.class, () -> timedRunnable.onRejection(exception));
        assertFalse(hasRun.get());
        assertSame(exception, thrown);
    }

    public void testTimedRunnableExecutesNestedOnAfterOnce() {
        final AtomicBoolean afterRan = new AtomicBoolean(false);
        new TimedRunnable(new AbstractRunnable() {

            @Override
            public void onFailure(final Exception e) {}

            @Override
            protected void doRun() {}

            @Override
            public void onAfter() {
                if (afterRan.compareAndSet(false, true) == false) {
                    fail("onAfter should have only been called once");
                }
            }
        }).run();
        assertTrue(afterRan.get());
    }

    public void testNestedOnFailureTriggersOnlyOnce() {
        final Exception expectedException = new RuntimeException();
        final AtomicBoolean onFailureRan = new AtomicBoolean(false);
        RuntimeException thrown = expectThrows(RuntimeException.class, () -> new TimedRunnable(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                if (onFailureRan.compareAndSet(false, true) == false) {
                    fail("onFailure should have only been called once");
                }
                ExceptionsHelper.reThrowIfNotNull(e);
            }

            @Override
            protected void doRun() throws Exception {
                throw expectedException;
            }

        }).run());
        assertTrue(onFailureRan.get());
        assertSame(thrown, expectedException);
    }
}
