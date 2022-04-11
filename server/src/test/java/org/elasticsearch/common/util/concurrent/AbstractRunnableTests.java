/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;
import org.mockito.InOrder;

import java.util.concurrent.Callable;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests {@link AbstractRunnable}
 */
public class AbstractRunnableTests extends ESTestCase {
    public void testRunSuccess() throws Exception {
        Callable<?> runCallable = mock(Callable.class);

        AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }

            @Override
            protected void doRun() throws Exception {
                runCallable.call();
            }
        };

        runnable.run();

        verify(runCallable).call();
    }

    public void testRunFailure() throws Exception {
        RuntimeException exception = new RuntimeException();

        AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                assertSame(exception, e);
            }

            @Override
            protected void doRun() throws Exception {
                throw exception;
            }
        };

        runnable.run();
    }

    public void testOnAfterSuccess() throws Exception {
        Callable<?> runCallable = mock(Callable.class);
        Callable<?> afterCallable = mock(Callable.class);

        AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }

            @Override
            protected void doRun() throws Exception {
                runCallable.call();
            }

            @Override
            public void onAfter() {
                try {
                    afterCallable.call();
                } catch (Exception e) {
                    fail(e.toString());
                }
            }
        };

        runnable.run();

        InOrder inOrder = inOrder(runCallable, afterCallable);

        inOrder.verify(runCallable).call();
        inOrder.verify(afterCallable).call();

    }

    public void testOnAfterFailure() throws Exception {
        RuntimeException exception = new RuntimeException();
        Callable<?> afterCallable = mock(Callable.class);

        AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                assertSame(exception, e);
            }

            @Override
            protected void doRun() throws Exception {
                throw exception;
            }

            @Override
            public void onAfter() {
                try {
                    afterCallable.call();
                } catch (Exception e) {
                    fail(e.toString());
                }
            }
        };

        runnable.run();

        verify(afterCallable).call();
    }

    public void testOnRejection() throws Exception {
        RuntimeException exception = new RuntimeException();
        Callable<?> failureCallable = mock(Callable.class);

        AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                assertSame(exception, e);

                try {
                    failureCallable.call();
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    fail(inner.toString());
                }
            }

            @Override
            protected void doRun() throws Exception {
                fail("Not tested");
            }
        };

        runnable.onRejection(exception);
    }

    public void testIsForceExecutuonDefaultsFalse() {
        AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }

            @Override
            protected void doRun() throws Exception {
                fail("Not tested");
            }
        };

        assertFalse(runnable.isForceExecution());
    }
}
