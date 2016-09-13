/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
                }
                catch (Exception e) {
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
                }
                catch (Exception e) {
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
                }
                catch (Exception inner) {
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
