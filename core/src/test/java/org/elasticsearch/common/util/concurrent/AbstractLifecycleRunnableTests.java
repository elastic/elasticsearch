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

import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.test.ESTestCase;

import org.mockito.InOrder;

import java.util.concurrent.Callable;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Tests {@link AbstractLifecycleRunnable}.
 */
public class AbstractLifecycleRunnableTests extends ESTestCase {
    private final Lifecycle lifecycle = mock(Lifecycle.class);
    private final ESLogger logger = mock(ESLogger.class);

    public void testDoRunOnlyRunsWhenNotStoppedOrClosed() throws Exception {
        Callable<?> runCallable = mock(Callable.class);

        // it's "not stopped or closed"
        when(lifecycle.stoppedOrClosed()).thenReturn(false);

        AbstractLifecycleRunnable runnable = new AbstractLifecycleRunnable(lifecycle, logger) {
            @Override
            public void onFailure(Throwable t) {
                fail("It should not fail");
            }

            @Override
            protected void doRunInLifecycle() throws Exception {
                runCallable.call();
            }
        };

        runnable.run();

        InOrder inOrder = inOrder(lifecycle, logger, runCallable);

        inOrder.verify(lifecycle).stoppedOrClosed();
        inOrder.verify(runCallable).call();
        inOrder.verify(lifecycle).stoppedOrClosed(); // onAfter uses it too, but we're not testing it here
        inOrder.verifyNoMoreInteractions();
    }

    public void testDoRunDoesNotRunWhenStoppedOrClosed() throws Exception {
        Callable<?> runCallable = mock(Callable.class);

        // it's stopped or closed
        when(lifecycle.stoppedOrClosed()).thenReturn(true);

        AbstractLifecycleRunnable runnable = new AbstractLifecycleRunnable(lifecycle, logger) {
            @Override
            public void onFailure(Throwable t) {
                fail("It should not fail");
            }

            @Override
            protected void doRunInLifecycle() throws Exception {
                fail("Should not run with lifecycle stopped or closed.");
            }
        };

        runnable.run();

        InOrder inOrder = inOrder(lifecycle, logger, runCallable);

        inOrder.verify(lifecycle).stoppedOrClosed();
        inOrder.verify(logger).trace(anyString());
        inOrder.verify(lifecycle).stoppedOrClosed(); // onAfter uses it too, but we're not testing it here
        inOrder.verifyNoMoreInteractions();
    }

    public void testOnAfterOnlyWhenNotStoppedOrClosed() throws Exception {
        Callable<?> runCallable = mock(Callable.class);
        Callable<?> afterCallable = mock(Callable.class);

        // it's "not stopped or closed"
        when(lifecycle.stoppedOrClosed()).thenReturn(false);

        AbstractLifecycleRunnable runnable = new AbstractLifecycleRunnable(lifecycle, logger) {
            @Override
            public void onFailure(Throwable t) {
                fail("It should not fail");
            }

            @Override
            protected void doRunInLifecycle() throws Exception {
                runCallable.call();
            }

            @Override
            protected void onAfterInLifecycle() {
                try {
                    afterCallable.call();
                }
                catch (Exception e) {
                    fail("Unexpected for mock.");
                }
            }
        };

        runnable.run();

        InOrder inOrder = inOrder(lifecycle, logger, runCallable, afterCallable);

        inOrder.verify(lifecycle).stoppedOrClosed();
        inOrder.verify(runCallable).call();
        inOrder.verify(lifecycle).stoppedOrClosed();
        inOrder.verify(afterCallable).call();
        inOrder.verifyNoMoreInteractions();
    }

    public void testOnAfterDoesNotHappenWhenStoppedOrClosed() throws Exception {
        Callable<?> runCallable = mock(Callable.class);

        // it's stopped or closed
        when(lifecycle.stoppedOrClosed()).thenReturn(true);

        AbstractLifecycleRunnable runnable = new AbstractLifecycleRunnable(lifecycle, logger) {
            @Override
            public void onFailure(Throwable t) {
                fail("It should not fail");
            }

            @Override
            protected void doRunInLifecycle() throws Exception {
                fail("Should not run with lifecycle stopped or closed.");
            }

            @Override
            protected void onAfterInLifecycle() {
                fail("Should not run with lifecycle stopped or closed.");
            }
        };

        runnable.run();

        InOrder inOrder = inOrder(lifecycle, runCallable);

        inOrder.verify(lifecycle, times(2)).stoppedOrClosed();
        inOrder.verifyNoMoreInteractions();
    }
}
