/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskManager;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.exception.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.elasticsearch.test.ESIntegTestCase.internalCluster;
import static org.elasticsearch.test.ESTestCase.asInstanceOf;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.safeAwait;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Utility plugin that captures the invocation of an action on a node after the task has been registered with the {@link TaskManager},
 * cancels it (e.g. by closing the connection used for the original REST request), verifies that the corresponding task is cancelled, then
 * lets the action execution proceed in order to verify that it fails with a {@link TaskCancelledException}. This allows to verify a few key
 * aspects of the cancellability of tasks:
 * <ul>
 *     <li>The task that the request creates is cancellable.</li>
 *     <li>The REST handler propagates cancellation to the task it starts.</li>
 *     <li>The action implementation checks for cancellation at least once.</li>
 * </ul>
 * However, note that this is implemented as an {@link ActionFilter} it blocks and cancels the action before it even starts executing on the
 * local node, so it does not verify that the cancellation is processed promptly at all stages of the execution of the action, nor that
 * cancellations are propagated correctly to subsidiary actions.
 */
public class CancellableActionTestPlugin extends Plugin implements ActionPlugin {

    public interface CapturingAction extends Releasable {
        /**
         * @param doCancel callback to invoke when the specified action has started which should cancel the action.
         */
        void captureAndCancel(Runnable doCancel);
    }

    /**
     * Returns a {@link CapturingAction}, typically for use in a try-with-resources block, which can be used to capture and cancel exactly
     * one invocation of the specified action on the specified node.
     */
    public static CapturingAction capturingActionOnNode(String actionName, String nodeName) {
        final var plugins = internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(CancellableActionTestPlugin.class)
            .toList();
        assertThat("unique " + CancellableActionTestPlugin.class.getCanonicalName() + " plugin not found", plugins, hasSize(1));
        return plugins.get(0).capturingAction(actionName);
    }

    private volatile String capturedActionName;
    private final AtomicReference<SubscribableListener<Captured>> capturedRef = new AtomicReference<>();

    private record Captured(Runnable doCancel, CountDownLatch countDownLatch) {}

    private CapturingAction capturingAction(String actionName) {
        final var captureListener = new SubscribableListener<Captured>();
        capturedActionName = actionName;
        assertTrue(capturedRef.compareAndSet(null, captureListener));

        final var completionLatch = new CountDownLatch(1);

        return new CapturingAction() {
            @Override
            public void captureAndCancel(Runnable doCancel) {
                assertFalse(captureListener.isDone());
                captureListener.onResponse(new Captured(doCancel, completionLatch));
                safeAwait(completionLatch);
            }

            @Override
            public void close() {
                // verify that a request was indeed captured
                assertNull(capturedRef.get());
                // and that it completed
                assertEquals(0, completionLatch.getCount());
            }
        };
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(new ActionFilter() {

            private final int order = randomInt();

            @Override
            public int order() {
                return order;
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                if (action.equals(capturedActionName)) {
                    final var capturingListener = capturedRef.getAndSet(null);
                    if (capturingListener != null) {
                        final var cancellableTask = asInstanceOf(CancellableTask.class, task);
                        capturingListener.addListener(assertNoFailureListener(captured -> {
                            cancellableTask.addListener(() -> chain.proceed(task, action, request, assertNoSuccessListener(e -> {
                                assertThat(unwrapCause(e), instanceOf(TaskCancelledException.class));
                                listener.onFailure(e);
                                captured.countDownLatch().countDown();
                            })));
                            assertFalse(cancellableTask.isCancelled());
                            captured.doCancel().run();
                        }));
                        return;
                    }
                }

                chain.proceed(task, action, request, listener);
            }
        });
    }
}
