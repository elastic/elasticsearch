/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ListTasksIT extends ESSingleNodeTestCase {

    public void testListTasksFilteredByDescription() {

        // The list tasks action itself is filtered out via this description filter
        assertThat(clusterAdmin().prepareListTasks().setDetailed(true).setDescriptions("match_nothing*").get().getTasks(), is(empty()));

        // The list tasks action itself is kept via this description filter which matches everything
        assertThat(clusterAdmin().prepareListTasks().setDetailed(true).setDescriptions("*").get().getTasks(), is(not(empty())));

    }

    public void testListTasksValidation() {

        ActionRequestValidationException ex = expectThrows(
            ActionRequestValidationException.class,
            clusterAdmin().prepareListTasks().setDescriptions("*")
        );
        assertThat(ex.getMessage(), containsString("matching on descriptions is not available when [detailed] is false"));

    }

    public void testWaitForCompletion() throws Exception {
        final var threadPool = getInstanceFromNode(ThreadPool.class);
        final var threadContext = threadPool.getThreadContext();

        final var barrier = new CyclicBarrier(2);
        getInstanceFromNode(PluginsService.class).filterPlugins(TestPlugin.class).findFirst().get().barrier = barrier;

        final var testActionFuture = new PlainActionFuture<ActionResponse.Empty>();
        client().execute(TEST_ACTION, new TestRequest(), testActionFuture.map(r -> {
            assertThat(threadContext.getResponseHeaders().get(TestTransportAction.HEADER_NAME), hasItem(TestTransportAction.HEADER_VALUE));
            return r;
        }));

        barrier.await(10, TimeUnit.SECONDS);

        final var listTasksResponse = clusterAdmin().prepareListTasks().setActions(TestTransportAction.NAME).get();
        assertThat(listTasksResponse.getNodeFailures(), empty());
        assertEquals(1, listTasksResponse.getTasks().size());
        final var task = listTasksResponse.getTasks().get(0);
        assertEquals(TestTransportAction.NAME, task.action());

        final var listWaitFuture = new PlainActionFuture<Void>();
        clusterAdmin().prepareListTasks()
            .setTargetTaskId(task.taskId())
            .setWaitForCompletion(true)
            .execute(listWaitFuture.delegateFailure((l, listResult) -> {
                assertEquals(1, listResult.getTasks().size());
                assertEquals(task.taskId(), listResult.getTasks().get(0).taskId());
                // the task must now be complete:
                clusterAdmin().prepareListTasks().setActions(TestTransportAction.NAME).execute(l.map(listAfterWaitResult -> {
                    assertThat(listAfterWaitResult.getTasks(), empty());
                    assertThat(listAfterWaitResult.getNodeFailures(), empty());
                    assertThat(listAfterWaitResult.getTaskFailures(), empty());
                    return null;
                }));
                // and we must not see its header:
                assertNull(threadContext.getResponseHeaders().get(TestTransportAction.HEADER_NAME));
            }));

        // briefly fill up the management pool so that (a) we know the wait has started and (b) we know it's not blocking
        flushThreadPoolExecutor(threadPool, ThreadPool.Names.MANAGEMENT);

        final var getWaitFuture = new PlainActionFuture<Void>();
        clusterAdmin().prepareGetTask(task.taskId()).setWaitForCompletion(true).execute(getWaitFuture.delegateFailure((l, getResult) -> {
            assertTrue(getResult.getTask().isCompleted());
            assertEquals(task.taskId(), getResult.getTask().getTask().taskId());
            // the task must now be complete:
            clusterAdmin().prepareListTasks().setActions(TestTransportAction.NAME).execute(l.map(listAfterWaitResult -> {
                assertThat(listAfterWaitResult.getTasks(), empty());
                assertThat(listAfterWaitResult.getNodeFailures(), empty());
                assertThat(listAfterWaitResult.getTaskFailures(), empty());
                return null;
            }));
            // and we must not see its header:
            assertNull(threadContext.getResponseHeaders().get(TestTransportAction.HEADER_NAME));
        }));

        assertFalse(listWaitFuture.isDone());
        assertFalse(testActionFuture.isDone());
        barrier.await(10, TimeUnit.SECONDS);
        testActionFuture.get(10, TimeUnit.SECONDS);
        listWaitFuture.get(10, TimeUnit.SECONDS);
        getWaitFuture.get(10, TimeUnit.SECONDS);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(TestPlugin.class);
    }

    private static final ActionType<ActionResponse.Empty> TEST_ACTION = new ActionType<>(TestTransportAction.NAME);

    public static class TestPlugin extends Plugin implements ActionPlugin {
        volatile CyclicBarrier barrier;

        @Override
        public List<ActionHandler> getActions() {
            return List.of(new ActionHandler(TEST_ACTION, TestTransportAction.class));
        }
    }

    public static class TestRequest extends LegacyActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class TestTransportAction extends HandledTransportAction<TestRequest, ActionResponse.Empty> {

        static final String NAME = "internal:test/action";

        static final String HEADER_NAME = "HEADER_NAME";
        static final String HEADER_VALUE = "HEADER_VALUE";

        private final TestPlugin testPlugin;
        private final ThreadPool threadPool;

        @Inject
        public TestTransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            PluginsService pluginsService,
            ThreadPool threadPool
        ) {
            super(NAME, transportService, actionFilters, in -> new TestRequest(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
            testPlugin = pluginsService.filterPlugins(TestPlugin.class).findFirst().get();
            this.threadPool = threadPool;
        }

        @Override
        protected void doExecute(Task task, TestRequest request, ActionListener<ActionResponse.Empty> listener) {
            final var barrier = testPlugin.barrier;
            assertNotNull(barrier);
            threadPool.generic().execute(ActionRunnable.run(listener, () -> {
                barrier.await(10, TimeUnit.SECONDS);
                threadPool.getThreadContext().addResponseHeader(HEADER_NAME, HEADER_VALUE);
                barrier.await(10, TimeUnit.SECONDS);
            }));
        }
    }

}
