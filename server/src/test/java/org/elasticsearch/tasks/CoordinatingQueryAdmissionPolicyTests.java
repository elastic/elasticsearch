/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CoordinatingQueryAdmissionPolicyTests extends ESTestCase {

    private static final String SEARCH = "indices:data/read/search";
    private static final String LOCAL_NODE = "test";

    private ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    private TaskManager newTaskManager() {
        return new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.NOOP, LOCAL_NODE);
    }

    private Task admit(TaskManager taskManager, CoordinatingQueryAdmissionPolicy policy, SearchRequest request) {
        Task task = taskManager.register("transport", SEARCH, request);
        policy.admit(task);
        return task;
    }

    public void testIncrementsUntilUnregister() {
        TaskManager taskManager = newTaskManager();
        CoordinatingQueryAdmissionPolicy policy = new CoordinatingQueryAdmissionPolicy(100, taskManager);
        assertThat(policy.current(), equalTo(0));
        assertThat(policy.rejected(), equalTo(0));

        Task first = admit(taskManager, policy, new SearchRequest());
        assertThat(policy.current(), equalTo(1));

        Task second = admit(taskManager, policy, new SearchRequest());
        assertThat(policy.current(), equalTo(2));

        taskManager.unregister(first);
        assertThat(policy.current(), equalTo(1));

        taskManager.unregister(second);
        assertThat(policy.current(), equalTo(0));
    }

    public void testRegisterWithoutAdmitDoesNotCount() {
        TaskManager taskManager = newTaskManager();
        CoordinatingQueryAdmissionPolicy policy = new CoordinatingQueryAdmissionPolicy(100, taskManager);

        Task task = taskManager.register("transport", SEARCH, new SearchRequest());
        assertThat(policy.current(), equalTo(0));
        assertThat(policy.rejected(), equalTo(0));
        taskManager.unregister(task);
        assertThat(policy.current(), equalTo(0));
        assertThat(policy.rejected(), equalTo(0));
    }

    public void testConcurrentAdmitAndUnregister() throws Exception {
        TaskManager taskManager = newTaskManager();
        CoordinatingQueryAdmissionPolicy policy = new CoordinatingQueryAdmissionPolicy(100, taskManager);
        int threads = between(2, 8);
        int operationsPerThread = between(10, 50);
        CyclicBarrier start = new CyclicBarrier(threads);
        AtomicInteger failures = new AtomicInteger();
        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            workers[i] = new Thread(() -> {
                try {
                    start.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        Task task = admit(taskManager, policy, new SearchRequest());
                        taskManager.unregister(task);
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                    throw new AssertionError(e);
                }
            });
            workers[i].start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertThat(failures.get(), equalTo(0));
        assertThat(policy.current(), equalTo(0));
        assertThat(policy.rejected(), equalTo(0));
    }

    public void testAdmitRejectsAtLimit() {
        TaskManager taskManager = newTaskManager();
        CoordinatingQueryAdmissionPolicy policy = new CoordinatingQueryAdmissionPolicy(1, taskManager);

        Task task = admit(taskManager, policy, new SearchRequest());
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(0));

        Task rejectedTask = taskManager.register("transport", SEARCH, new SearchRequest());
        EsRejectedExecutionException rejected = expectThrows(EsRejectedExecutionException.class, () -> policy.admit(rejectedTask));
        assertThat(rejected.getMessage(), containsString("limit [1]"));
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(1));
        taskManager.unregister(rejectedTask);
        assertThat(policy.current(), equalTo(1));
        assertThat(taskManager.getCancellableTasks().size(), equalTo(1));

        Task rejectedAgain = taskManager.register("transport", SEARCH, new SearchRequest());
        expectThrows(EsRejectedExecutionException.class, () -> policy.admit(rejectedAgain));
        assertThat(policy.rejected(), equalTo(2));
        taskManager.unregister(rejectedAgain);

        Task notAdmitted = taskManager.register("transport", SEARCH, new SearchRequest());
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(2));
        taskManager.unregister(notAdmitted);

        taskManager.unregister(task);
        assertThat(policy.current(), equalTo(0));
        assertThat(policy.rejected(), equalTo(2));
        Task again = admit(taskManager, policy, new SearchRequest());
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(2));
        taskManager.unregister(again);
    }

    public void testChildOfAdmittedTaskCountsButDoesNotReject() {
        TaskManager taskManager = newTaskManager();
        CoordinatingQueryAdmissionPolicy policy = new CoordinatingQueryAdmissionPolicy(1, taskManager);

        Task parent = admit(taskManager, policy, new SearchRequest());
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(0));

        SearchRequest childRequest = new SearchRequest();
        childRequest.setParentTask(LOCAL_NODE, parent.getId());
        Task child = admit(taskManager, policy, childRequest);
        assertThat(policy.current(), equalTo(2));
        assertThat(policy.rejected(), equalTo(0));

        Task rejectedTask = taskManager.register("transport", SEARCH, new SearchRequest());
        EsRejectedExecutionException rejected = expectThrows(EsRejectedExecutionException.class, () -> policy.admit(rejectedTask));
        assertThat(rejected.getMessage(), containsString("limit [1]"));
        assertThat(policy.current(), equalTo(2));
        assertThat(policy.rejected(), equalTo(1));
        taskManager.unregister(rejectedTask);

        taskManager.unregister(child);
        assertThat(policy.current(), equalTo(1));
        Task rejectedAgain = taskManager.register("transport", SEARCH, new SearchRequest());
        expectThrows(EsRejectedExecutionException.class, () -> policy.admit(rejectedAgain));
        assertThat(policy.rejected(), equalTo(2));
        taskManager.unregister(rejectedAgain);

        taskManager.unregister(parent);
        assertThat(policy.current(), equalTo(0));
        Task again = admit(taskManager, policy, new SearchRequest());
        assertThat(policy.current(), equalTo(1));
        taskManager.unregister(again);
    }

    public void testChildOfParentThatWasNotAdmittedIsANewQuery() {
        TaskManager taskManager = newTaskManager();
        CoordinatingQueryAdmissionPolicy policy = new CoordinatingQueryAdmissionPolicy(1, taskManager);

        Task occupying = admit(taskManager, policy, new SearchRequest());
        Task unadmittedParent = taskManager.register("transport", "indices:data/write/reindex", new SearchRequest());
        SearchRequest childRequest = new SearchRequest();
        childRequest.setParentTask(LOCAL_NODE, unadmittedParent.getId());
        Task child = taskManager.register("transport", SEARCH, childRequest);
        expectThrows(EsRejectedExecutionException.class, () -> policy.admit(child));
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(1));
        taskManager.unregister(child);

        taskManager.unregister(occupying);
        taskManager.unregister(unadmittedParent);
        Task admitted = admit(taskManager, policy, childRequest);
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(1));
        taskManager.unregister(admitted);
    }

    public void testChildOfRemoteParentIsANewQuery() {
        TaskManager taskManager = newTaskManager();
        CoordinatingQueryAdmissionPolicy policy = new CoordinatingQueryAdmissionPolicy(1, taskManager);

        Task occupying = admit(taskManager, policy, new SearchRequest());
        SearchRequest childRequest = new SearchRequest();
        childRequest.setParentTask("other-node", occupying.getId());
        Task child = taskManager.register("transport", SEARCH, childRequest);
        expectThrows(EsRejectedExecutionException.class, () -> policy.admit(child));
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(1));
        taskManager.unregister(child);
        taskManager.unregister(occupying);
    }

    public void testSearchUnderMsearchUnderAdmittedSearchIsNotRejected() {
        TaskManager taskManager = newTaskManager();
        CoordinatingQueryAdmissionPolicy policy = new CoordinatingQueryAdmissionPolicy(1, taskManager);

        Task search = admit(taskManager, policy, new SearchRequest());
        SearchRequest msearchRequest = new SearchRequest();
        msearchRequest.setParentTask(LOCAL_NODE, search.getId());
        Task msearch = taskManager.register("transport", "indices:data/read/msearch", msearchRequest);

        SearchRequest innerRequest = new SearchRequest();
        innerRequest.setParentTask(LOCAL_NODE, msearch.getId());
        Task inner = admit(taskManager, policy, innerRequest);
        assertThat(policy.current(), equalTo(2));
        assertThat(policy.rejected(), equalTo(0));

        Task rejectedTask = taskManager.register("transport", SEARCH, new SearchRequest());
        expectThrows(EsRejectedExecutionException.class, () -> policy.admit(rejectedTask));
        assertThat(policy.rejected(), equalTo(1));
        taskManager.unregister(rejectedTask);
        taskManager.unregister(inner);
        taskManager.unregister(msearch);
        taskManager.unregister(search);
    }

    public void testNoopDoesNotCountOrReject() {
        TaskManager taskManager = newTaskManager();
        Task first = admit(taskManager, CoordinatingQueryAdmissionPolicy.NOOP, new SearchRequest());
        Task second = admit(taskManager, CoordinatingQueryAdmissionPolicy.NOOP, new SearchRequest());
        assertThat(CoordinatingQueryAdmissionPolicy.NOOP.current(), equalTo(0));
        assertThat(CoordinatingQueryAdmissionPolicy.NOOP.rejected(), equalTo(0));
        assertThat(taskManager.getCancellableTasks().size(), equalTo(2));
        taskManager.unregister(first);
        taskManager.unregister(second);
    }

    public void testAdmitRejectsAfterCreatingTask() {
        TaskManager taskManager = newTaskManager();
        CoordinatingQueryAdmissionPolicy policy = new CoordinatingQueryAdmissionPolicy(0, taskManager);

        Task task = taskManager.register("transport", SEARCH, new SearchRequest());
        expectThrows(EsRejectedExecutionException.class, () -> policy.admit(task));
        assertThat(taskManager.getCancellableTasks().isEmpty(), equalTo(false));
        assertThat(policy.current(), equalTo(0));
        assertThat(policy.rejected(), equalTo(1));
        taskManager.unregister(task);
        assertThat(policy.current(), equalTo(0));
    }
}
