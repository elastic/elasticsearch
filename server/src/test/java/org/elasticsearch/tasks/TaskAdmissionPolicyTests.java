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

public class TaskAdmissionPolicyTests extends ESTestCase {

    private static final String TRACKED = "indices:data/read/search";
    private static final String UNTRACKED = "cluster:monitor/health";

    private ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    private TaskManager taskManager(TaskAdmissionPolicy policy) {
        return new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.NOOP, "test", policy);
    }

    public void testTrackedActionIncrementsUntilUnregister() {
        TaskAdmissionPolicy policy = new TaskAdmissionPolicy(Set.of(TRACKED), 100);
        TaskManager taskManager = taskManager(policy);
        assertThat(policy.current(), equalTo(0));
        assertThat(policy.rejected(), equalTo(0));

        Task first = taskManager.register("transport", TRACKED, new SearchRequest());
        assertThat(policy.current(), equalTo(1));

        Task second = taskManager.register("transport", TRACKED, new SearchRequest());
        assertThat(policy.current(), equalTo(2));

        taskManager.unregister(first);
        assertThat(policy.current(), equalTo(1));

        taskManager.unregister(second);
        assertThat(policy.current(), equalTo(0));
    }

    public void testUntrackedActionIsIgnored() {
        TaskAdmissionPolicy policy = new TaskAdmissionPolicy(Set.of(TRACKED), 100);
        TaskManager taskManager = taskManager(policy);

        Task task = taskManager.register("transport", UNTRACKED, new SearchRequest());
        assertThat(policy.current(), equalTo(0));
        assertThat(policy.rejected(), equalTo(0));
        taskManager.unregister(task);
        assertThat(policy.current(), equalTo(0));
        assertThat(policy.rejected(), equalTo(0));
    }

    public void testConcurrentRegisterAndUnregister() throws Exception {
        TaskAdmissionPolicy policy = new TaskAdmissionPolicy(Set.of(TRACKED), 100);
        TaskManager taskManager = taskManager(policy);
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
                        Task task = taskManager.register("transport", TRACKED, new SearchRequest());
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

    public void testRegisterRejectsAtLimit() {
        TaskAdmissionPolicy policy = new TaskAdmissionPolicy(Set.of(TRACKED), 1);
        TaskManager taskManager = taskManager(policy);

        Task task = taskManager.register("transport", TRACKED, new SearchRequest());
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(0));

        EsRejectedExecutionException rejected = expectThrows(
            EsRejectedExecutionException.class,
            () -> taskManager.register("transport", TRACKED, new SearchRequest())
        );
        assertThat(rejected.getMessage(), containsString("limit [1]"));
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(1));
        assertThat(taskManager.getCancellableTasks().size(), equalTo(1));

        expectThrows(EsRejectedExecutionException.class, () -> taskManager.register("transport", TRACKED, new SearchRequest()));
        assertThat(policy.rejected(), equalTo(2));

        Task untracked = taskManager.register("transport", UNTRACKED, new SearchRequest());
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(2));
        taskManager.unregister(untracked);

        taskManager.unregister(task);
        assertThat(policy.current(), equalTo(0));
        assertThat(policy.rejected(), equalTo(2));
        Task again = taskManager.register("transport", TRACKED, new SearchRequest());
        assertThat(policy.current(), equalTo(1));
        assertThat(policy.rejected(), equalTo(2));
        taskManager.unregister(again);
    }

    public void testNoopDoesNotCountOrReject() {
        TaskManager taskManager = taskManager(TaskAdmissionPolicy.NOOP);
        Task first = taskManager.register("transport", TRACKED, new SearchRequest());
        Task second = taskManager.register("transport", TRACKED, new SearchRequest());
        assertThat(TaskAdmissionPolicy.NOOP.current(), equalTo(0));
        assertThat(TaskAdmissionPolicy.NOOP.rejected(), equalTo(0));
        assertThat(taskManager.getCancellableTasks().size(), equalTo(2));
        taskManager.unregister(first);
        taskManager.unregister(second);
    }

    public void testRegisterRejectsBeforeCreatingTask() {
        TaskAdmissionPolicy policy = new TaskAdmissionPolicy(Set.of(TRACKED), 0);
        TaskManager taskManager = taskManager(policy);

        expectThrows(EsRejectedExecutionException.class, () -> taskManager.register("transport", TRACKED, new SearchRequest()));
        assertThat(taskManager.getTasks().isEmpty(), equalTo(true));
        assertThat(policy.current(), equalTo(0));
        assertThat(policy.rejected(), equalTo(1));
    }
}
