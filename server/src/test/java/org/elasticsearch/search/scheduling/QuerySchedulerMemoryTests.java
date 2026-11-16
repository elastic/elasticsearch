/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.PreallocatedCircuitBreakerService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.tasks.RemovedTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The lifecycle of the queries a {@link QueryScheduler} tracks, and the memory they account.
 */
public class QuerySchedulerMemoryTests extends ESTestCase {

    private final CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofKb(100));
    private final CircuitBreaker request = breakerService.getBreaker(CircuitBreaker.REQUEST);
    private final QueryScheduler scheduler = new QueryScheduler(System::nanoTime, request);
    private final RemovedTaskListener removed = scheduler.removedTaskListener();
    private final long owner = 1;

    /**
     * Every byte a query charges is on the request breaker too. Its peak is the most it held at once.
     */
    public void testChargesTheRequestBreaker() {
        try (var part = scheduler.part(queryId("q"), owner)) {
            CircuitBreaker breaker = part.breaker();
            breaker.addEstimateBytesAndMaybeBreak(100, "test");
            breaker.addWithoutBreaking(50);
            assertThat(part.memoryBytes(), equalTo(150L));
            assertThat(request.getUsed(), equalTo(150L));

            breaker.addWithoutBreaking(-120, "test");
            breaker.addEstimateBytesAndMaybeBreak(20, "test");
            assertThat(part.memoryBytes(), equalTo(50L));
            assertThat(request.getUsed(), equalTo(50L));
            assertThat(part.peakMemoryBytes(), equalTo(150L));
            breaker.addWithoutBreaking(-50);
        }
        assertThat(request.getUsed(), equalTo(0L));
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * The breaker of a query reads as the request breaker, so a check that projects a charge, like the one on query
     * construction, sees the whole node. The scheduler reads the bytes of each query.
     */
    public void testReadsAsTheRequestBreaker() {
        HierarchyCircuitBreakerService node = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100kb").build(),
            List.of(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        CircuitBreaker nodeRequest = node.getBreaker(CircuitBreaker.REQUEST);
        QueryScheduler onNode = new QueryScheduler(System::nanoTime, nodeRequest);
        try (var part = onNode.part(queryId("q"), owner); var other = onNode.part(queryId("other"), owner)) {
            CircuitBreaker breaker = part.breaker();
            other.breaker().addEstimateBytesAndMaybeBreak(30_000, "test");
            breaker.addEstimateBytesAndMaybeBreak(1000, "test");
            assertThat(breaker.getName(), equalTo(nodeRequest.getName()));
            assertThat(breaker.getUsed(), equalTo(nodeRequest.getUsed()));
            assertThat(breaker.getLimit(), equalTo(nodeRequest.getLimit()));
            assertThat(onNode.memoryBytes(queryId("q")), equalTo(1000L));
            assertThat(onNode.memoryBytes(queryId("other")), equalTo(30_000L));
            assertThat(onNode.memoryBytes(queryId("none")), equalTo(0L));
            expectThrows(CircuitBreakingException.class, () -> breaker.circuitBreak("query", nodeRequest.getLimit() + 1));

            breaker.addWithoutBreaking(-1000);
            other.breaker().addWithoutBreaking(-30_000);
        }
        assertThat(nodeRequest.getUsed(), equalTo(0L));
    }

    /**
     * A charge the request breaker rejects is not counted for the query.
     */
    public void testRejectedChargeIsNotCounted() {
        try (var part = scheduler.part(queryId("q"), owner)) {
            CircuitBreaker breaker = part.breaker();
            breaker.addEstimateBytesAndMaybeBreak(1024, "test");
            expectThrows(
                CircuitBreakingException.class,
                () -> breaker.addEstimateBytesAndMaybeBreak(ByteSizeValue.ofKb(200).getBytes(), "test")
            );
            assertThat(part.memoryBytes(), equalTo(1024L));
            assertThat(request.getUsed(), equalTo(1024L));
            assertThat(part.peakMemoryBytes(), equalTo(1024L));
            breaker.addWithoutBreaking(-1024);
        }
    }

    /**
     * The parts of a query on a node share one breaker.
     */
    public void testPartsOfAQueryShareItsBreaker() {
        var coordination = scheduler.part(queryId("q"), owner);
        var context = scheduler.part(queryId("q"), 2);
        var other = scheduler.part(queryId("other"), owner);
        assertThat(context.breaker(), sameInstance(coordination.breaker()));
        assertThat(other.breaker(), not(sameInstance(coordination.breaker())));
        assertThat(scheduler.queries(), equalTo(2));

        coordination.breaker().addWithoutBreaking(10);
        context.breaker().addWithoutBreaking(20);
        other.breaker().addWithoutBreaking(5);
        assertThat(coordination.peakMemoryBytes(), equalTo(30L));
        assertThat(context.peakMemoryBytes(), equalTo(30L));
        assertThat(other.peakMemoryBytes(), equalTo(5L));
        assertThat(request.getUsed(), equalTo(35L));
        context.breaker().addWithoutBreaking(-30);
        other.breaker().addWithoutBreaking(-5);

        coordination.close();
        coordination.close();
        assertThat(scheduler.queries(), equalTo(2));
        context.close();
        other.close();
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * A query stays tracked while a task it submitted runs, after its last part closed.
     */
    public void testTasksKeepTheQueryTracked() {
        List<Runnable> pool = new ArrayList<>();
        var part = scheduler.part(queryId("q"), owner);
        scheduler.executor(queryId("q"), pool::add, List.of()).execute(() -> {});
        part.close();
        assertThat(scheduler.queries(), equalTo(1));
        pool.removeFirst().run();
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * Removing a task closes the parts it owns, so a part its engine fails to close does not keep its query.
     */
    public void testRemovingTheOwnerClosesItsParts() {
        long otherOwner = 2;
        var first = scheduler.part(queryId("q"), owner);
        var second = scheduler.part(queryId("q"), owner);
        var unrelated = scheduler.part(queryId("other"), otherOwner);
        assertThat(scheduler.queries(), equalTo(2));

        removed.onRemoved(task(3));
        assertThat(scheduler.queries(), equalTo(2));

        removed.onRemoved(task(owner));
        assertThat(scheduler.queries(), equalTo(1));
        assertTrue(first.isClosed());
        assertTrue(second.isClosed());
        assertFalse(unrelated.isClosed());
        first.close();

        unrelated.close();
        removed.onRemoved(task(otherOwner));
        assertThat(scheduler.queries(), equalTo(0));
        assertTrue(scheduler.ownedParts().isEmpty());
    }

    /**
     * A part closed before its owner is removed is no longer owned.
     */
    public void testClosedPartsAreNoLongerOwned() {
        scheduler.part(queryId("q"), owner).close();
        scheduler.part(queryId("other"), owner).close();
        assertTrue(scheduler.ownedParts().isEmpty());
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * A query stays tracked while it holds memory, after its last part closed. Its next part shares its breaker. Releasing
     * the memory forgets the query.
     */
    public void testMemoryKeepsTheQueryTracked() {
        CircuitBreaker breaker;
        try (var part = scheduler.part(queryId("q"), owner)) {
            breaker = part.breaker();
            breaker.addEstimateBytesAndMaybeBreak(64, "test");
        }
        assertThat(scheduler.queries(), equalTo(1));
        try (var part = scheduler.part(queryId("q"), owner)) {
            assertThat(part.breaker(), sameInstance(breaker));
            assertThat(part.peakMemoryBytes(), equalTo(64L));
        }
        assertThat(scheduler.queries(), equalTo(1));

        breaker.addWithoutBreaking(-64);
        assertThat(scheduler.queries(), equalTo(0));
        assertThat(request.getUsed(), equalTo(0L));
    }

    /**
     * BigArrays allocated through the breaker service of a part charge the query.
     */
    public void testBigArraysChargeTheQuery() {
        BigArrays nodeBigArrays = new BigArrays(null, breakerService, CircuitBreaker.REQUEST).withCircuitBreaking();
        try (var part = scheduler.part(queryId("q"), owner)) {
            BigArrays bigArrays = nodeBigArrays.withBreakerService(part.breakerService(breakerService));
            try (LongArray array = bigArrays.newLongArray(1000)) {
                array.set(0, 1);
                assertThat(part.memoryBytes(), greaterThan(0L));
                assertThat(part.memoryBytes(), equalTo(request.getUsed()));
            }
            assertThat(part.memoryBytes(), equalTo(0L));
            assertThat(part.peakMemoryBytes(), greaterThan(0L));
        }
        assertThat(request.getUsed(), equalTo(0L));
    }

    /**
     * Closing a used-up preallocation moves its bytes between categories of the node's request breaker. It never
     * releases them, not even briefly.
     */
    public void testPreallocationOnTheNodeBreaker() {
        HierarchyCircuitBreakerService node = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            List.of(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        CircuitBreaker nodeRequest = node.getBreaker(CircuitBreaker.REQUEST);
        QueryScheduler onNode = new QueryScheduler(System::nanoTime, nodeRequest);
        try (var part = onNode.part(queryId("q"), owner)) {
            var preallocated = new PreallocatedCircuitBreakerService(part.breakerService(node), CircuitBreaker.REQUEST, 1024, "aggs");
            CircuitBreaker breaker = preallocated.getBreaker(CircuitBreaker.REQUEST);
            breaker.addEstimateBytesAndMaybeBreak(4096, "aggs");
            assertThat(part.memoryBytes(), equalTo(4096L));
            breaker.addWithoutBreaking(-4096);
            preallocated.close();
            assertThat(part.memoryBytes(), equalTo(0L));
            assertThat(nodeRequest.getUsed(), equalTo(0L));
        }
    }

    /**
     * Concurrent charges and releases keep the query and the request breaker aligned.
     */
    public void testConcurrentCharges() throws Exception {
        int threads = between(2, 8);
        int iterations = between(100, 1000);
        try (var part = scheduler.part(queryId("q"), owner)) {
            CircuitBreaker breaker = part.breaker();
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                Thread worker = new Thread(() -> {
                    safeAwait(barrier);
                    for (int i = 0; i < iterations; i++) {
                        breaker.addEstimateBytesAndMaybeBreak(8, "test");
                        breaker.addWithoutBreaking(-8);
                    }
                });
                workers.add(worker);
                worker.start();
            }
            for (Thread worker : workers) {
                worker.join();
            }
            assertThat(part.memoryBytes(), equalTo(0L));
            assertThat(request.getUsed(), equalTo(0L));
            assertThat(part.peakMemoryBytes(), greaterThan(0L));
        }
    }

    /**
     * Parts opened and closed concurrently, by their engines or by removing their owner, leave no query tracked.
     */
    public void testConcurrentLifecycle() throws Exception {
        int threads = between(2, 8);
        int iterations = between(100, 1000);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int thread = t;
            Thread worker = new Thread(() -> {
                safeAwait(barrier);
                for (int i = 0; i < iterations; i++) {
                    Task task = task(thread * iterations + i);
                    var part = scheduler.part(queryId("q" + (i % 3)), task.getId());
                    part.breaker().addWithoutBreaking(1);
                    part.breaker().addWithoutBreaking(-1);
                    if (randomBoolean()) {
                        part.close();
                    }
                    removed.onRemoved(task);
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertThat(scheduler.queries(), equalTo(0));
        assertTrue(scheduler.ownedParts().isEmpty());
        assertThat(request.getUsed(), equalTo(0L));
    }

    private static Task task(long id) {
        return new Task(id, "transport", "action", "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static TaskId queryId(String node) {
        return new TaskId(node, 1);
    }
}
