/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DriverContextTests extends ESTestCase {

    final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService());

    public void testEmptyFinished() {
        DriverContext driverContext = new DriverContext();
        driverContext.finish();
        assertTrue(driverContext.isFinished());
        var snapshot = driverContext.getSnapshot();
        assertThat(snapshot.releasables(), empty());
    }

    public void testAddByIdentity() {
        DriverContext driverContext = new DriverContext();
        ReleasablePoint point1 = new ReleasablePoint(1, 2);
        ReleasablePoint point2 = new ReleasablePoint(1, 2);
        assertThat(point1, equalTo(point2));
        driverContext.addReleasable(point1);
        driverContext.addReleasable(point2);
        driverContext.finish();
        assertTrue(driverContext.isFinished());
        var snapshot = driverContext.getSnapshot();
        assertThat(snapshot.releasables(), hasSize(2));
        assertThat(snapshot.releasables(), contains(point1, point2));
    }

    public void testAddFinish() {
        DriverContext driverContext = new DriverContext();
        int count = randomInt(128);
        Set<Releasable> releasables = IntStream.range(0, count).mapToObj(i -> randomReleasable()).collect(toIdentitySet());
        assertThat(releasables, hasSize(count));

        releasables.forEach(driverContext::addReleasable);
        driverContext.finish();
        var snapshot = driverContext.getSnapshot();
        assertThat(snapshot.releasables(), hasSize(count));
        assertThat(snapshot.releasables(), containsInAnyOrder(releasables.toArray()));
        assertTrue(driverContext.isFinished());
        releasables.forEach(Releasable::close);
        releasables.stream().filter(o -> CheckableReleasable.class.isAssignableFrom(o.getClass())).forEach(Releasable::close);
    }

    public void testRemoveAbsent() {
        DriverContext driverContext = new DriverContext();
        boolean removed = driverContext.removeReleasable(new NoOpReleasable());
        assertThat(removed, equalTo(false));
        driverContext.finish();
        assertTrue(driverContext.isFinished());
        var snapshot = driverContext.getSnapshot();
        assertThat(snapshot.releasables(), empty());
    }

    public void testAddRemoveFinish() {
        DriverContext driverContext = new DriverContext();
        int count = randomInt(128);
        Set<Releasable> releasables = IntStream.range(0, count).mapToObj(i -> randomReleasable()).collect(toIdentitySet());
        assertThat(releasables, hasSize(count));

        releasables.forEach(driverContext::addReleasable);
        releasables.forEach(driverContext::removeReleasable);
        driverContext.finish();
        var snapshot = driverContext.getSnapshot();
        assertThat(snapshot.releasables(), empty());
        assertTrue(driverContext.isFinished());
        releasables.forEach(Releasable::close);
    }

    public void testMultiThreaded() throws Exception {
        ExecutorService executor = threadPool.executor("esql_test_executor");

        int tasks = randomIntBetween(4, 32);
        List<TestDriver> testDrivers = IntStream.range(0, tasks)
            .mapToObj(i -> new TestDriver(new AssertingDriverContext(), randomInt(128), bigArrays))
            .toList();
        List<Future<Void>> futures = executor.invokeAll(testDrivers, 1, TimeUnit.MINUTES);
        assertThat(futures, hasSize(tasks));
        for (var fut : futures) {
            fut.get(); // ensures that all completed without an error
        }

        int expectedTotal = testDrivers.stream().mapToInt(TestDriver::numReleasables).sum();
        List<Set<Releasable>> finishedReleasables = testDrivers.stream()
            .map(TestDriver::driverContext)
            .map(DriverContext::getSnapshot)
            .map(DriverContext.Snapshot::releasables)
            .toList();
        assertThat(finishedReleasables.stream().mapToInt(Set::size).sum(), equalTo(expectedTotal));
        assertThat(
            testDrivers.stream().map(TestDriver::driverContext).map(DriverContext::isFinished).anyMatch(b -> b == false),
            equalTo(false)
        );
        finishedReleasables.stream().flatMap(Set::stream).forEach(Releasable::close);
    }

    static class AssertingDriverContext extends DriverContext {
        volatile Thread thread;

        @Override
        public boolean addReleasable(Releasable releasable) {
            checkThread();
            return super.addReleasable(releasable);
        }

        @Override
        public boolean removeReleasable(Releasable releasable) {
            checkThread();
            return super.removeReleasable(releasable);
        }

        @Override
        public Snapshot getSnapshot() {
            // can be called by either the Driver thread or the runner thread, but typically the runner
            return super.getSnapshot();
        }

        @Override
        public boolean isFinished() {
            // can be called by either the Driver thread or the runner thread
            return super.isFinished();
        }

        public void finish() {
            checkThread();
            super.finish();
        }

        void checkThread() {
            if (thread == null) {
                thread = Thread.currentThread();
            }
            assertThat(thread, equalTo(Thread.currentThread()));
        }

    }

    record TestDriver(DriverContext driverContext, int numReleasables, BigArrays bigArrays) implements Callable<Void> {
        @Override
        public Void call() {
            int extraToAdd = randomInt(16);
            Set<Releasable> releasables = IntStream.range(0, numReleasables + extraToAdd)
                .mapToObj(i -> randomReleasable(bigArrays))
                .collect(toIdentitySet());
            assertThat(releasables, hasSize(numReleasables + extraToAdd));
            Set<Releasable> toRemove = randomNFromCollection(releasables, extraToAdd);
            for (var r : releasables) {
                driverContext.addReleasable(r);
                if (toRemove.contains(r)) {
                    driverContext.removeReleasable(r);
                    r.close();
                }
            }
            assertThat(driverContext.workingSet, hasSize(numReleasables));
            driverContext.finish();
            return null;
        }
    }

    // Selects a number of random elements, n, from the given Set.
    static <T> Set<T> randomNFromCollection(Set<T> input, int n) {
        final int size = input.size();
        if (n < 0 || n > size) {
            throw new IllegalArgumentException(n + " is out of bounds for set of size:" + size);
        }
        if (n == size) {
            return input;
        }
        Set<T> result = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<Integer> selected = new HashSet<>();
        while (selected.size() < n) {
            int idx = randomValueOtherThanMany(selected::contains, () -> randomInt(size - 1));
            selected.add(idx);
            result.add(input.stream().skip(idx).findFirst().get());
        }
        assertThat(result.size(), equalTo(n));
        assertTrue(input.containsAll(result));
        return result;
    }

    Releasable randomReleasable() {
        return randomReleasable(bigArrays);
    }

    static Releasable randomReleasable(BigArrays bigArrays) {
        return switch (randomInt(3)) {
            case 0 -> new NoOpReleasable();
            case 1 -> new ReleasablePoint(1, 2);
            case 2 -> new CheckableReleasable();
            case 3 -> bigArrays.newLongArray(32, false);
            default -> throw new AssertionError();
        };
    }

    record ReleasablePoint(int x, int y) implements Releasable {
        @Override
        public void close() {}
    }

    static class NoOpReleasable implements Releasable {

        @Override
        public void close() {
            // no-op
        }
    }

    static class CheckableReleasable implements Releasable {

        boolean closed;

        @Override
        public void close() {
            closed = true;
        }
    }

    static <T> Collector<T, ?, Set<T>> toIdentitySet() {
        return Collectors.toCollection(() -> Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private TestThreadPool threadPool;

    @Before
    public void setThreadPool() {
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, "esql_test_executor", numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }
}
