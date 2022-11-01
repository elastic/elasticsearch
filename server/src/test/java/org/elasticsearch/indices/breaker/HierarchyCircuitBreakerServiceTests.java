/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;

public class HierarchyCircuitBreakerServiceTests extends ESTestCase {

    public void testThreadedUpdatesToChildBreaker() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(3, 15);
        final int BYTES_PER_THREAD = scaledRandomIntBetween(500, 4500);
        final Thread[] threads = new Thread[NUM_THREADS];
        final AtomicBoolean tripped = new AtomicBoolean(false);
        final AtomicReference<Throwable> lastException = new AtomicReference<>(null);

        final AtomicReference<ChildMemoryCircuitBreaker> breakerRef = new AtomicReference<>(null);
        final CircuitBreakerService service = new HierarchyCircuitBreakerService(
            Settings.EMPTY,
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ) {

            @Override
            public CircuitBreaker getBreaker(String name) {
                return breakerRef.get();
            }

            @Override
            public void checkParentLimit(long newBytesReserved, String label) throws CircuitBreakingException {
                // never trip
            }
        };
        final BreakerSettings settings = new BreakerSettings(CircuitBreaker.REQUEST, (BYTES_PER_THREAD * NUM_THREADS) - 1, 1.0);
        final ChildMemoryCircuitBreaker breaker = new ChildMemoryCircuitBreaker(
            settings,
            logger,
            (HierarchyCircuitBreakerService) service,
            CircuitBreaker.REQUEST
        );
        breakerRef.set(breaker);

        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < BYTES_PER_THREAD; j++) {
                    try {
                        breaker.addEstimateBytesAndMaybeBreak(1L, "test");
                    } catch (CircuitBreakingException e) {
                        if (tripped.get()) {
                            assertThat("tripped too many times", true, equalTo(false));
                        } else {
                            assertThat(tripped.compareAndSet(false, true), equalTo(true));
                        }
                    } catch (Exception e) {
                        lastException.set(e);
                    }
                }
            });

            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        assertThat("no other exceptions were thrown", lastException.get(), equalTo(null));
        assertThat("breaker was tripped", tripped.get(), equalTo(true));
        assertThat("breaker was tripped at least once", breaker.getTrippedCount(), greaterThanOrEqualTo(1L));
    }

    public void testThreadedUpdatesToChildBreakerWithParentLimit() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(3, 15);
        final int BYTES_PER_THREAD = scaledRandomIntBetween(500, 4500);
        final int parentLimit = (BYTES_PER_THREAD * NUM_THREADS) - 2;
        final int childLimit = parentLimit + 10;
        final Thread[] threads = new Thread[NUM_THREADS];
        final AtomicInteger tripped = new AtomicInteger(0);
        final AtomicReference<Throwable> lastException = new AtomicReference<>(null);

        final AtomicInteger parentTripped = new AtomicInteger(0);
        final AtomicReference<ChildMemoryCircuitBreaker> breakerRef = new AtomicReference<>(null);
        final CircuitBreakerService service = new HierarchyCircuitBreakerService(
            Settings.EMPTY,
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ) {

            @Override
            public CircuitBreaker getBreaker(String name) {
                return breakerRef.get();
            }

            @Override
            public void checkParentLimit(long newBytesReserved, String label) throws CircuitBreakingException {
                // Parent will trip right before regular breaker would trip
                long requestBreakerUsed = getBreaker(CircuitBreaker.REQUEST).getUsed();
                if (requestBreakerUsed > parentLimit) {
                    parentTripped.incrementAndGet();
                    logger.info("--> parent tripped");
                    throw new CircuitBreakingException(
                        "parent tripped",
                        requestBreakerUsed + newBytesReserved,
                        parentLimit,
                        CircuitBreaker.Durability.PERMANENT
                    );
                }
            }
        };
        final BreakerSettings settings = new BreakerSettings(CircuitBreaker.REQUEST, childLimit, 1.0);
        final ChildMemoryCircuitBreaker breaker = new ChildMemoryCircuitBreaker(
            settings,
            logger,
            (HierarchyCircuitBreakerService) service,
            CircuitBreaker.REQUEST
        );
        breakerRef.set(breaker);

        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < BYTES_PER_THREAD; j++) {
                    try {
                        breaker.addEstimateBytesAndMaybeBreak(1L, "test");
                    } catch (CircuitBreakingException e) {
                        tripped.incrementAndGet();
                    } catch (Exception e) {
                        lastException.set(e);
                    }
                }
            });
        }

        logger.info(
            "--> NUM_THREADS: [{}], BYTES_PER_THREAD: [{}], TOTAL_BYTES: [{}], PARENT_LIMIT: [{}], CHILD_LIMIT: [{}]",
            NUM_THREADS,
            BYTES_PER_THREAD,
            (BYTES_PER_THREAD * NUM_THREADS),
            parentLimit,
            childLimit
        );

        logger.info("--> starting threads...");
        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        logger.info("--> child breaker: used: {}, limit: {}", breaker.getUsed(), breaker.getLimit());
        logger.info("--> parent tripped: {}, total trip count: {} (expecting 1-2 for each)", parentTripped.get(), tripped.get());
        assertThat("no other exceptions were thrown", lastException.get(), equalTo(null));
        assertThat(
            "breaker should be reset back to the parent limit after parent breaker trips",
            breaker.getUsed(),
            greaterThanOrEqualTo((long) parentLimit - NUM_THREADS)
        );
        assertThat("parent breaker was tripped at least once", parentTripped.get(), greaterThanOrEqualTo(1));
        assertThat("total breaker was tripped at least once", tripped.get(), greaterThanOrEqualTo(1));
    }

    /**
     * Test that a breaker correctly redistributes to a different breaker, in
     * this case, the request breaker borrows space from the fielddata breaker
     */
    public void testBorrowingSiblingBreakerMemory() {
        Settings clusterSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "200mb")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "150mb")
            .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "150mb")
            .build();
        try (
            CircuitBreakerService service = new HierarchyCircuitBreakerService(
                clusterSettings,
                Collections.emptyList(),
                new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        ) {
            CircuitBreaker requestCircuitBreaker = service.getBreaker(CircuitBreaker.REQUEST);
            CircuitBreaker fieldDataCircuitBreaker = service.getBreaker(CircuitBreaker.FIELDDATA);

            assertEquals(new ByteSizeValue(200, ByteSizeUnit.MB).getBytes(), service.stats().getStats(CircuitBreaker.PARENT).getLimit());
            assertEquals(new ByteSizeValue(150, ByteSizeUnit.MB).getBytes(), requestCircuitBreaker.getLimit());
            assertEquals(new ByteSizeValue(150, ByteSizeUnit.MB).getBytes(), fieldDataCircuitBreaker.getLimit());

            fieldDataCircuitBreaker.addEstimateBytesAndMaybeBreak(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(), "should not break");
            assertEquals(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(), fieldDataCircuitBreaker.getUsed(), 0.0);
            requestCircuitBreaker.addEstimateBytesAndMaybeBreak(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(), "should not break");
            assertEquals(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(), requestCircuitBreaker.getUsed(), 0.0);
            requestCircuitBreaker.addEstimateBytesAndMaybeBreak(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(), "should not break");
            assertEquals(new ByteSizeValue(100, ByteSizeUnit.MB).getBytes(), requestCircuitBreaker.getUsed(), 0.0);
            CircuitBreakingException exception = expectThrows(
                CircuitBreakingException.class,
                () -> requestCircuitBreaker.addEstimateBytesAndMaybeBreak(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(), "should break")
            );
            assertThat(exception.getMessage(), containsString("[parent] Data too large, data for [should break] would be"));
            assertThat(exception.getMessage(), containsString("which is larger than the limit of [209715200/200mb]"));
            assertThat(exception.getMessage(), containsString("usages ["));
            assertThat(exception.getMessage(), containsString("fielddata=54001664/51.5mb"));
            assertThat(exception.getMessage(), containsString("inflight_requests=0/0b"));
            assertThat(exception.getMessage(), containsString("request=157286400/150mb"));
            assertThat(exception.getDurability(), equalTo(CircuitBreaker.Durability.TRANSIENT));
        }
    }

    public void testParentBreaksOnRealMemoryUsage() {
        Settings clusterSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), Boolean.TRUE)
            .put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "200b")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "350b")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 2)
            .build();

        AtomicLong memoryUsage = new AtomicLong();
        final CircuitBreakerService service = new HierarchyCircuitBreakerService(
            clusterSettings,
            Collections.emptyList(),
            new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ) {
            @Override
            long currentMemoryUsage() {
                return memoryUsage.get();
            }
        };
        final CircuitBreaker requestBreaker = service.getBreaker(CircuitBreaker.REQUEST);

        // anything below 100 bytes should work (overhead) - current memory usage is zero
        requestBreaker.addEstimateBytesAndMaybeBreak(randomLongBetween(0, 99), "request");
        assertEquals(0, requestBreaker.getTrippedCount());
        // assume memory usage has increased to 150 bytes
        memoryUsage.set(150);

        // a reservation that bumps memory usage to less than 200 (150 bytes used + reservation < 200)
        requestBreaker.addEstimateBytesAndMaybeBreak(randomLongBetween(0, 24), "request");
        assertEquals(0, requestBreaker.getTrippedCount());
        memoryUsage.set(181);

        long reservationInBytes = randomLongBetween(10, 50);
        // anything >= 20 bytes (10 bytes * 2 overhead) reservation breaks the parent but it must be low enough to avoid
        // breaking the child breaker.
        CircuitBreakingException exception = expectThrows(
            CircuitBreakingException.class,
            () -> requestBreaker.addEstimateBytesAndMaybeBreak(reservationInBytes, "request")
        );
        // it was the parent that rejected the reservation
        assertThat(exception.getMessage(), containsString("[parent] Data too large, data for [request] would be"));
        assertThat(exception.getMessage(), containsString("which is larger than the limit of [200/200b]"));
        assertThat(
            exception.getMessage(),
            containsString(
                "real usage: [181/181b], new bytes reserved: ["
                    + (reservationInBytes * 2)
                    + "/"
                    + ByteSizeValue.ofBytes(reservationInBytes * 2)
                    + "]"
            )
        );
        final long requestCircuitBreakerUsed = (requestBreaker.getUsed() + reservationInBytes) * 2;
        assertThat(exception.getMessage(), containsString("usages ["));
        assertThat(exception.getMessage(), containsString("fielddata=0/0b"));
        assertThat(
            exception.getMessage(),
            containsString("request=" + requestCircuitBreakerUsed + "/" + ByteSizeValue.ofBytes(requestCircuitBreakerUsed))
        );
        assertThat(exception.getMessage(), containsString("inflight_requests=0/0b"));
        assertThat(exception.getDurability(), equalTo(CircuitBreaker.Durability.TRANSIENT));
        assertEquals(0, requestBreaker.getTrippedCount());
        assertEquals(1, service.stats().getStats(CircuitBreaker.PARENT).getTrippedCount());

        // lower memory usage again - the same reservation should succeed
        memoryUsage.set(100);
        requestBreaker.addEstimateBytesAndMaybeBreak(reservationInBytes, "request");
        assertEquals(0, requestBreaker.getTrippedCount());
    }

    /**
     * "Integration test" checking that we ask the G1 over limit check before parent breaking.
     * Given that it depends on GC, the main assertion that we do not get a circuit breaking exception in the threads towards
     * the end of the test is not enabled. The following tests checks this in more unit test style.
     */
    public void testParentTriggersG1GCBeforeBreaking() throws InterruptedException, TimeoutException, BrokenBarrierException {
        assumeTrue("Only G1GC can utilize the over limit check", JvmInfo.jvmInfo().useG1GC().equals("true"));
        long g1RegionSize = JvmInfo.jvmInfo().getG1RegionSize();
        assumeTrue("Must have region size", g1RegionSize > 0);

        Settings clusterSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), Boolean.TRUE)
            .put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "50%")
            .build();

        AtomicInteger leaderTriggerCount = new AtomicInteger();
        AtomicReference<Consumer<Boolean>> onOverLimit = new AtomicReference<>(leader -> {});
        AtomicLong time = new AtomicLong(randomLongBetween(Long.MIN_VALUE / 2, Long.MAX_VALUE / 2));
        long interval = randomLongBetween(1, 1000);
        final HierarchyCircuitBreakerService service = new HierarchyCircuitBreakerService(
            clusterSettings,
            Collections.emptyList(),
            new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            trackRealMemoryUsage -> new HierarchyCircuitBreakerService.G1OverLimitStrategy(
                JvmInfo.jvmInfo(),
                HierarchyCircuitBreakerService::realMemoryUsage,
                HierarchyCircuitBreakerService.createYoungGcCountSupplier(),
                time::get,
                interval,
                TimeValue.timeValueSeconds(30)
            ) {

                @Override
                void overLimitTriggered(boolean leader) {
                    if (leader) {
                        leaderTriggerCount.incrementAndGet();
                    }
                    onOverLimit.get().accept(leader);
                }
            }
        );

        long maxHeap = JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        int regionCount = Math.toIntExact((maxHeap / 2 + g1RegionSize - 1) / g1RegionSize);

        // First setup a host of large byte[]'s, must be Humongous objects since those are cleaned during a young phase (no concurrent cycle
        // necessary, which is hard to control in the test).
        List<byte[]> data = new ArrayList<>();
        for (int i = 0; i < regionCount; ++i) {
            data.add(new byte[(int) (JvmInfo.jvmInfo().getG1RegionSize() / 2)]);
        }
        try {
            service.checkParentLimit(0, "test");
            fail("must exceed memory limit");
        } catch (CircuitBreakingException e) {
            // OK
        }

        time.addAndGet(randomLongBetween(interval, interval + 10));
        onOverLimit.set(leader -> {
            if (leader) {
                data.clear();
            }
        });

        logger.trace("black hole [{}]", data.hashCode());

        int threadCount = randomIntBetween(1, 10);
        CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
        List<Thread> threads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; ++i) {
            threads.add(new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    service.checkParentLimit(0, "test-thread");
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new AssertionError(e);
                } catch (CircuitBreakingException e) {
                    // very rare
                    logger.info("Thread got semi-unexpected circuit breaking exception", e);
                }
            }));
        }

        threads.forEach(Thread::start);
        barrier.await(20, TimeUnit.SECONDS);

        for (Thread thread : threads) {
            thread.join(10000);
        }
        threads.forEach(thread -> assertFalse(thread.isAlive()));

        assertThat(leaderTriggerCount.get(), equalTo(2));
    }

    public void testParentDoesOverLimitCheck() {
        long g1RegionSize = JvmInfo.jvmInfo().getG1RegionSize();

        Settings clusterSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), Boolean.TRUE)
            .put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "50%")
            .build();
        boolean saveTheDay = randomBoolean();
        AtomicBoolean overLimitTriggered = new AtomicBoolean();
        final HierarchyCircuitBreakerService service = new HierarchyCircuitBreakerService(
            clusterSettings,
            Collections.emptyList(),
            new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            trackRealMemoryUsage -> memoryUsed -> {
                assertTrue(overLimitTriggered.compareAndSet(false, true));
                if (saveTheDay) {
                    return new HierarchyCircuitBreakerService.MemoryUsage(
                        memoryUsed.baseUsage / 2,
                        memoryUsed.totalUsage - (memoryUsed.baseUsage / 2),
                        memoryUsed.transientChildUsage,
                        memoryUsed.permanentChildUsage
                    );
                } else {
                    return memoryUsed;
                }
            }
        );

        int allocationSize = g1RegionSize > 0 ? (int) (g1RegionSize / 2) : 1024 * 1024;
        int allocationCount = (int) (JvmInfo.jvmInfo().getConfiguredMaxHeapSize() / allocationSize) + 1;
        List<byte[]> data = new ArrayList<>();
        try {
            for (int i = 0; i < allocationCount && overLimitTriggered.get() == false; ++i) {
                data.add(new byte[allocationSize]);
                service.checkParentLimit(0, "test");
            }
            assertTrue(saveTheDay);
        } catch (CircuitBreakingException e) {
            assertFalse(saveTheDay);
        }

        logger.trace("black hole [{}]", data.hashCode());
    }

    public void testFallbackG1RegionSize() {
        assumeTrue("Only G1GC can utilize the over limit check", JvmInfo.jvmInfo().useG1GC().equals("true"));
        assumeTrue("Must have region size", JvmInfo.jvmInfo().getG1RegionSize() > 0);

        assertThat(
            HierarchyCircuitBreakerService.G1OverLimitStrategy.fallbackRegionSize(JvmInfo.jvmInfo()),
            equalTo(JvmInfo.jvmInfo().getG1RegionSize())
        );
    }

    public void testG1OverLimitStrategyBreakOnMemory() {
        AtomicLong time = new AtomicLong(randomLongBetween(Long.MIN_VALUE / 2, Long.MAX_VALUE / 2));
        AtomicInteger leaderTriggerCount = new AtomicInteger();
        AtomicInteger nonLeaderTriggerCount = new AtomicInteger();
        long interval = randomLongBetween(1, 1000);
        AtomicLong memoryUsage = new AtomicLong();

        HierarchyCircuitBreakerService.G1OverLimitStrategy strategy = new HierarchyCircuitBreakerService.G1OverLimitStrategy(
            JvmInfo.jvmInfo(),
            memoryUsage::get,
            () -> 0,
            time::get,
            interval,
            TimeValue.timeValueSeconds(30)
        ) {
            @Override
            void overLimitTriggered(boolean leader) {
                if (leader) {
                    leaderTriggerCount.incrementAndGet();
                } else {
                    nonLeaderTriggerCount.incrementAndGet();
                }
            }
        };
        memoryUsage.set(randomLongBetween(100, 110));
        HierarchyCircuitBreakerService.MemoryUsage input = new HierarchyCircuitBreakerService.MemoryUsage(
            100,
            randomLongBetween(100, 110),
            randomLongBetween(0, 50),
            randomLongBetween(0, 50)
        );

        assertThat(strategy.overLimit(input), sameInstance(input));
        assertThat(leaderTriggerCount.get(), equalTo(1));

        memoryUsage.set(99);
        HierarchyCircuitBreakerService.MemoryUsage output = strategy.overLimit(input);
        assertThat(output, not(sameInstance(input)));
        assertThat(output.baseUsage, equalTo(memoryUsage.get()));
        assertThat(output.totalUsage, equalTo(99 + input.totalUsage - 100));
        assertThat(output.transientChildUsage, equalTo(input.transientChildUsage));
        assertThat(output.permanentChildUsage, equalTo(input.permanentChildUsage));
        assertThat(nonLeaderTriggerCount.get(), equalTo(1));

        time.addAndGet(randomLongBetween(interval, interval * 2));
        output = strategy.overLimit(input);
        assertThat(output, not(sameInstance(input)));
        assertThat(output.baseUsage, equalTo(memoryUsage.get()));
        assertThat(output.totalUsage, equalTo(99 + input.totalUsage - 100));
        assertThat(output.transientChildUsage, equalTo(input.transientChildUsage));
        assertThat(output.permanentChildUsage, equalTo(input.permanentChildUsage));
        assertThat(leaderTriggerCount.get(), equalTo(2));
    }

    public void testG1OverLimitStrategyBreakOnGcCount() {
        AtomicLong time = new AtomicLong(randomLongBetween(Long.MIN_VALUE / 2, Long.MAX_VALUE / 2));
        AtomicInteger leaderTriggerCount = new AtomicInteger();
        AtomicInteger nonLeaderTriggerCount = new AtomicInteger();
        long interval = randomLongBetween(1, 1000);
        AtomicLong memoryUsageCounter = new AtomicLong();
        AtomicLong gcCounter = new AtomicLong();
        LongSupplier memoryUsageSupplier = () -> {
            memoryUsageCounter.incrementAndGet();
            return randomLongBetween(100, 110);
        };
        HierarchyCircuitBreakerService.G1OverLimitStrategy strategy = new HierarchyCircuitBreakerService.G1OverLimitStrategy(
            JvmInfo.jvmInfo(),
            memoryUsageSupplier,
            gcCounter::incrementAndGet,
            time::get,
            interval,
            TimeValue.timeValueSeconds(30)
        ) {

            @Override
            void overLimitTriggered(boolean leader) {
                if (leader) {
                    leaderTriggerCount.incrementAndGet();
                } else {
                    nonLeaderTriggerCount.incrementAndGet();
                }
            }
        };
        HierarchyCircuitBreakerService.MemoryUsage input = new HierarchyCircuitBreakerService.MemoryUsage(
            100,
            randomLongBetween(100, 110),
            randomLongBetween(0, 50),
            randomLongBetween(0, 50)
        );

        assertThat(strategy.overLimit(input), sameInstance(input));
        assertThat(leaderTriggerCount.get(), equalTo(1));
        assertThat(gcCounter.get(), equalTo(2L));
        assertThat(memoryUsageCounter.get(), equalTo(2L)); // 1 before gc count break and 1 to get resulting memory usage.
    }

    public void testG1OverLimitStrategyThrottling() throws InterruptedException, BrokenBarrierException, TimeoutException {
        AtomicLong time = new AtomicLong(randomLongBetween(Long.MIN_VALUE / 2, Long.MAX_VALUE / 2));
        AtomicInteger leaderTriggerCount = new AtomicInteger();
        long interval = randomLongBetween(1, 1000);
        AtomicLong memoryUsage = new AtomicLong();
        HierarchyCircuitBreakerService.G1OverLimitStrategy strategy = new HierarchyCircuitBreakerService.G1OverLimitStrategy(
            JvmInfo.jvmInfo(),
            memoryUsage::get,
            () -> 0,
            time::get,
            interval,
            TimeValue.timeValueSeconds(30)
        ) {

            @Override
            void overLimitTriggered(boolean leader) {
                if (leader) {
                    leaderTriggerCount.incrementAndGet();
                }
            }
        };

        int threadCount = randomIntBetween(1, 10);
        CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
        AtomicReference<CountDownLatch> countDown = new AtomicReference<>(new CountDownLatch(randomIntBetween(1, 20)));
        List<Thread> threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread(() -> {
            try {
                barrier.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new AssertionError(e);
            }
            do {
                HierarchyCircuitBreakerService.MemoryUsage input = new HierarchyCircuitBreakerService.MemoryUsage(
                    randomLongBetween(0, 100),
                    randomLongBetween(0, 100),
                    randomLongBetween(0, 100),
                    randomLongBetween(0, 100)
                );
                HierarchyCircuitBreakerService.MemoryUsage output = strategy.overLimit(input);
                assertThat(output.totalUsage, equalTo(output.baseUsage + input.totalUsage - input.baseUsage));
                assertThat(output.transientChildUsage, equalTo(input.transientChildUsage));
                assertThat(output.permanentChildUsage, equalTo(input.permanentChildUsage));
                countDown.get().countDown();
            } while (Thread.interrupted() == false);
        })).toList();

        threads.forEach(Thread::start);
        barrier.await(20, TimeUnit.SECONDS);

        int iterationCount = randomIntBetween(1, 5);
        for (int i = 0; i < iterationCount; ++i) {
            memoryUsage.set(randomLongBetween(0, 100));
            assertTrue(countDown.get().await(20, TimeUnit.SECONDS));
            assertThat(leaderTriggerCount.get(), lessThanOrEqualTo(i + 1));
            assertThat(leaderTriggerCount.get(), greaterThanOrEqualTo(i / 2 + 1));
            time.addAndGet(randomLongBetween(interval, interval * 2));
            countDown.set(new CountDownLatch(randomIntBetween(1, 20)));
        }

        threads.forEach(Thread::interrupt);
        for (Thread thread : threads) {
            thread.join(10000);
        }
        threads.forEach(thread -> assertFalse(thread.isAlive()));
    }

    public void testCreateOverLimitStrategy() {
        assertThat(
            HierarchyCircuitBreakerService.createOverLimitStrategy(false),
            not(instanceOf(HierarchyCircuitBreakerService.G1OverLimitStrategy.class))
        );
        HierarchyCircuitBreakerService.OverLimitStrategy overLimitStrategy = HierarchyCircuitBreakerService.createOverLimitStrategy(true);
        if (JvmInfo.jvmInfo().useG1GC().equals("true")) {
            assertThat(overLimitStrategy, instanceOf(HierarchyCircuitBreakerService.G1OverLimitStrategy.class));
            assertThat(
                ((HierarchyCircuitBreakerService.G1OverLimitStrategy) overLimitStrategy).getLockTimeout(),
                equalTo(TimeValue.timeValueMillis(500))
            );
        } else {
            assertThat(overLimitStrategy, not(instanceOf(HierarchyCircuitBreakerService.G1OverLimitStrategy.class)));
        }
    }

    public void testG1LockTimeout() throws Exception {
        CountDownLatch startedBlocking = new CountDownLatch(1);
        CountDownLatch blockingUntil = new CountDownLatch(1);
        AtomicLong gcCounter = new AtomicLong();
        HierarchyCircuitBreakerService.G1OverLimitStrategy strategy = new HierarchyCircuitBreakerService.G1OverLimitStrategy(
            JvmInfo.jvmInfo(),
            () -> 100,
            gcCounter::incrementAndGet,
            () -> 0,
            1,
            TimeValue.timeValueMillis(randomFrom(0, 5, 10))
        ) {

            @Override
            void overLimitTriggered(boolean leader) {
                if (leader) {
                    startedBlocking.countDown();
                    try {
                        // this is the central assertion - the overLimit call below should complete in a timely manner.
                        assertThat(blockingUntil.await(10, TimeUnit.SECONDS), is(true));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            }
        };

        HierarchyCircuitBreakerService.MemoryUsage input = new HierarchyCircuitBreakerService.MemoryUsage(100, 100, 0, 0);
        Thread blocker = new Thread(() -> { strategy.overLimit(input); });
        blocker.start();
        try {
            assertThat(startedBlocking.await(10, TimeUnit.SECONDS), is(true));

            // this should complete in a timely manner, verified by the assertion in the thread.
            assertThat(strategy.overLimit(input), sameInstance(input));
        } finally {
            blockingUntil.countDown();
            blocker.join(10000);
            assertThat(blocker.isAlive(), is(false));
        }
    }

    public void testTrippedCircuitBreakerDurability() {
        Settings clusterSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), Boolean.FALSE)
            .put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "200mb")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "150mb")
            .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "150mb")
            .build();
        try (
            CircuitBreakerService service = new HierarchyCircuitBreakerService(
                clusterSettings,
                Collections.emptyList(),
                new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        ) {
            CircuitBreaker requestCircuitBreaker = service.getBreaker(CircuitBreaker.REQUEST);
            CircuitBreaker fieldDataCircuitBreaker = service.getBreaker(CircuitBreaker.FIELDDATA);

            CircuitBreaker.Durability expectedDurability;
            if (randomBoolean()) {
                fieldDataCircuitBreaker.addEstimateBytesAndMaybeBreak(mb(100), "should not break");
                requestCircuitBreaker.addEstimateBytesAndMaybeBreak(mb(70), "should not break");
                expectedDurability = CircuitBreaker.Durability.PERMANENT;
            } else {
                fieldDataCircuitBreaker.addEstimateBytesAndMaybeBreak(mb(70), "should not break");
                requestCircuitBreaker.addEstimateBytesAndMaybeBreak(mb(120), "should not break");
                expectedDurability = CircuitBreaker.Durability.TRANSIENT;
            }

            CircuitBreakingException exception = expectThrows(
                CircuitBreakingException.class,
                () -> fieldDataCircuitBreaker.addEstimateBytesAndMaybeBreak(mb(40), "should break")
            );

            assertThat(exception.getMessage(), containsString("[parent] Data too large, data for [should break] would be"));
            assertThat(exception.getMessage(), containsString("which is larger than the limit of [209715200/200mb]"));
            assertThat(
                "Expected [" + expectedDurability + "] due to [" + exception.getMessage() + "]",
                exception.getDurability(),
                equalTo(expectedDurability)
            );
        }
    }

    public void testAllocationBucketsBreaker() {
        Settings clusterSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100b")
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), "false")
            .build();

        try (
            HierarchyCircuitBreakerService service = new HierarchyCircuitBreakerService(
                clusterSettings,
                Collections.emptyList(),
                new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        ) {

            long parentLimitBytes = service.getParentLimit();
            assertEquals(new ByteSizeValue(100, ByteSizeUnit.BYTES).getBytes(), parentLimitBytes);

            CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
            MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
                10000,
                breaker
            );

            // make sure used bytes is greater than the total circuit breaker limit
            breaker.addWithoutBreaking(200);
            // make sure that we check on the the following call
            for (int i = 0; i < 1023; i++) {
                multiBucketConsumer.accept(0);
            }
            CircuitBreakingException exception = expectThrows(CircuitBreakingException.class, () -> multiBucketConsumer.accept(1024));
            assertThat(exception.getMessage(), containsString("[parent] Data too large, data for [allocated_buckets] would be"));
            assertThat(exception.getMessage(), containsString("which is larger than the limit of [100/100b]"));
        }
    }

    public void testRegisterCustomCircuitBreakers_WithDuplicates() {
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> new HierarchyCircuitBreakerService(
                Settings.EMPTY,
                Collections.singletonList(new BreakerSettings(CircuitBreaker.FIELDDATA, 100, 1.2)),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        );
        assertThat(
            iae.getMessage(),
            containsString("More than one circuit breaker with the name [fielddata] exists. Circuit breaker names must be unique")
        );

        iae = expectThrows(
            IllegalArgumentException.class,
            () -> new HierarchyCircuitBreakerService(
                Settings.EMPTY,
                Arrays.asList(new BreakerSettings("foo", 100, 1.2), new BreakerSettings("foo", 200, 0.1)),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        );
        assertThat(
            iae.getMessage(),
            containsString("More than one circuit breaker with the name [foo] exists. Circuit breaker names must be unique")
        );
    }

    public void testCustomCircuitBreakers() {
        try (
            CircuitBreakerService service = new HierarchyCircuitBreakerService(
                Settings.EMPTY,
                Arrays.asList(new BreakerSettings("foo", 100, 1.2), new BreakerSettings("bar", 200, 0.1)),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        ) {
            assertThat(service.getBreaker("foo"), is(not(nullValue())));
            assertThat(service.getBreaker("foo").getOverhead(), equalTo(1.2));
            assertThat(service.getBreaker("foo").getLimit(), equalTo(100L));
            assertThat(service.getBreaker("bar"), is(not(nullValue())));
            assertThat(service.getBreaker("bar").getOverhead(), equalTo(0.1));
            assertThat(service.getBreaker("bar").getLimit(), equalTo(200L));
        }
    }

    private static long mb(long size) {
        return new ByteSizeValue(size, ByteSizeUnit.MB).getBytes();
    }

    public void testUpdatingUseRealMemory() {
        try (
            HierarchyCircuitBreakerService service = new HierarchyCircuitBreakerService(
                Settings.EMPTY,
                Collections.emptyList(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        ) {
            // use real memory default true
            assertTrue(service.isTrackRealMemoryUsage());
            assertThat(service.getOverLimitStrategy(), instanceOf(HierarchyCircuitBreakerService.G1OverLimitStrategy.class));

            // update use_real_memory to false
            service.updateUseRealMemorySetting(false);
            assertFalse(service.isTrackRealMemoryUsage());
            assertThat(service.getOverLimitStrategy(), not(instanceOf(HierarchyCircuitBreakerService.G1OverLimitStrategy.class)));

            // update use_real_memory to true
            service.updateUseRealMemorySetting(true);
            assertTrue(service.isTrackRealMemoryUsage());
            assertThat(service.getOverLimitStrategy(), instanceOf(HierarchyCircuitBreakerService.G1OverLimitStrategy.class));
        }
    }

    public void testApplySettingForUpdatingUseRealMemory() {
        String useRealMemoryUsageSetting = HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey();
        String totalCircuitBreakerLimitSetting = HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey();
        Settings initialSettings = Settings.builder().put(useRealMemoryUsageSetting, "true").build();
        ClusterSettings clusterSettings = new ClusterSettings(initialSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        try (
            HierarchyCircuitBreakerService service = new HierarchyCircuitBreakerService(
                Settings.EMPTY,
                Collections.emptyList(),
                clusterSettings
            )
        ) {
            // total.limit defaults to 95% of the JVM heap if use_real_memory is true
            assertEquals(
                MemorySizeValue.parseBytesSizeValueOrHeapRatio("95%", totalCircuitBreakerLimitSetting).getBytes(),
                service.getParentLimit()
            );

            // total.limit defaults to 70% of the JVM heap if use_real_memory set to false
            clusterSettings.applySettings(Settings.builder().put(useRealMemoryUsageSetting, false).build());
            assertEquals(
                MemorySizeValue.parseBytesSizeValueOrHeapRatio("70%", totalCircuitBreakerLimitSetting).getBytes(),
                service.getParentLimit()
            );

            // total.limit defaults to 70% of the JVM heap if use_real_memory set to true
            clusterSettings.applySettings(Settings.builder().put(useRealMemoryUsageSetting, true).build());
            assertEquals(
                MemorySizeValue.parseBytesSizeValueOrHeapRatio("95%", totalCircuitBreakerLimitSetting).getBytes(),
                service.getParentLimit()
            );
        }
    }

    public void testBuildParentTripMessage() {
        class TestChildCircuitBreaker extends NoopCircuitBreaker {
            private final long used;

            TestChildCircuitBreaker(long used) {
                super("child");
                this.used = used;
            }

            @Override
            public long getUsed() {
                return used;
            }

            @Override
            public double getOverhead() {
                return 1.0;
            }
        }

        assertThat(
            HierarchyCircuitBreakerService.buildParentTripMessage(
                1L,
                "test",
                new HierarchyCircuitBreakerService.MemoryUsage(2L, 3L, 4L, 5L),
                6L,
                false,
                Map.of("child", new TestChildCircuitBreaker(7L), "otherChild", new TestChildCircuitBreaker(8L))
            ),
            oneOf(
                "[parent] Data too large, data for [test] would be [3/3b], which is larger than the limit of [6/6b], "
                    + "usages [child=7/7b, otherChild=8/8b]",
                "[parent] Data too large, data for [test] would be [3/3b], which is larger than the limit of [6/6b], "
                    + "usages [otherChild=8/8b, child=7/7b]"
            )
        );

        assertThat(
            HierarchyCircuitBreakerService.buildParentTripMessage(
                1L,
                "test",
                new HierarchyCircuitBreakerService.MemoryUsage(2L, 3L, 4L, 5L),
                6L,
                true,
                Map.of()
            ),
            equalTo(
                "[parent] Data too large, data for [test] would be [3/3b], which is larger than the limit of [6/6b], "
                    + "real usage: [2/2b], new bytes reserved: [1/1b], usages []"
            )
        );

        try {
            HierarchyCircuitBreakerService.permitNegativeValues = true;
            assertThat(
                HierarchyCircuitBreakerService.buildParentTripMessage(
                    -1L,
                    "test",
                    new HierarchyCircuitBreakerService.MemoryUsage(-2L, -3L, -4L, -5L),
                    -6L,
                    true,
                    Map.of("child1", new TestChildCircuitBreaker(-7L))
                ),
                equalTo(
                    "[parent] Data too large, data for [test] would be [-3], which is larger than the limit of [-6], "
                        + "real usage: [-2], new bytes reserved: [-1/-1b], usages [child1=-7]"
                )
            );
        } finally {
            HierarchyCircuitBreakerService.permitNegativeValues = false;
        }
    }
}
