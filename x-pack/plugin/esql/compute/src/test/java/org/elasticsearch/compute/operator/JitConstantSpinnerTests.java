/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Pattern;

public class JitConstantSpinnerTests extends ESTestCase {

    private JitConstantSpinner spinner;

    // ---- Test base classes the spinner will subclass ----

    public abstract static class LongBase {
        protected abstract long divisor();

        public long applyMod(long lhs) {
            return lhs % divisor();
        }

        public long applyDiv(long lhs) {
            return lhs / divisor();
        }
    }

    public abstract static class IntBase {
        protected abstract int n();

        public int triple() {
            return n() * 3;
        }
    }

    public abstract static class DoubleBase {
        protected abstract double scale();

        public double scaled(double x) {
            return x * scale();
        }
    }

    public abstract static class PatternBase {
        protected abstract Pattern pat();

        public boolean matches(String s) {
            return pat().matcher(s).matches();
        }
    }

    public abstract static class TwoCtorBase {
        public final String tag;
        public final long extra;

        public TwoCtorBase(String tag, long extra) {
            this.tag = tag;
            this.extra = extra;
        }

        protected abstract long value();
    }

    @Before
    public void reset() {
        // Each test gets its own spinner so counters/cache are isolated; the
        // SHARED instance used by production code is process-global and would
        // cross-contaminate test ordering.
        spinner = new JitConstantSpinner();
        // Most correctness tests don't care about admission filtering; force
        // threshold=1 (spin immediately on first miss) so they can call
        // .orElseThrow() freely. Tests that exercise admission filtering
        // set threshold explicitly.
        spinner.setAdmissionThreshold(1);
    }

    // ---- Correctness ----

    public void testLongSubclassReturnsConstant() throws Exception {
        Class<? extends LongBase> klass = spinner.longConstantSubclass(LongBase.class, "divisor", 60L).orElseThrow();
        LongBase inst = klass.getConstructor().newInstance();
        assertEquals(60L, inst.divisor());
        assertEquals(7L, inst.applyMod(127L));
        assertEquals(2L, inst.applyDiv(127L));
    }

    public void testIntSubclass() throws Exception {
        Class<? extends IntBase> klass = spinner.intConstantSubclass(IntBase.class, "n", 7).orElseThrow();
        IntBase inst = klass.getConstructor().newInstance();
        assertEquals(7, inst.n());
        assertEquals(21, inst.triple());
    }

    public void testDoubleSubclass() throws Exception {
        Class<? extends DoubleBase> klass = spinner.doubleConstantSubclass(DoubleBase.class, "scale", 0.5).orElseThrow();
        DoubleBase inst = klass.getConstructor().newInstance();
        assertEquals(0.5, inst.scale(), 0.0);
        assertEquals(3.0, inst.scaled(6.0), 0.0);
    }

    public void testReferenceSubclassReturnsExactInstance() throws Exception {
        Pattern p = Pattern.compile("\\d+");
        Class<? extends PatternBase> klass = spinner.referenceConstantSubclass(PatternBase.class, "pat", Pattern.class, p).orElseThrow();
        PatternBase inst = klass.getConstructor().newInstance();
        assertSame(p, inst.pat());
        assertTrue(inst.matches("123"));
        assertFalse(inst.matches("abc"));
    }

    public void testCtorWithArgsIsReproduced() throws Exception {
        Class<? extends TwoCtorBase> klass = spinner.longConstantSubclass(TwoCtorBase.class, "value", 99L).orElseThrow();
        TwoCtorBase inst = klass.getConstructor(String.class, long.class).newInstance("hello", 42L);
        assertEquals("hello", inst.tag);
        assertEquals(42L, inst.extra);
        assertEquals(99L, inst.value());
    }

    // ---- Cache behavior ----

    public void testCacheHitReturnsSameClass() {
        Class<? extends LongBase> a = spinner.longConstantSubclass(LongBase.class, "divisor", 100L).orElseThrow();
        Class<? extends LongBase> b = spinner.longConstantSubclass(LongBase.class, "divisor", 100L).orElseThrow();
        assertSame(a, b);
        assertEquals(1, spinner.spinCount());
        assertEquals(1, spinner.hitCount());
        assertEquals(1, spinner.missCount());
    }

    public void testDifferentValueDifferentClass() {
        Class<?> a = spinner.longConstantSubclass(LongBase.class, "divisor", 1L).orElseThrow();
        Class<?> b = spinner.longConstantSubclass(LongBase.class, "divisor", 2L).orElseThrow();
        assertNotSame(a, b);
        assertEquals(2, spinner.spinCount());
    }

    public void testDifferentMethodDifferentClass() {
        Class<?> a = spinner.longConstantSubclass(LongBase.class, "divisor", 5L).orElseThrow();
        Class<?> b = spinner.longConstantSubclass(LongBase.class, "applyDiv", 5L).orElseThrow();
        // applyDiv isn't abstract — class still spins (we don't validate at spin time),
        // but cache key distinguishes
        assertNotSame(a, b);
    }

    public void testDifferentBaseDifferentClass() {
        Class<?> a = spinner.longConstantSubclass(LongBase.class, "divisor", 5L).orElseThrow();
        Class<?> b = spinner.intConstantSubclass(IntBase.class, "n", 5).orElseThrow();
        assertNotSame(a, b);
    }

    // ---- Admission filtering ----

    public void testAdmissionRejectsFirstAccess() {
        spinner.setAdmissionThreshold(2);  // explicit default
        // First access: admission counter goes 0->1, threshold=2, return empty
        Optional<Class<? extends LongBase>> first = spinner.longConstantSubclass(LongBase.class, "divisor", 17L);
        assertTrue("first access should be rejected by admission", first.isEmpty());
        assertEquals(0, spinner.spinCount());
        assertEquals(1, spinner.admissionRejectedCount());

        // Second access: counter 1->2, threshold met, spin and return class
        Optional<Class<? extends LongBase>> second = spinner.longConstantSubclass(LongBase.class, "divisor", 17L);
        assertTrue("second access should spin and return class", second.isPresent());
        assertEquals(1, spinner.spinCount());

        // Third+ access: cache hit
        Optional<Class<? extends LongBase>> third = spinner.longConstantSubclass(LongBase.class, "divisor", 17L);
        assertSame(second.get(), third.get());
        assertEquals(1, spinner.spinCount());
        assertEquals(1, spinner.hitCount());
    }

    public void testAdmissionFiltersOneOffKeys() {
        spinner.setAdmissionThreshold(2);
        // 100 distinct keys, each accessed once. With threshold=2, none should spin.
        for (long i = 0; i < 100; i++) {
            Optional<Class<? extends LongBase>> r = spinner.longConstantSubclass(LongBase.class, "divisor", 1000L + i);
            assertTrue("one-off key should be rejected", r.isEmpty());
        }
        assertEquals("no spins for one-off keys", 0, spinner.spinCount());
        assertEquals(100, spinner.admissionRejectedCount());
    }

    public void testAdmissionTrackerLRUEviction() {
        spinner.setAdmissionCapacity(3);
        spinner.setAdmissionThreshold(2);

        // Fill the admission tracker with 3 keys, each at count 1
        spinner.longConstantSubclass(LongBase.class, "divisor", 1L);
        spinner.longConstantSubclass(LongBase.class, "divisor", 2L);
        spinner.longConstantSubclass(LongBase.class, "divisor", 3L);
        assertEquals(3, spinner.admissionTrackerSize());

        // Add a 4th key — LRU should evict key 1
        spinner.longConstantSubclass(LongBase.class, "divisor", 4L);
        assertEquals(3, spinner.admissionTrackerSize());
        assertEquals(1, spinner.evictionCount());

        // Key 1's counter is gone — next access starts fresh (count=1), still rejected
        Optional<Class<? extends LongBase>> r = spinner.longConstantSubclass(LongBase.class, "divisor", 1L);
        assertTrue("key with evicted counter should be re-rejected", r.isEmpty());
        assertEquals(0, spinner.spinCount());
    }

    public void testEvaluatorInstancePinsClassThroughWeakRef() throws Exception {
        spinner.setAdmissionThreshold(1);  // skip admission for this test
        Class<? extends LongBase> klass = spinner.longConstantSubclass(LongBase.class, "divisor", 99L).orElseThrow();
        LongBase instance = klass.getConstructor().newInstance();

        // GC should NOT reclaim the class while the instance is alive
        System.gc();
        Thread.sleep(50);
        System.gc();

        Class<? extends LongBase> klassAgain = spinner.longConstantSubclass(LongBase.class, "divisor", 99L).orElseThrow();
        assertSame("class must still be cached while instance lives", klass, klassAgain);
        assertEquals(99L, instance.divisor());  // sanity: instance still works
    }

    // ---- Concurrency ----

    public void testConcurrentSpinningOfSameKeyProducesSingleClass() throws Exception {
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch go = new CountDownLatch(1);
            AtomicReferenceArray<Class<?>> results = new AtomicReferenceArray<>(threads);
            for (int i = 0; i < threads; i++) {
                int idx = i;
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                    } catch (InterruptedException ignore) {}
                    results.set(idx, spinner.longConstantSubclass(LongBase.class, "divisor", 42L).orElseThrow());
                });
            }
            ready.await(5, TimeUnit.SECONDS);
            go.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

            Class<?> first = results.get(0);
            assertNotNull(first);
            for (int i = 1; i < threads; i++) {
                assertSame(first, results.get(i));
            }
            // At least one spin happened. Under tight races several may spin redundantly;
            // cache de-dups via putIfAbsent so cacheSize stays at 1.
            assertEquals(1, spinner.cacheSize());
        } finally {
            pool.shutdownNow();
        }
    }

    // ---- Telemetry ----

    public void testCounters() {
        assertEquals(0, spinner.spinCount());
        assertEquals(0, spinner.hitCount());
        assertEquals(0, spinner.missCount());

        spinner.longConstantSubclass(LongBase.class, "divisor", 1L);
        assertEquals(1, spinner.spinCount());
        assertEquals(0, spinner.hitCount());
        assertEquals(1, spinner.missCount());

        spinner.longConstantSubclass(LongBase.class, "divisor", 1L);
        assertEquals(1, spinner.spinCount());
        assertEquals(1, spinner.hitCount());
        assertEquals(1, spinner.missCount());

        spinner.longConstantSubclass(LongBase.class, "divisor", 2L);
        assertEquals(2, spinner.spinCount());
        assertEquals(1, spinner.hitCount());
        assertEquals(2, spinner.missCount());
    }

    public void testSetCapacityValidation() {
        expectThrows(IllegalArgumentException.class, () -> spinner.setAdmissionCapacity(0));
        expectThrows(IllegalArgumentException.class, () -> spinner.setAdmissionCapacity(-1));
        expectThrows(IllegalArgumentException.class, () -> spinner.setAdmissionThreshold(0));
        expectThrows(IllegalArgumentException.class, () -> spinner.setAdmissionThreshold(-1));
    }

    public void testReferenceSubclassRejectsNull() {
        expectThrows(
            IllegalArgumentException.class,
            () -> spinner.referenceConstantSubclass(PatternBase.class, "pat", Pattern.class, null)
        );
    }

    public void testReferenceSubclassRejectsPrimitive() {
        expectThrows(IllegalArgumentException.class, () -> spinner.referenceConstantSubclass(LongBase.class, "divisor", long.class, 1L));
    }

    // NOTE: drift-guard negative tests (forbidden opcodes, foreign field owners, extra
    // methods, wrong superclass, ~50 paranoia cases total) live deliberately in a separate
    // file — SpunClassShapeContractTests — so that a developer modifying the spinner
    // doesn't reflexively update them alongside this file. If one fails, the failure
    // itself is the warning that the bytecode shape contract has shifted.
}
