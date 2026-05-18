/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CuVSIvfPqIndexParams;
import com.nvidia.cuvs.CuVSIvfPqParams;
import com.nvidia.cuvs.CuVSMatrix;
import com.nvidia.cuvs.CuVSResources;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class CuVSResourceManagerTests extends ESTestCase {

    private static final Logger log = LogManager.getLogger(CuVSResourceManagerTests.class);

    public static final long TOTAL_DEVICE_MEMORY_IN_BYTES = 256L * 1024 * 1024;

    private static void testBasic(CagraIndexParams params) throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params, "test");
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params, "test");
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        mgr.release(res1);
        mgr.release(res2);
        res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params, "test");
        res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params, "test");
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        mgr.release(res1);
        mgr.release(res2);
        mgr.shutdown();
    }

    public void testBasicWithNNDescent() throws Exception {
        testBasic(createNnDescentParams());
    }

    public void testBasicWithIvfPq() throws Exception {
        testBasic(createIvfPqParams());
    }

    public void testMultipleAcquireRelease() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        var res2 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, createIvfPqParams(), "test");
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        assertThat(mgr.availableMemory(), lessThan(TOTAL_DEVICE_MEMORY_IN_BYTES / 2));
        mgr.release(res1);
        mgr.release(res2);
        assertThat(mgr.availableMemory(), equalTo(TOTAL_DEVICE_MEMORY_IN_BYTES));
        res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        res2 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, createIvfPqParams(), "test");
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        assertThat(mgr.availableMemory(), lessThan(TOTAL_DEVICE_MEMORY_IN_BYTES / 2));
        mgr.release(res1);
        mgr.release(res2);
        assertThat(mgr.availableMemory(), equalTo(TOTAL_DEVICE_MEMORY_IN_BYTES));
        mgr.shutdown();
    }

    private static void testBlocking(CagraIndexParams params) throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params, "test");
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params, "test");

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res3 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params, "test");
                holder.set(res3);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        t.start();
        Thread.sleep(1_000);
        assertNull(holder.get());
        mgr.release(randomFrom(res1, res2));
        t.join();
        assertThat(holder.get().toString(), anyOf(containsString("id=0"), containsString("id=1")));
        mgr.shutdown();
    }

    public void testBlockingWithNNDescent() throws Exception {
        testBlocking(createNnDescentParams());
    }

    public void testBlockingWithIvfPq() throws Exception {
        testBlocking(createIvfPqParams());
    }

    private static void testBlockingOnInsufficientMemory(CagraIndexParams params, CuVSResourceManager mgr) throws Exception {
        var res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, params, "test");

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res2 = mgr.acquire((16 * 1024) + 1, 1024, CuVSMatrix.DataType.FLOAT, params, "test");
                holder.set(res2);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        t.start();
        Thread.sleep(1_000);
        assertNull(holder.get());
        mgr.release(res1);
        t.join();
        assertThat(holder.get().toString(), anyOf(containsString("id=0"), containsString("id=1")));
        mgr.shutdown();
    }

    public void testBlockingOnInsufficientMemoryNnDescent() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        testBlockingOnInsufficientMemory(createNnDescentParams(), mgr);
    }

    public void testBlockingOnInsufficientMemoryIvfPq() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2, 32L * 1024 * 1024);
        testBlockingOnInsufficientMemory(createIvfPqParams(), mgr);
    }

    private static void testNotBlockingOnSufficientMemory(CagraIndexParams params, CuVSResourceManager mgr) throws Exception {
        var res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, params, "test");

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res2 = mgr.acquire((16 * 1024) - 1000, 1024, CuVSMatrix.DataType.FLOAT, params, "test");
                holder.set(res2);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        t.start();
        t.join(5_000);
        assertNotNull(holder.get());
        assertThat(holder.get().toString(), not(equalTo(res1.toString())));
        mgr.shutdown();
    }

    public void testNotBlockingOnSufficientMemoryNnDescent() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        testNotBlockingOnSufficientMemory(createNnDescentParams(), mgr);
    }

    public void testNotBlockingOnSufficientMemoryIvfPq() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2, 32L * 1024 * 1024);
        testNotBlockingOnSufficientMemory(createIvfPqParams(), mgr);
    }

    public void testManagedResIsNotClosable() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(1);
        var res = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        assertThrows(UnsupportedOperationException.class, res::close);
        mgr.release(res);
        mgr.shutdown();
    }

    // Tests that a failed createNew() causes acquire to wait for an existing resource rather than crash with NPE.
    public void testCreateNewFailsAfterFirstResource() throws Exception {
        var mgr = new FailingAfterNMockPoolingCuVSResourceManager(2, 1);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        assertThat(res1.toString(), containsString("id=0"));

        AtomicReference<CuVSResourceManager.ManagedCuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
                holder.set(res2);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        t.start();
        // Wait until createNew() has failed and returned null, meaning the thread is about to block
        assertTrue(mgr.createFailedLatch.await(5, TimeUnit.SECONDS));
        mgr.release(res1);
        t.join(5_000);
        // Should have received the first (now released) resource
        assertNotNull(holder.get());
        assertThat(holder.get().toString(), containsString("id=0"));
        mgr.shutdown();
    }

    // Tests that acquire throws IOException immediately if no resources exist and createNew() always fails.
    public void testCreateNewAlwaysFailsThrowsIOException() throws Exception {
        var mgr = new FailingAfterNMockPoolingCuVSResourceManager(2, 0);
        expectThrows(java.io.IOException.class, () -> mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test"));
        mgr.shutdown();
    }

    public void testDoubleRelease() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        mgr.release(res1);
        mgr.release(res2);
        assertThrows(AssertionError.class, () -> mgr.release(randomFrom(res1, res2)));
        mgr.shutdown();
    }

    // tryAcquire returns null when all resources in the pool are locked
    public void testTryAcquireReturnsNullWhenBusy() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(1);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        assertNotNull(res1);
        var res2 = mgr.tryAcquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        assertNull(res2);
        mgr.release(res1);
        mgr.shutdown();
    }

    // tryAcquire succeeds when resources are available in the pool
    public void testTryAcquireSucceedsWhenFree() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.tryAcquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        assertNotNull(res1);
        assertThat(res1.toString(), containsString("id=0"));
        var res2 = mgr.tryAcquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        assertNotNull(res2);
        assertThat(res2.toString(), containsString("id=1"));
        mgr.release(res1);
        mgr.release(res2);
        mgr.shutdown();
    }

    // tryAcquire returns null when available GPU memory is insufficient
    public void testTryAcquireReturnsNullOnInsufficientMemory() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.tryAcquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        assertNotNull(res1);
        var res2 = mgr.tryAcquire((16 * 1024) + 1, 1024, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        assertNull(res2);
        mgr.release(res1);
        mgr.shutdown();
    }

    // tryAcquire succeeds after a previously locked resource is released
    public void testTryAcquireSucceedsAfterRelease() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(1);
        var res1 = mgr.tryAcquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        assertNotNull(res1);
        assertNull(mgr.tryAcquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test"));
        mgr.release(res1);
        var res2 = mgr.tryAcquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        assertNotNull(res2);
        assertThat(res2.toString(), containsString("id=0"));
        mgr.release(res2);
        mgr.shutdown();
    }

    // tryAcquire returns null (not IOException) when createNew fails and no existing resource is free
    public void testTryAcquireReturnsNullWhenCreateNewFails() throws Exception {
        var mgr = new FailingAfterNMockPoolingCuVSResourceManager(2, 0);
        var res = mgr.tryAcquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
        assertNull(res);
        mgr.shutdown();
    }

    // N threads tryAcquire concurrently: exactly capacity succeed, the rest get null
    public void testTryAcquireConcurrent() throws Exception {
        int capacity= randomIntBetween(2, 4);
        var mgr = new MockPoolingCuVSResourceManager(capacity);
        int numThreads = randomIntBetween(8, 12);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger nulls = new AtomicInteger();

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    var res = mgr.tryAcquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams(), "test");
                    if (res != null) {
                        successes.incrementAndGet();
                    } else {
                        nulls.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }
        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        assertThat(successes.get(), equalTo(capacity));
        assertThat(nulls.get(), equalTo(numThreads - capacity));
        mgr.shutdown();
    }

    public void testEstimateNNDescentMemoryOverflow() {
        int numVectors = 500_000;
        int dims = 1024;
        long result = CuVSResourceManager.estimateNNDescentMemory(numVectors, dims, CuVSMatrix.DataType.FLOAT);
        assertThat(result, equalTo(4_096_000_000L));
    }

    private static CagraIndexParams createNnDescentParams() {
        return new CagraIndexParams.Builder().withCagraGraphBuildAlgo(CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT)
            .withNNDescentNumIterations(5)
            .build();
    }

    private static CagraIndexParams createIvfPqParams() {
        return new CagraIndexParams.Builder().withCagraGraphBuildAlgo(CagraIndexParams.CagraGraphBuildAlgo.IVF_PQ)
            .withCuVSIvfPqParams(
                new CuVSIvfPqParams.Builder().withCuVSIvfPqIndexParams(
                    new CuVSIvfPqIndexParams.Builder().withPqBits(4).withPqDim(1024).build()
                ).build()
            )
            .build();
    }

    static class MockPoolingCuVSResourceManager extends CuVSResourceManager.PoolingCuVSResourceManager {

        private final AtomicInteger idGenerator = new AtomicInteger();
        private final GPUMemoryService gpuMemoryService;

        MockPoolingCuVSResourceManager(int capacity) {
            this(capacity, TOTAL_DEVICE_MEMORY_IN_BYTES);
        }

        MockPoolingCuVSResourceManager(int capacity, long totalMemoryInBytes) {
            this(capacity, new TrackingGPUMemoryService(totalMemoryInBytes));
        }

        private MockPoolingCuVSResourceManager(int capacity, GPUMemoryService gpuMemoryService) {
            super(capacity, gpuMemoryService);
            this.gpuMemoryService = gpuMemoryService;
        }

        long availableMemory() {
            return gpuMemoryService.availableMemoryInBytes(null);
        }

        @Override
        protected CuVSResources createNew() {
            return new MockCuVSResources(idGenerator.getAndIncrement());
        }
    }

    static class FailingAfterNMockPoolingCuVSResourceManager extends CuVSResourceManager.PoolingCuVSResourceManager {

        private final AtomicInteger idGenerator = new AtomicInteger();
        private final int succeedCount;
        private final CountDownLatch createFailedLatch = new CountDownLatch(1);

        FailingAfterNMockPoolingCuVSResourceManager(int capacity, int succeedCount) {
            super(capacity, new TrackingGPUMemoryService(TOTAL_DEVICE_MEMORY_IN_BYTES));
            this.succeedCount = succeedCount;
        }

        @Override
        protected CuVSResources createNew() {
            int id = idGenerator.getAndIncrement();
            if (id < succeedCount) {
                return new MockCuVSResources(id);
            }
            createFailedLatch.countDown();
            return null;
        }
    }

    static class MockCuVSResources implements CuVSResources {

        final int id;

        MockCuVSResources(int id) {
            this.id = id;
        }

        @Override
        public ScopedAccess access() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int deviceId() {
            return 0;
        }

        @Override
        public void close() {}

        @Override
        public Path tempDirectory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "MockCuVSResources[id=" + id + "]";
        }
    }
}
