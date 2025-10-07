/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CuVSMatrix;
import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.CuVSResourcesInfo;
import com.nvidia.cuvs.GPUInfo;
import com.nvidia.cuvs.GPUInfoProvider;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class CuVSResourceManagerTests extends ESTestCase {

    private static final Logger log = LogManager.getLogger(CuVSResourceManagerTests.class);

    public static final long TOTAL_DEVICE_MEMORY_IN_BYTES = 256L * 1024 * 1024;

    public void testBasic() throws InterruptedException {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT);
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT);
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        mgr.release(res1);
        mgr.release(res2);
        res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT);
        res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT);
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        mgr.release(res1);
        mgr.release(res2);
        mgr.shutdown();
    }

    public void testBlocking() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT);
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT);

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res3 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT);
                holder.set(res3);
            } catch (InterruptedException e) {
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

    public void testBlockingOnInsufficientMemory() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT);

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res2 = mgr.acquire((16 * 1024) + 1, 1024, CuVSMatrix.DataType.FLOAT);
                holder.set(res2);
            } catch (InterruptedException e) {
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

    public void testNotBlockingOnSufficientMemory() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT);

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res2 = mgr.acquire((16 * 1024) - 1, 1024, CuVSMatrix.DataType.FLOAT);
                holder.set(res2);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        t.start();
        t.join(5_000);
        assertNotNull(holder.get());
        assertThat(holder.get().toString(), not(equalTo(res1.toString())));
        mgr.shutdown();
    }

    public void testManagedResIsNotClosable() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(1);
        var res = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT);
        assertThrows(UnsupportedOperationException.class, res::close);
        mgr.release(res);
        mgr.shutdown();
    }

    public void testDoubleRelease() throws InterruptedException {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT);
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT);
        mgr.release(res1);
        mgr.release(res2);
        assertThrows(AssertionError.class, () -> mgr.release(randomFrom(res1, res2)));
        mgr.shutdown();
    }

    static class MockPoolingCuVSResourceManager extends CuVSResourceManager.PoolingCuVSResourceManager {

        private final AtomicInteger idGenerator = new AtomicInteger();
        private final List<Long> allocations;

        MockPoolingCuVSResourceManager(int capacity) {
            this(capacity, new ArrayList<>());
        }

        private MockPoolingCuVSResourceManager(int capacity, List<Long> allocationList) {
            super(capacity, new MockGPUInfoProvider(() -> freeMemoryFunction(allocationList)));
            this.allocations = allocationList;
        }

        private static long freeMemoryFunction(List<Long> allocations) {
            return TOTAL_DEVICE_MEMORY_IN_BYTES - allocations.stream().mapToLong(x -> x).sum();
        }

        @Override
        protected CuVSResources createNew() {
            return new MockCuVSResources(idGenerator.getAndIncrement());
        }

        @Override
        public ManagedCuVSResources acquire(int numVectors, int dims, CuVSMatrix.DataType dataType) throws InterruptedException {
            var res = super.acquire(numVectors, dims, dataType);
            long memory = (long) (numVectors * dims * Float.BYTES
                * CuVSResourceManager.PoolingCuVSResourceManager.GPU_COMPUTATION_MEMORY_FACTOR);
            allocations.add(memory);
            log.info("Added [{}]", memory);
            return res;
        }

        @Override
        public void release(ManagedCuVSResources resources) {
            if (allocations.isEmpty() == false) {
                var x = allocations.removeLast();
                log.info("Removed [{}]", x);
            }
            super.release(resources);
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

    private static class MockGPUInfoProvider implements GPUInfoProvider {
        private final LongSupplier freeMemorySupplier;

        MockGPUInfoProvider(LongSupplier freeMemorySupplier) {
            this.freeMemorySupplier = freeMemorySupplier;
        }

        @Override
        public List<GPUInfo> availableGPUs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<GPUInfo> compatibleGPUs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CuVSResourcesInfo getCurrentInfo(CuVSResources cuVSResources) {
            return new CuVSResourcesInfo(freeMemorySupplier.getAsLong(), TOTAL_DEVICE_MEMORY_IN_BYTES);
        }
    }
}
