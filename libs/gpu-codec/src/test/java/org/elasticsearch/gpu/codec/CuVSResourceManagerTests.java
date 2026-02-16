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

    private static void testBasic(CagraIndexParams params) throws InterruptedException {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params);
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params);
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        mgr.release(res1);
        mgr.release(res2);
        res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params);
        res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params);
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        mgr.release(res1);
        mgr.release(res2);
        mgr.shutdown();
    }

    public void testBasicWithNNDescent() throws InterruptedException {
        testBasic(createNnDescentParams());
    }

    public void testBasicWithIvfPq() throws InterruptedException {
        testBasic(createIvfPqParams());
    }

    public void testMultipleAcquireRelease() throws InterruptedException {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, createNnDescentParams());
        var res2 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, createIvfPqParams());
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        assertThat(mgr.availableMemory(), lessThan(TOTAL_DEVICE_MEMORY_IN_BYTES / 2));
        mgr.release(res1);
        mgr.release(res2);
        assertThat(mgr.availableMemory(), equalTo(TOTAL_DEVICE_MEMORY_IN_BYTES));
        res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, createNnDescentParams());
        res2 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, createIvfPqParams());
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
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params);
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params);

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res3 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, params);
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

    public void testBlockingWithNNDescent() throws Exception {
        testBlocking(createNnDescentParams());
    }

    public void testBlockingWithIvfPq() throws Exception {
        testBlocking(createIvfPqParams());
    }

    private static void testBlockingOnInsufficientMemory(CagraIndexParams params, CuVSResourceManager mgr) throws Exception {
        var res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, params);

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res2 = mgr.acquire((16 * 1024) + 1, 1024, CuVSMatrix.DataType.FLOAT, params);
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

    public void testBlockingOnInsufficientMemoryNnDescent() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        testBlockingOnInsufficientMemory(createNnDescentParams(), mgr);
    }

    public void testBlockingOnInsufficientMemoryIvfPq() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2, 32L * 1024 * 1024);
        testBlockingOnInsufficientMemory(createIvfPqParams(), mgr);
    }

    private static void testNotBlockingOnSufficientMemory(CagraIndexParams params, CuVSResourceManager mgr) throws Exception {
        var res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, params);

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res2 = mgr.acquire((16 * 1024) - 1000, 1024, CuVSMatrix.DataType.FLOAT, params);
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
        var res = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams());
        assertThrows(UnsupportedOperationException.class, res::close);
        mgr.release(res);
        mgr.shutdown();
    }

    public void testDoubleRelease() throws InterruptedException {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams());
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, createNnDescentParams());
        mgr.release(res1);
        mgr.release(res2);
        assertThrows(AssertionError.class, () -> mgr.release(randomFrom(res1, res2)));
        mgr.shutdown();
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
