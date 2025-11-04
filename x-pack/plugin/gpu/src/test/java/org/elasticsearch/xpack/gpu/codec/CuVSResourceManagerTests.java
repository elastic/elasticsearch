/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CuVSMatrix;
import com.nvidia.cuvs.CuVSResources;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class CuVSResourceManagerTests extends ESTestCase {

    private static final Logger log = LogManager.getLogger(CuVSResourceManagerTests.class);

    public static final long TOTAL_DEVICE_MEMORY_IN_BYTES = 256L * 1024 * 1024;

    public void testBasic() throws InterruptedException {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        mgr.release(res1);
        mgr.release(res2);
        res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);
        res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        mgr.release(res1);
        mgr.release(res2);
        mgr.shutdown();
    }

    public void testBlocking() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res3 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);
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
        var res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res2 = mgr.acquire((16 * 1024) + 1, 1024, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);
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
        var res1 = mgr.acquire(16 * 1024, 1024, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res2 = mgr.acquire((16 * 1024) - 1, 1024, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);
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
        var res = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);
        assertThrows(UnsupportedOperationException.class, res::close);
        mgr.release(res);
        mgr.shutdown();
    }

    public void testDoubleRelease() throws InterruptedException {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);
        var res2 = mgr.acquire(0, 0, CuVSMatrix.DataType.FLOAT, CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT);
        mgr.release(res1);
        mgr.release(res2);
        assertThrows(AssertionError.class, () -> mgr.release(randomFrom(res1, res2)));
        mgr.shutdown();
    }

    static class MockPoolingCuVSResourceManager extends CuVSResourceManager.PoolingCuVSResourceManager {

        private final AtomicInteger idGenerator = new AtomicInteger();

        MockPoolingCuVSResourceManager(int capacity) {
            this(capacity, new ArrayList<>());
        }

        private MockPoolingCuVSResourceManager(int capacity, List<Long> allocationList) {
            super(capacity, new TrackingGPUMemoryService(TOTAL_DEVICE_MEMORY_IN_BYTES));
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
