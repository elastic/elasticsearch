/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CuVSResources;

import com.nvidia.cuvs.CuVSResourcesInfo;
import com.nvidia.cuvs.GPUInfo;
import com.nvidia.cuvs.GPUInfoProvider;

import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;

public class CuVSResourceManagerTests extends ESTestCase {

    public void testBasic() throws InterruptedException {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0);
        var res2 = mgr.acquire(0, 0);
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        mgr.release(res1);
        mgr.release(res2);
        res1 = mgr.acquire(0, 0);
        res2 = mgr.acquire(0, 0);
        assertThat(res1.toString(), containsString("id=0"));
        assertThat(res2.toString(), containsString("id=1"));
        mgr.release(res1);
        mgr.release(res2);
        mgr.shutdown();
    }

    public void testBlocking() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0);
        var res2 = mgr.acquire(0, 0);

        AtomicReference<CuVSResources> holder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                var res3 = mgr.acquire(0, 0);
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

    public void testManagedResIsNotClosable() throws Exception {
        var mgr = new MockPoolingCuVSResourceManager(1);
        var res = mgr.acquire(0, 0);
        assertThrows(UnsupportedOperationException.class, () -> res.close());
        mgr.release(res);
        mgr.shutdown();
    }

    public void testDoubleRelease() throws InterruptedException {
        var mgr = new MockPoolingCuVSResourceManager(2);
        var res1 = mgr.acquire(0, 0);
        var res2 = mgr.acquire(0, 0);
        mgr.release(res1);
        mgr.release(res2);
        assertThrows(AssertionError.class, () -> mgr.release(randomFrom(res1, res2)));
        mgr.shutdown();
    }

    static class MockPoolingCuVSResourceManager extends CuVSResourceManager.PoolingCuVSResourceManager {

        final AtomicInteger idGenerator = new AtomicInteger();

        MockPoolingCuVSResourceManager(int capacity) {
            super(capacity, new MockGPUInfoProvider());
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
        @Override
        public List<GPUInfo> availableGPUs() throws Throwable {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<GPUInfo> compatibleGPUs() throws Throwable {
            throw new UnsupportedOperationException();
        }

        @Override
        public CuVSResourcesInfo getCurrentInfo(CuVSResources cuVSResources) {
            return new CuVSResourcesInfo(256L * 1024 * 1024, 2048L * 1024 * 1024);
        }
    }
}
