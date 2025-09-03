/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CuVSResources;

import org.elasticsearch.xpack.gpu.GPUSupport;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A manager of {@link com.nvidia.cuvs.CuVSResources}. There is one manager per GPU.
 *
 * <p>All access to GPU resources is mediated through a manager. A manager helps coordinate usage threads to:
 * <ul>
 *     <li>ensure single-threaded access to any particular resource at a time</li>
 *     <li>Control the total number of concurrent operations that may be performed on a GPU</li>
 *     <li>Pool resources, to avoid frequent creation and destruction, which are expensive operations. </li>
 * </ul>
 *
 * <p> Fundamentally, a resource is used in compute and memory bound operations. The former occurs prior to the latter, e.g.
 * index build (compute), followed by a copy/process of the newly built index (memory). The manager allows the resource
 * user to indicate that compute is complete before releasing the resources. This can help improve parallelism of compute
 * on the GPU - allowing the next compute operation to proceed before releasing the resources.
 *
 */
public interface CuVSResourceManager {

    /**
     * Acquires a resource from the manager.
     *
     * <p>A manager can use the given parameters, numVectors and dims, to estimate the potential
     * effect on GPU memory and compute usage to determine whether to give out
     * another resource or wait for a resources to be returned before giving out another.
     */
    // numVectors and dims are currently unused, but could be used along with GPU metadata,
    // memory, generation, etc, when acquiring for 10M x 1536 dims, or 100,000 x 128 dims,
    // to give out a resources or not.
    ManagedCuVSResources acquire(int numVectors, int dims) throws InterruptedException;

    /** Marks the resources as finished with regard to compute. */
    void finishedComputation(ManagedCuVSResources resources);

    /** Returns the given resource to the manager. */
    void release(ManagedCuVSResources resources);

    /** Shuts down the manager, releasing all open resources. */
    void shutdown();

    /** Returns the system-wide pooling manager. */
    static CuVSResourceManager pooling() {
        return PoolingCuVSResourceManager.INSTANCE;
    }

    /**
     * A manager that maintains a pool of resources.
     */
    class PoolingCuVSResourceManager implements CuVSResourceManager {

        static final int MAX_RESOURCES = 2;
        static final PoolingCuVSResourceManager INSTANCE = new PoolingCuVSResourceManager(MAX_RESOURCES);

        final BlockingQueue<ManagedCuVSResources> pool;
        final int capacity;
        int createdCount;

        public PoolingCuVSResourceManager(int capacity) {
            if (capacity < 1 || capacity > MAX_RESOURCES) {
                throw new IllegalArgumentException("Resource count must be between 1 and " + MAX_RESOURCES);
            }
            this.capacity = capacity;
            this.pool = new ArrayBlockingQueue<>(capacity);
        }

        @Override
        public ManagedCuVSResources acquire(int numVectors, int dims) throws InterruptedException {
            ManagedCuVSResources res = pool.poll();
            if (res != null) {
                return res;
            }
            synchronized (this) {
                if (createdCount < capacity) {
                    createdCount++;
                    return new ManagedCuVSResources(Objects.requireNonNull(createNew()));
                }
            }
            // Otherwise, wait for one to be released
            return pool.take();
        }

        // visible for testing
        protected CuVSResources createNew() {
            return GPUSupport.cuVSResourcesOrNull(true);
        }

        @Override
        public void finishedComputation(ManagedCuVSResources resources) {
            // currently does nothing, but could allow acquire to return possibly blocked resources
        }

        @Override
        public void release(ManagedCuVSResources resources) {
            var added = pool.offer(Objects.requireNonNull(resources));
            assert added : "Failed to release resource back to pool";
        }

        @Override
        public void shutdown() {
            for (ManagedCuVSResources res : pool) {
                res.delegate.close();
            }
            pool.clear();
        }
    }

    /** A managed resource. Cannot be closed. */
    final class ManagedCuVSResources implements CuVSResources {

        final CuVSResources delegate;

        ManagedCuVSResources(CuVSResources resources) {
            this.delegate = resources;
        }

        @Override
        public ScopedAccess access() {
            return delegate.access();
        }

        @Override
        public int deviceId() {
            return delegate.deviceId();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("this resource is managed, cannot be closed by clients");
        }

        @Override
        public Path tempDirectory() {
            return null;
        }

        @Override
        public String toString() {
            return "ManagedCuVSResources[delegate=" + delegate + "]";
        }
    }
}
