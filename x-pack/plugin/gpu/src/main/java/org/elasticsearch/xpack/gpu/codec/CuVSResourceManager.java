/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CuVSResources;

import com.nvidia.cuvs.GPUInfoProvider;

import com.nvidia.cuvs.spi.CuVSProvider;

import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.gpu.GPUSupport;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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

    @FunctionalInterface
    interface GpuInfoFunction {
        long get(CuVSResources resources);
    }

    /**
     * A manager that maintains a pool of resources.
     */
    class PoolingCuVSResourceManager implements CuVSResourceManager {

        /** A multiplier on input data to account for intermediate and output data size required while processing it */
        static final double GPU_COMPUTATION_MEMORY_FACTOR = 2.0;
        static final int GPU_UTILIZATION_MAX_PERCENT = 80;
        static final int MAX_RESOURCES = 2;
        static final GPUInfoProvider gpuInfoProvider = CuVSProvider.provider().gpuInfoProvider();
        static final PoolingCuVSResourceManager INSTANCE = new PoolingCuVSResourceManager(
            MAX_RESOURCES,
            res ->gpuInfoProvider.getCurrentInfo(res).totalDeviceMemoryInBytes(),
            res ->gpuInfoProvider.getCurrentInfo(res).freeDeviceMemoryInBytes(),
            PoolingCuVSResourceManager::getGpuUtilizationPercent
        );

        private final ManagedCuVSResources[] pool;
        private final int capacity;
        private int createdCount;

        private final GpuInfoFunction totalMemoryInBytesProvider;
        private final GpuInfoFunction freeMemoryInBytesProvider;
        private final GpuInfoFunction gpuUtilizationPercentProvider;

        ReentrantLock lock = new ReentrantLock();
        Condition enoughResourcesCondition = lock.newCondition();

        public PoolingCuVSResourceManager(
            int capacity,
            GpuInfoFunction totalMemoryInBytesProvider,
            GpuInfoFunction freeMemoryInBytesProvider,
            GpuInfoFunction gpuUtilizationPercentProvider
            ) {
            this.totalMemoryInBytesProvider = totalMemoryInBytesProvider;
            this.freeMemoryInBytesProvider = freeMemoryInBytesProvider;
            this.gpuUtilizationPercentProvider = gpuUtilizationPercentProvider;
            if (capacity < 1 || capacity > MAX_RESOURCES) {
                throw new IllegalArgumentException("Resource count must be between 1 and " + MAX_RESOURCES);
            }
            this.capacity = capacity;
            this.pool = new ManagedCuVSResources[MAX_RESOURCES];

            NVML.nvmlInit_v2();
        }

        private ManagedCuVSResources getResourceFromPool() {
            for (int i = 0; i < createdCount; ++i) {
                var res = pool[i];
                if (res.locked == false) {
                    return res;
                }
            }
            if (createdCount < capacity) {
                var res = new ManagedCuVSResources(Objects.requireNonNull(createNew()));
                pool[createdCount++] = res;
                return res;
            }
            return null;
        }

        private int numLockedResources() {
            int lockedResources = 0;
            for (int i = 0; i < createdCount; ++i) {
                var res = pool[i];
                if (res.locked) {
                    lockedResources++;
                }
            }
            return lockedResources;
        }

        @Override
        public ManagedCuVSResources acquire(int numVectors, int dims) throws InterruptedException {
            try {
                lock.lock();

                boolean allConditionsMet = false;
                ManagedCuVSResources res = null;
                while (allConditionsMet == false) {
                    res = getResourceFromPool();

                    final boolean enoughMemory;
                    final boolean enoughComputation;
                    if (res != null) {
                        // If no resource in the pool is locked, short circuit to avoid livelock
                        if (numLockedResources() == 0) {
                            break;
                        }

                        // Check resources availability
                        long requiredMemoryInBytes = estimateRequiredMemory(numVectors, dims);
                        if (requiredMemoryInBytes > totalMemoryInBytesProvider.get(res)) {
                            throw new IllegalArgumentException(
                                Strings.format(
                                    "Requested GPU memory for [%d] vectors, [%d] dims is greater than the GPU total memory [%dMB]",
                                    numVectors,
                                    dims,
                                    totalMemoryInBytesProvider.get(res) / (1024L * 1024L)
                                )
                            );
                        }
                        enoughMemory = requiredMemoryInBytes <= freeMemoryInBytesProvider.get(res);
                        enoughComputation = gpuUtilizationPercentProvider.get(res) < GPU_UTILIZATION_MAX_PERCENT;
                    } else {
                        enoughMemory = false;
                        enoughComputation = false;
                    }

                    allConditionsMet = enoughMemory && enoughComputation;
                    if (allConditionsMet == false) {
                        enoughResourcesCondition.await();
                    }
                }
                res.locked = true;
                return res;
            } finally {
                lock.unlock();
            }
        }

        private long estimateRequiredMemory(int numVectors, int dims) {
            return (long)(GPU_COMPUTATION_MEMORY_FACTOR * numVectors * dims * Float.BYTES);
        }

        private static int getGpuUtilizationPercent(CuVSResources resources) {
            try (var localArena = Arena.ofConfined()) {
                var deviceHandle = NVML.nvmlDeviceGetHandleByIndex_v2(resources.deviceId());
                var nvmlUtilizationPtr = localArena.allocate(NVML.nvmlUtilization_t.layout());
                NVML.nvmlDeviceGetUtilizationRates(deviceHandle, nvmlUtilizationPtr);
                return NVML.nvmlUtilization_t.gpu(nvmlUtilizationPtr);
            }
        }

        // visible for testing
        protected CuVSResources createNew() {
            return GPUSupport.cuVSResourcesOrNull(true);
        }

        @Override
        public void finishedComputation(ManagedCuVSResources resources) {
            // Allow acquire to return possibly blocked resources
            enoughResourcesCondition.signalAll();
        }

        @Override
        public void release(ManagedCuVSResources resources) {
            try {
                lock.lock();
                assert resources.locked;
                resources.locked = false;
                enoughResourcesCondition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < createdCount; ++i) {
                var res = pool[i];
                assert res != null;
                res.delegate.close();
            }
            NVML.nvmlShutdown();
        }
    }

    /** A managed resource. Cannot be closed. */
    final class ManagedCuVSResources implements CuVSResources {

        final CuVSResources delegate;
        boolean locked = false;

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
