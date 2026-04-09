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
import com.nvidia.cuvs.CuVSMatrix;
import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.spi.CuVSProvider;

import org.elasticsearch.core.Strings;
import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

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
    /** A multiplier on input data to account for intermediate and output data size required while processing it */
    double GPU_COMPUTATION_MEMORY_FACTOR = 2.0;

    /**
     * Acquires a resource from the manager.
     *
     * <p>A manager can use the given parameters, numVectors and dims, to estimate the potential
     * effect on GPU memory and compute usage to determine whether to give out
     * another resource or wait for a resources to be returned before giving out another.
     */
    ManagedCuVSResources acquire(int numVectors, int dims, CuVSMatrix.DataType dataType, CagraIndexParams cagraIndexParams)
        throws InterruptedException;

    /** Marks the resources as finished with regard to compute. */
    void finishedComputation(ManagedCuVSResources resources);

    /** Returns the given resource to the manager. */
    void release(ManagedCuVSResources resources);

    /** Shuts down the manager, releasing all open resources. */
    void shutdown();

    /**
     * Estimates the required GPU memory for building an index using the NN_DESCENT algorithm.
     *
     * @param numVectors the number of vectors
     * @param dims the dimensionality of vectors
     * @param dataType the data type of the vectors
     * @return the estimated memory in bytes needed for NN_DESCENT
     */
    static long estimateNNDescentMemory(int numVectors, int dims, CuVSMatrix.DataType dataType) {
        int elementTypeBytes = switch (dataType) {
            case FLOAT -> Float.BYTES;
            case INT, UINT -> Integer.BYTES;
            case BYTE -> Byte.BYTES;
        };
        return (long) (GPU_COMPUTATION_MEMORY_FACTOR * numVectors * dims * elementTypeBytes);
    }

    /** Returns the system-wide pooling manager. */
    static CuVSResourceManager pooling() {
        return PoolingCuVSResourceManager.Holder.INSTANCE;
    }

    /**
     * A manager that maintains a pool of resources.
     */
    class PoolingCuVSResourceManager implements CuVSResourceManager {

        static final Logger logger = LogManager.getLogger(CuVSResourceManager.class);
        static final int MAX_RESOURCES = 4;

        static class Holder {
            static final PoolingCuVSResourceManager INSTANCE = new PoolingCuVSResourceManager(
                MAX_RESOURCES,
                new RealGPUMemoryService(CuVSProvider.provider().gpuInfoProvider())
            );
        }

        private final ManagedCuVSResources[] pool;
        private final int capacity;
        private final GPUMemoryService gpuMemoryService;
        private int createdCount;

        ReentrantLock lock = new ReentrantLock();
        Condition enoughResourcesCondition = lock.newCondition();

        PoolingCuVSResourceManager(int capacity, GPUMemoryService gpuMemoryService) {
            if (capacity < 1 || capacity > MAX_RESOURCES) {
                throw new IllegalArgumentException("Resource count must be between 1 and " + MAX_RESOURCES);
            }
            this.capacity = capacity;
            this.gpuMemoryService = gpuMemoryService;
            this.pool = new ManagedCuVSResources[MAX_RESOURCES];
        }

        private ManagedCuVSResources getResourceFromPool() {
            for (int i = 0; i < createdCount; ++i) {
                var res = pool[i];
                if (res.isLocked() == false) {
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
                if (res.isLocked()) {
                    lockedResources++;
                }
            }
            return lockedResources;
        }

        @Override
        public ManagedCuVSResources acquire(int numVectors, int dims, CuVSMatrix.DataType dataType, CagraIndexParams cagraIndexParams)
            throws InterruptedException {
            try {
                var started = System.nanoTime();
                lock.lock();

                boolean allConditionsMet = false;
                ManagedCuVSResources res = null;

                long requiredMemoryInBytes = estimateRequiredMemory(numVectors, dims, dataType, cagraIndexParams);
                logger.debug(
                    "Estimated memory for [{}] vectors, [{}] dims of type [{}] is [{} B]",
                    numVectors,
                    dims,
                    dataType.name(),
                    requiredMemoryInBytes
                );

                while (allConditionsMet == false) {
                    res = getResourceFromPool();

                    final boolean enoughMemory;
                    if (res != null) {
                        // Check immutable constraints
                        long totalMemoryInBytes = gpuMemoryService.totalMemoryInBytes(res);
                        if (requiredMemoryInBytes > totalMemoryInBytes) {
                            String message = Strings.format(
                                "Requested GPU memory for [%d] vectors, [%d] dims is greater than the GPU total memory [%d B]",
                                numVectors,
                                dims,
                                totalMemoryInBytes
                            );
                            logger.error(message);
                            throw new IllegalArgumentException(message);
                        }

                        // If no resource in the pool is locked, short circuit to avoid livelock
                        if (numLockedResources() == 0) {
                            logger.debug("No resources currently locked, proceeding");
                            break;
                        }

                        // Check resources availability
                        long availableMemoryInBytes = gpuMemoryService.availableMemoryInBytes(res);
                        enoughMemory = requiredMemoryInBytes <= availableMemoryInBytes;
                        logger.debug("Free device memory [{} B], enoughMemory[{}]", availableMemoryInBytes, enoughMemory);
                    } else {
                        logger.debug("No resources available in pool");
                        enoughMemory = false;
                    }
                    // TODO: add enoughComputation / enoughComputationCondition here
                    allConditionsMet = enoughMemory; // && enoughComputation
                    if (allConditionsMet == false) {
                        enoughResourcesCondition.await();
                    }
                }
                var elapsed = started - System.nanoTime();
                logger.debug("Resource acquired in [{}ms]", elapsed / 1_000_000.0);
                gpuMemoryService.reserveMemory(requiredMemoryInBytes);
                res.lock(() -> gpuMemoryService.releaseMemory(requiredMemoryInBytes));
                return res;
            } finally {
                lock.unlock();
            }
        }

        private long estimateRequiredMemory(int numVectors, int dims, CuVSMatrix.DataType dataType, CagraIndexParams cagraIndexParams) {
            if (cagraIndexParams.getCagraGraphBuildAlgo() == CagraIndexParams.CagraGraphBuildAlgo.IVF_PQ
                && cagraIndexParams.getCuVSIvfPqParams() != null
                && cagraIndexParams.getCuVSIvfPqParams().getIndexParams() != null
                && cagraIndexParams.getCuVSIvfPqParams().getIndexParams().getPqDim() != 0) {
                // See https://docs.rapids.ai/api/cuvs/nightly/neighbors/ivfpq/#index-device-memory
                int elementTypeBytes = switch (dataType) {
                    case FLOAT -> Float.BYTES;
                    case INT, UINT -> Integer.BYTES;
                    case BYTE -> Byte.BYTES;
                };
                var pqDim = cagraIndexParams.getCuVSIvfPqParams().getIndexParams().getPqDim();
                var pqBits = cagraIndexParams.getCuVSIvfPqParams().getIndexParams().getPqBits();
                var numClusters = cagraIndexParams.getCuVSIvfPqParams().getIndexParams().getnLists();
                var approximatedIvfBytes = numVectors * (pqDim * (pqBits / 8.0) + elementTypeBytes) + (long) numClusters * Integer.BYTES;
                return (long) (GPU_COMPUTATION_MEMORY_FACTOR * approximatedIvfBytes);
            }

            return CuVSResourceManager.estimateNNDescentMemory(numVectors, dims, dataType);
        }

        // visible for testing
        protected CuVSResources createNew() {
            return GPUSupport.cuVSResourcesOrNull(true);
        }

        @Override
        public void finishedComputation(ManagedCuVSResources resources) {
            logger.debug("Computation finished");
            // currently does nothing, but could allow acquire to return possibly blocked resources
            // enoughResourcesCondition.signalAll()
        }

        @Override
        public void release(ManagedCuVSResources resources) {
            logger.debug("Releasing resources to pool");
            try {
                lock.lock();
                assert resources.isLocked();
                resources.unlock();
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
        }
    }

    /** A managed resource. Cannot be closed. */
    final class ManagedCuVSResources implements CuVSResources {

        private final CuVSResources delegate;
        private static final Runnable NOT_LOCKED = () -> {};
        private Runnable unlockAction = NOT_LOCKED;

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

        void lock(Runnable unlockAction) {
            this.unlockAction = unlockAction;
        }

        void unlock() {
            unlockAction.run();
            unlockAction = NOT_LOCKED;
        }

        boolean isLocked() {
            return unlockAction != NOT_LOCKED;
        }
    }
}
