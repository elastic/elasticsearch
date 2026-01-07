/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu;

import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.GPUInfoProvider;
import com.nvidia.cuvs.spi.CuVSProvider;

import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

public class GPUSupport {

    private static final Logger LOG = LogManager.getLogger(GPUSupport.class);

    // Set the minimum at 7.5GB: 8GB GPUs (which are our targeted minimum) report less than that via the API
    private static final long MIN_DEVICE_MEMORY_IN_BYTES = 8053063680L;

    private static class Holder {
        static final long TOTAL_GPU_MEMORY;
        static final boolean IS_SUPPORTED;

        static {
            TOTAL_GPU_MEMORY = initializeGpuInfo();
            IS_SUPPORTED = TOTAL_GPU_MEMORY != -1L;
        }
    }

    /**
     * Initializes GPU support information by finding the first compatible GPU.
     * Returns the total GPU memory in bytes, or -1 if GPU is not found or supported.
     */
    private static long initializeGpuInfo() {
        try {
            var gpuInfoProvider = CuVSProvider.provider().gpuInfoProvider();
            var availableGPUs = gpuInfoProvider.availableGPUs();
            if (availableGPUs.isEmpty()) {
                LOG.warn("No GPU found");
                return -1L;
            }

            for (var gpu : availableGPUs) {
                int major = gpu.computeCapabilityMajor();
                int minor = gpu.computeCapabilityMinor();
                boolean hasRequiredCapability = major >= GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MAJOR
                    && (major > GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MAJOR || minor >= GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MINOR);
                boolean hasRequiredMemory = gpu.totalDeviceMemoryInBytes() >= MIN_DEVICE_MEMORY_IN_BYTES;

                if (hasRequiredCapability == false) {
                    LOG.warn(
                        "GPU [{}] does not have the minimum compute capabilities (required: [{}.{}], found: [{}.{}])",
                        gpu.name(),
                        GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MAJOR,
                        GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MINOR,
                        gpu.computeCapabilityMajor(),
                        gpu.computeCapabilityMinor()
                    );
                } else if (hasRequiredMemory == false) {
                    LOG.warn(
                        "GPU [{}] does not have minimum memory required (required: [{}], found: [{}])",
                        gpu.name(),
                        MIN_DEVICE_MEMORY_IN_BYTES,
                        gpu.totalDeviceMemoryInBytes()
                    );
                } else {
                    LOG.info("Found compatible GPU [{}] (id: [{}])", gpu.name(), gpu.gpuId());
                    return gpu.totalDeviceMemoryInBytes();
                }
            }

            return -1L;
        } catch (UnsupportedOperationException uoe) {
            final String msg;
            if (uoe.getMessage() == null) {
                msg = Strings.format(
                    "runtime Java version [%d], OS [%s], arch [%s]",
                    Runtime.version().feature(),
                    System.getProperty("os.name"),
                    System.getProperty("os.arch")
                );
            } else {
                msg = uoe.getMessage();
            }
            LOG.warn("GPU based vector indexing is not supported on this platform; " + msg);
            return -1L;
        } catch (Throwable t) {
            if (t instanceof ExceptionInInitializerError ex) {
                t = ex.getCause();
            }
            LOG.warn("Exception occurred during creation of cuvs resources", t);
            return -1L;
        }
    }

    /** Tells whether the platform supports cuvs. */
    public static boolean isSupported() {
        return Holder.IS_SUPPORTED;
    }

    /** Returns a resources if supported, otherwise null. */
    public static CuVSResources cuVSResourcesOrNull(boolean logError) {
        try {
            var resources = CuVSResources.create();
            return resources;
        } catch (UnsupportedOperationException uoe) {
            if (logError) {
                String msg = "";
                if (uoe.getMessage() == null) {
                    msg = "Runtime Java version: " + Runtime.version().feature();
                } else {
                    msg = ": " + uoe.getMessage();
                }
                LOG.warn("GPU based vector indexing is not supported on this platform or java version; " + msg);
            }
        } catch (Throwable t) {
            if (logError) {
                if (t instanceof ExceptionInInitializerError ex) {
                    t = ex.getCause();
                }
                LOG.warn("Exception occurred during creation of cuvs resources", t);
            }
        }
        return null;
    }

    /**
     * Returns the total device memory in bytes of the first available compatible GPU.
     *
     * @return total device memory in bytes, or -1 if GPU is not available or supported
     */
    public static long getTotalGpuMemory() {
        return Holder.TOTAL_GPU_MEMORY;
    }
}
