/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu;

import com.nvidia.cuvs.GPUInfoProvider;
import com.nvidia.cuvs.spi.CuVSProvider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

/**
 * Implementation of the {@link GPUSupport} interface that provides GPU support
 * detection and information based on the cuVS library.
 */
public class CuVSGPUSupport implements GPUSupport {

    private static final Logger LOG = LogManager.getLogger(CuVSGPUSupport.class);

    // Set the minimum at 7.5GB: 8GB GPUs (which are our targeted minimum) report less than that via the API
    private static final long MIN_DEVICE_MEMORY_IN_BYTES = 8053063680L;


    private record GpuInfo(long totalMemory, String name) {

        static final GpuInfo UNSUPPORTED = new GpuInfo(0L, null);

        GpuInfo {
            checkNonNegative(totalMemory, "totalMemory");
        }
    }

    static long checkNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative, got [" + value + "]");
        }
        return value;
    }

    private static class Holder {
        static final GpuInfo GPU_INFO = initializeGpuInfo();
    }

    /**
     * Initializes GPU support information by finding the first compatible GPU.
     * Returns a {@link GpuInfo} with memory and name, or {@link GpuInfo#UNSUPPORTED} if no compatible GPU is found.
     */
    private static GpuInfo initializeGpuInfo() {
        try {
            var gpuInfoProvider = CuVSProvider.provider().gpuInfoProvider();
            LOG.info("Initializing GPU support with CuVS provider {}", gpuInfoProvider.getClass().getName());
            var availableGPUs = gpuInfoProvider.availableGPUs();
            if (availableGPUs.isEmpty()) {
                LOG.warn("No GPU found");
                return GpuInfo.UNSUPPORTED;
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
                    return new GpuInfo(gpu.totalDeviceMemoryInBytes(), gpu.name());
                }
            }

            return GpuInfo.UNSUPPORTED;
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
            return GpuInfo.UNSUPPORTED;
        } catch (Throwable t) {
            if (t instanceof ExceptionInInitializerError ex) {
                t = ex.getCause();
            }
            LOG.warn("Exception occurred during creation of cuvs resources", t);
            return GpuInfo.UNSUPPORTED;
        }
    }


    @Override
    public boolean isSupported() {
        return Holder.GPU_INFO != GpuInfo.UNSUPPORTED;
    }

    @Override
    public long getTotalGpuMemory() {
        return Holder.GPU_INFO.totalMemory();
    }

    @Override
    public String getGpuName() {
        return Holder.GPU_INFO.name();
    }
}
