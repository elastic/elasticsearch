/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

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

    /** Tells whether the platform supports cuvs. */
    public static boolean isSupported(boolean logError) {
        try {
            var gpuInfoProvider = CuVSProvider.provider().gpuInfoProvider();
            var availableGPUs = gpuInfoProvider.availableGPUs();
            if (availableGPUs.isEmpty()) {
                if (logError) {
                    LOG.warn("No GPU found");
                }
                return false;
            }

            for (var gpu : availableGPUs) {
                if (gpu.computeCapabilityMajor() < GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MAJOR
                    || (gpu.computeCapabilityMajor() == GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MAJOR
                        && gpu.computeCapabilityMinor() < GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MINOR)) {
                    if (logError) {
                        LOG.warn(
                            "GPU [{}] does not have the minimum compute capabilities (required: [{}.{}], found: [{}.{}])",
                            gpu.name(),
                            GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MAJOR,
                            GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MINOR,
                            gpu.computeCapabilityMajor(),
                            gpu.computeCapabilityMinor()
                        );
                    }
                } else if (gpu.totalDeviceMemoryInBytes() < MIN_DEVICE_MEMORY_IN_BYTES) {
                    if (logError) {
                        LOG.warn(
                            "GPU [{}] does not have minimum memory required (required: [{}], found: [{}])",
                            gpu.name(),
                            MIN_DEVICE_MEMORY_IN_BYTES,
                            gpu.totalDeviceMemoryInBytes()
                        );
                    }
                } else {
                    if (logError) {
                        LOG.info("Found compatible GPU [{}] (id: [{}])", gpu.name(), gpu.gpuId());
                    }
                    return true;
                }
            }

        } catch (UnsupportedOperationException uoe) {
            if (logError) {
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
            }
        } catch (Throwable t) {
            if (logError) {
                if (t instanceof ExceptionInInitializerError ex) {
                    t = ex.getCause();
                }
                LOG.warn("Exception occurred during creation of cuvs resources", t);
            }
        }
        return false;
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
}
