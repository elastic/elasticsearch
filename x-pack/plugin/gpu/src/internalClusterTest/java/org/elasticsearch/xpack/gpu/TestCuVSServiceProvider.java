/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.CuVSResourcesInfo;
import com.nvidia.cuvs.GPUInfo;
import com.nvidia.cuvs.GPUInfoProvider;
import com.nvidia.cuvs.spi.CuVSProvider;
import com.nvidia.cuvs.spi.CuVSServiceProvider;

import java.util.List;
import java.util.function.Function;

public class TestCuVSServiceProvider extends CuVSServiceProvider {

    static Function<CuVSProvider, GPUInfoProvider> mockedGPUInfoProvider;

    @Override
    public CuVSProvider get(CuVSProvider builtin) {
        if (mockedGPUInfoProvider == null) {
            return builtin;
        }
        return new CuVSProviderDelegate(builtin) {
            @Override
            public GPUInfoProvider gpuInfoProvider() {
                return mockedGPUInfoProvider.apply(builtin);
            }
        };
    }

    static class TestGPUInfoProvider implements GPUInfoProvider {
        private final List<GPUInfo> gpuList;

        TestGPUInfoProvider(List<GPUInfo> gpuList) {
            this.gpuList = gpuList;
        }

        @Override
        public List<GPUInfo> availableGPUs() {
            return gpuList;
        }

        @Override
        public List<GPUInfo> compatibleGPUs() {
            return gpuList;
        }

        @Override
        public CuVSResourcesInfo getCurrentInfo(CuVSResources cuVSResources) {
            return null;
        }
    }
}
