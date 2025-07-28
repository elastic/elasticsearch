/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.gpu;

import com.nvidia.cuvs.CuVSResources;

import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.gpu.codec.GPUVectorsFormat;

public class GPUPlugin extends Plugin implements MapperPlugin {

    public static final FeatureFlag GPU_FORMAT = new FeatureFlag("gpu_format");

    @Override
    public VectorsFormatProvider getVectorsFormatProvider() {
        return (indexSettings, indexOptions) -> {
            if (GPU_FORMAT.isEnabled()) {
                IndexSettings.GpuMode gpuMode = indexSettings.getValue(IndexSettings.VECTORS_INDEXING_USE_GPU_SETTING);
                if (gpuMode == IndexSettings.GpuMode.TRUE) {
                    CuVSResources resources = GPUVectorsFormat.cuVSResourcesOrNull(true);
                    if (resources == null) {
                        throw new IllegalArgumentException(
                            "[index.vectors.indexing.use_gpu] was set to [true], but GPU resources are not accessible on the node."
                        );
                    } else {
                        return new GPUVectorsFormat();
                    }
                }
                if ((gpuMode == IndexSettings.GpuMode.AUTO) && GPUVectorsFormat.cuVSResourcesOrNull(false) != null) {
                    return new GPUVectorsFormat();
                }
            }
            return null;
        };
    }
}
