/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.gpu;

import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

import static org.elasticsearch.xpack.gpu.GPUIndexType.GPU_INDEX_TYPE;
import static org.elasticsearch.xpack.gpu.GPUIndexType.GPU_INDEX_TYPE_NAME;

public class GPUPlugin extends Plugin implements MapperPlugin {

    public static final FeatureFlag GPU_FORMAT = new FeatureFlag("gpu_format");

    public Map<String, DenseVectorFieldMapper.VectorIndexType> getDenseVectorIndexTypes() {
        if (GPU_FORMAT.isEnabled()) {
            return Map.of(GPU_INDEX_TYPE_NAME, GPU_INDEX_TYPE);
        } else {
            return Map.of();
        }
    }
}
