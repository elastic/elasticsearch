/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.gpu;

import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.gpu.codec.GPUVectorsFormat;

public class GPUPlugin extends Plugin implements MapperPlugin {

    public static final FeatureFlag GPU_FORMAT = new FeatureFlag("gpu_format");

    @Override
    public VectorsFormatProvider getVectorsFormatProvider() {
        // TODO check indexSettings if it allows for GPU indexing
        return (indexSettings, indexOptions) -> {
            if (GPU_FORMAT.isEnabled()
                && GPUVectorsFormat.cuVSResourcesOrNull(false) != null
                && indexOptions.getType() == DenseVectorFieldMapper.VectorIndexType.HNSW) {
                return new GPUVectorsFormat();
            } else {
                return null;
            }
        };
    }
}
