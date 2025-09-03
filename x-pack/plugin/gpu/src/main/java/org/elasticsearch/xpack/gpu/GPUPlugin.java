/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.gpu;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.gpu.codec.ESGpuHnswSQVectorsFormat;
import org.elasticsearch.xpack.gpu.codec.ESGpuHnswVectorsFormat;

public class GPUPlugin extends Plugin implements MapperPlugin {

    public static final FeatureFlag GPU_FORMAT = new FeatureFlag("gpu_format");

    @Override
    public VectorsFormatProvider getVectorsFormatProvider() {
        return (indexSettings, indexOptions) -> {
            if (GPU_FORMAT.isEnabled()) {
                IndexSettings.GpuMode gpuMode = indexSettings.getValue(IndexSettings.VECTORS_INDEXING_USE_GPU_SETTING);
                if (gpuMode == IndexSettings.GpuMode.TRUE) {
                    if (vectorIndexTypeSupported(indexOptions.getType()) == false) {
                        throw new IllegalArgumentException(
                            "[index.vectors.indexing.use_gpu] doesn't support [index_options.type] of [" + indexOptions.getType() + "]."
                        );
                    }
                    if (GPUSupport.isSupported(true) == false) {
                        throw new IllegalArgumentException(
                            "[index.vectors.indexing.use_gpu] was set to [true], but GPU resources are not accessible on the node."
                        );
                    }
                    return getVectorsFormat(indexOptions);
                }
                if (gpuMode == IndexSettings.GpuMode.AUTO
                    && vectorIndexTypeSupported(indexOptions.getType())
                    && GPUSupport.isSupported(false)) {
                    return getVectorsFormat(indexOptions);
                }
            }
            return null;
        };
    }

    private boolean vectorIndexTypeSupported(DenseVectorFieldMapper.VectorIndexType type) {
        return type == DenseVectorFieldMapper.VectorIndexType.HNSW || type == DenseVectorFieldMapper.VectorIndexType.INT8_HNSW;
    }

    private static KnnVectorsFormat getVectorsFormat(DenseVectorFieldMapper.DenseVectorIndexOptions indexOptions) {
        if (indexOptions.getType() == DenseVectorFieldMapper.VectorIndexType.HNSW) {
            DenseVectorFieldMapper.HnswIndexOptions hnswIndexOptions = (DenseVectorFieldMapper.HnswIndexOptions) indexOptions;
            int efConstruction = hnswIndexOptions.efConstruction();
            if (efConstruction == HnswGraphBuilder.DEFAULT_BEAM_WIDTH) {
                efConstruction = ESGpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH; // default value for GPU graph construction is 128
            }
            return new ESGpuHnswVectorsFormat(hnswIndexOptions.m(), efConstruction);
        } else if (indexOptions.getType() == DenseVectorFieldMapper.VectorIndexType.INT8_HNSW) {
            DenseVectorFieldMapper.Int8HnswIndexOptions int8HnswIndexOptions = (DenseVectorFieldMapper.Int8HnswIndexOptions) indexOptions;
            int efConstruction = int8HnswIndexOptions.efConstruction();
            if (efConstruction == HnswGraphBuilder.DEFAULT_BEAM_WIDTH) {
                efConstruction = ESGpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH; // default value for GPU graph construction is 128
            }
            return new ESGpuHnswSQVectorsFormat(
                int8HnswIndexOptions.m(),
                efConstruction,
                int8HnswIndexOptions.confidenceInterval(),
                7,
                false
            );
        } else {
            throw new IllegalArgumentException(
                "GPU vector indexing is not supported on this vector type: [" + indexOptions.getType() + "]"
            );
        }
    }
}
