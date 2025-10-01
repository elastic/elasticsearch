/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.gpu;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.InternalVectorFormatProviderPlugin;
import org.elasticsearch.xpack.gpu.codec.ES92GpuHnswSQVectorsFormat;
import org.elasticsearch.xpack.gpu.codec.ES92GpuHnswVectorsFormat;

import java.util.List;

public class GPUPlugin extends Plugin implements InternalVectorFormatProviderPlugin {

    public static final FeatureFlag GPU_FORMAT = new FeatureFlag("gpu_vectors_indexing");

    /**
     * An enum for the tri-state value of the `index.vectors.indexing.use_gpu` setting.
     */
    public enum GpuMode {
        TRUE,
        FALSE,
        AUTO
    }

    /**
     * Setting to control whether to use GPU for vectors indexing.
     * Currently only applicable for index_options.type: hnsw.
     *
     * If unset or "auto", an automatic decision is made based on the presence of GPU, necessary libraries, vectors' index type.
     * If set to <code>true</code>, GPU must be used for vectors indexing, and if GPU or necessary libraries are not available,
     * an exception will be thrown.
     * If set to <code>false</code>, GPU will not be used for vectors indexing.
     */
    public static final Setting<GpuMode> VECTORS_INDEXING_USE_GPU_SETTING = Setting.enumSetting(
        GpuMode.class,
        "index.vectors.indexing.use_gpu",
        GpuMode.AUTO,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    @Override
    public List<Setting<?>> getSettings() {
        if (GPU_FORMAT.isEnabled()) {
            return List.of(VECTORS_INDEXING_USE_GPU_SETTING);
        } else {
            return List.of();
        }
    }

    @Override
    public VectorsFormatProvider getVectorsFormatProvider() {
        return (indexSettings, indexOptions) -> {
            if (GPU_FORMAT.isEnabled()) {
                GpuMode gpuMode = indexSettings.getValue(VECTORS_INDEXING_USE_GPU_SETTING);
                if (gpuMode == GpuMode.TRUE) {
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
                if (gpuMode == GpuMode.AUTO && vectorIndexTypeSupported(indexOptions.getType()) && GPUSupport.isSupported(false)) {
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
                efConstruction = ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH; // default value for GPU graph construction is 128
            }
            return new ES92GpuHnswVectorsFormat(hnswIndexOptions.m(), efConstruction);
        } else if (indexOptions.getType() == DenseVectorFieldMapper.VectorIndexType.INT8_HNSW) {
            DenseVectorFieldMapper.Int8HnswIndexOptions int8HnswIndexOptions = (DenseVectorFieldMapper.Int8HnswIndexOptions) indexOptions;
            int efConstruction = int8HnswIndexOptions.efConstruction();
            if (efConstruction == HnswGraphBuilder.DEFAULT_BEAM_WIDTH) {
                efConstruction = ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH; // default value for GPU graph construction is 128
            }
            return new ES92GpuHnswSQVectorsFormat(
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
