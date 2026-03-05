/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.gpu.codec.ES92GpuHnswSQVectorsFormat;
import org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.InternalVectorFormatProviderPlugin;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;

import java.util.List;

public class GPUPlugin extends Plugin implements InternalVectorFormatProviderPlugin, ActionPlugin {

    private static final Logger log = LogManager.getLogger(GPUPlugin.class);

    public static final LicensedFeature.Momentary GPU_INDEXING_FEATURE = LicensedFeature.momentary(
        null,
        XPackField.GPU_VECTOR_INDEXING,
        License.OperationMode.ENTERPRISE
    );

    private final GpuMode gpuMode;

    public GPUPlugin(Settings settings) {
        this.gpuMode = VECTORS_INDEXING_USE_GPU_NODE_SETTING.get(settings);
    }

    /**
     * An enum for the tri-state value of the `vectors.indexing.use_gpu` setting.
     */
    public enum GpuMode {
        TRUE,
        FALSE,
        AUTO
    }

    /**
     * Node-level setting to control whether to use GPU for vectors indexing across the node.
     * This is a static, node-scoped setting.
     *
     * If unset or "auto", an automatic decision is made based on the presence of GPU and necessary libraries.
     * If set to <code>true</code>, GPU must be used for vectors indexing, and if GPU or necessary libraries are not available,
     * the node will refuse to start and throw an exception.
     * If set to <code>false</code>, GPU will not be used for vectors indexing.
     */
    public static final Setting<GpuMode> VECTORS_INDEXING_USE_GPU_NODE_SETTING = Setting.enumSetting(
        GpuMode.class,
        "vectors.indexing.use_gpu",
        GpuMode.AUTO,
        Setting.Property.NodeScope
    );

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(VECTORS_INDEXING_USE_GPU_NODE_SETTING);
    }

    @Override
    public List<ActionPlugin.ActionHandler> getActions() {
        return List.of(
            new ActionHandler(XPackUsageFeatureAction.GPU_VECTOR_INDEXING, GpuUsageTransportAction.class),
            new ActionHandler(XPackInfoFeatureAction.GPU_VECTOR_INDEXING, GpuInfoTransportAction.class),
            new ActionHandler(GpuStatsAction.INSTANCE, TransportGpuStatsAction.class)
        );
    }

    // Allow tests to override the license state
    protected boolean isGpuIndexingFeatureAllowed() {
        var licenseState = XPackPlugin.getSharedLicenseState();
        return licenseState != null && GPU_INDEXING_FEATURE.check(licenseState);
    }

    @Override
    public List<BootstrapCheck> getBootstrapChecks() {
        return List.of(new GpuModeBootstrapCheck());
    }

    /**
     * Bootstrap check to ensure GPU is available when vectors.indexing.use_gpu is set to true.
     * Refuses to start the node if GPU support is required but not available.
     */
    static class GpuModeBootstrapCheck implements BootstrapCheck {
        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            Settings settings = context.settings();
            GpuMode gpuMode = VECTORS_INDEXING_USE_GPU_NODE_SETTING.get(settings);

            if (gpuMode == GpuMode.TRUE && GPUSupport.isSupported() == false) {
                return BootstrapCheckResult.failure(
                    "vectors.indexing.use_gpu is set to [true], but GPU resources are not accessible on this node. Check logs for details."
                );
            }

            return BootstrapCheckResult.success();
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECKS;
        }
    }

    @Override
    public VectorsFormatProvider getVectorsFormatProvider() {
        return (indexSettings, indexOptions, similarity, elementType, mergingExecutorService, maxMergingWorkers) -> {
            if (vectorIndexAndElementTypeSupported(indexOptions.getType(), elementType) == false) {
                return null;
            }

            if (gpuMode == GpuMode.TRUE || (gpuMode == GpuMode.AUTO && GPUSupport.isSupported())) {
                assert GPUSupport.isSupported();
                if (isGpuIndexingFeatureAllowed()) {
                    return getVectorsFormat(indexOptions, similarity);
                } else {
                    log.warn(
                        Strings.format(
                            "The current configuration supports GPU indexing, but it is not allowed by the current license. "
                                + "If this is intentional, it is possible to suppress this message by setting [%s] to FALSE",
                            VECTORS_INDEXING_USE_GPU_NODE_SETTING.getKey()
                        )
                    );
                    return null;
                }
            }
            return null;
        };
    }

    private boolean vectorIndexAndElementTypeSupported(
        DenseVectorFieldMapper.VectorIndexType type,
        DenseVectorFieldMapper.ElementType elementType
    ) {
        if (elementType != DenseVectorFieldMapper.ElementType.FLOAT) {
            return false;
        }
        return type == DenseVectorFieldMapper.VectorIndexType.HNSW || type == DenseVectorFieldMapper.VectorIndexType.INT8_HNSW;
    }

    private static KnnVectorsFormat getVectorsFormat(
        DenseVectorFieldMapper.DenseVectorIndexOptions indexOptions,
        DenseVectorFieldMapper.VectorSimilarity similarity
    ) {
        // TODO: cuvs 2025.12 will provide an API for converting HNSW CPU Params to Cagra params; use that instead
        if (indexOptions.getType() == DenseVectorFieldMapper.VectorIndexType.HNSW) {
            DenseVectorFieldMapper.HnswIndexOptions hnswIndexOptions = (DenseVectorFieldMapper.HnswIndexOptions) indexOptions;
            int efConstruction = hnswIndexOptions.efConstruction();
            int m = hnswIndexOptions.m();
            int gpuM = 2 + m * 2 / 3;
            int gpuEfConstruction = m + m * efConstruction / 256;
            return new ES92GpuHnswVectorsFormat(gpuM, gpuEfConstruction);
        } else if (indexOptions.getType() == DenseVectorFieldMapper.VectorIndexType.INT8_HNSW) {
            if (similarity == DenseVectorFieldMapper.VectorSimilarity.MAX_INNER_PRODUCT) {
                throw new IllegalArgumentException(
                    "GPU vector indexing does not support ["
                        + similarity
                        + "] similarity for [int8_hnsw] index type. "
                        + "Instead, consider using ["
                        + DenseVectorFieldMapper.VectorSimilarity.COSINE
                        + "] or "
                        + " [hnsw] index type."
                );
            }
            DenseVectorFieldMapper.Int8HnswIndexOptions int8HnswIndexOptions = (DenseVectorFieldMapper.Int8HnswIndexOptions) indexOptions;
            int efConstruction = int8HnswIndexOptions.efConstruction();
            int m = int8HnswIndexOptions.m();
            int gpuM = 2 + m * 2 / 3;
            int gpuEfConstruction = m + m * efConstruction / 256;
            return new ES92GpuHnswSQVectorsFormat(gpuM, gpuEfConstruction, int8HnswIndexOptions.confidenceInterval(), 7, false);
        } else {
            throw new IllegalArgumentException(
                "GPU vector indexing is not supported on this vector type: [" + indexOptions.getType() + "]"
            );
        }
    }
}
