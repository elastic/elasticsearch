/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import com.nvidia.cuvs.GPUInfo;
import com.nvidia.cuvs.GPUInfoProvider;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldTypeTests;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.gpu.TestVectorsFormatUtils.randomGPUSupportedSimilarity;

public class GPUPluginInitializationWithGPUIT extends ESIntegTestCase {

    static {
        TestCuVSServiceProvider.mockedGPUInfoProvider = p -> new TestCuVSServiceProvider.TestGPUInfoProvider(
            List.of(
                new GPUInfo(
                    0,
                    "TestGPU",
                    8 * 1024 * 1024 * 1024L,
                    GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MAJOR,
                    GPUInfoProvider.MIN_COMPUTE_CAPABILITY_MINOR,
                    true,
                    true
                )
            )
        );
    }

    private static boolean isGpuIndexingFeatureAllowed = true;
    private static GPUPlugin.GpuMode gpuMode = GPUPlugin.GpuMode.AUTO;

    public static class TestGPUPlugin extends GPUPlugin {

        public TestGPUPlugin() {
            super(Settings.builder().put("vectors.indexing.use_gpu", gpuMode.name()).build());
        }

        @Override
        protected boolean isGpuIndexingFeatureAllowed() {
            return GPUPluginInitializationWithGPUIT.isGpuIndexingFeatureAllowed;
        }
    }

    @After
    public void reset() {
        isGpuIndexingFeatureAllowed = true;
        gpuMode = GPUPlugin.GpuMode.AUTO;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestGPUPlugin.class);
    }

    // Feature flag disabled tests
    public void testFFOff() {
        assumeFalse("GPU_FORMAT feature flag disabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(TestGPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        var format = vectorsFormatProvider.getKnnVectorsFormat(null, null, null, null);
        assertNull(format);
    }

    // AUTO mode tests
    public void testAutoModeSupportedVectorType() {
        gpuMode = GPUPlugin.GpuMode.AUTO;
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(TestGPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1");
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomGpuSupportedIndexOptions();

        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            randomGPUSupportedSimilarity(indexOptions.getType()),
            // TODO add other type support
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        assertNotNull(format);
    }

    public void testAutoModeUnsupportedVectorType() {
        gpuMode = GPUPlugin.GpuMode.AUTO;
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(TestGPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1");
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomFlatIndexOptions();

        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            randomGPUSupportedSimilarity(indexOptions.getType()),
            // TODO add other type support
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        assertNull(format);
    }

    public void testAutoModeLicenseNotSupported() {
        gpuMode = GPUPlugin.GpuMode.AUTO;
        isGpuIndexingFeatureAllowed = false;
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(TestGPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1");
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomGpuSupportedIndexOptions();

        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            randomGPUSupportedSimilarity(indexOptions.getType()),
            // TODO add other type support
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        assertNull(format);
    }

    // TRUE mode tests
    public void testTrueModeSupportedVectorType() {
        gpuMode = GPUPlugin.GpuMode.TRUE;
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(TestGPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1");
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomGpuSupportedIndexOptions();

        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            randomGPUSupportedSimilarity(indexOptions.getType()),
            // TODO add other type support
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        assertNotNull(format);
    }

    public void testTrueModeUnsupportedVectorType() {
        gpuMode = GPUPlugin.GpuMode.TRUE;
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(TestGPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1");
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomFlatIndexOptions();

        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            randomGPUSupportedSimilarity(indexOptions.getType()),
            // TODO add other type support
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        assertNull(format);
    }

    public void testTrueModeLicenseNotSupported() {
        gpuMode = GPUPlugin.GpuMode.TRUE;
        isGpuIndexingFeatureAllowed = false;
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(TestGPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1");
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomGpuSupportedIndexOptions();

        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            randomGPUSupportedSimilarity(indexOptions.getType()),
            // TODO add other type support
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        assertNull(format);
    }

    // FALSE mode tests
    public void testFalseModeNeverUsesGpu() {
        gpuMode = GPUPlugin.GpuMode.FALSE;
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(TestGPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1");
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomGpuSupportedIndexOptions();

        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            randomGPUSupportedSimilarity(indexOptions.getType()),
            // TODO add other type support
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        assertNull(format);
    }

    private IndexSettings getIndexSettings() {
        ensureGreen("index1");
        IndexSettings settings = null;
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("index1"));
            if (indexService != null) {
                settings = indexService.getIndexSettings();
                break;
            }
        }
        assertNotNull(settings);
        return settings;
    }

}
