/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldTypeTests;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.gpu.TestVectorsFormatUtils.randomGPUSupportedSimilarity;
import static org.hamcrest.Matchers.equalTo;

public class GPUPluginInitializationUnsupportedIT extends ESIntegTestCase {

    static {
        // Mocks a cuvs-java UnsupportedProvider
        TestCuVSServiceProvider.mockedGPUInfoProvider = p -> { throw new UnsupportedOperationException("cuvs-java UnsupportedProvider"); };
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GPUPlugin.class);
    }

    public void testIndexSettingOnGPUSupportThrowsRethrows() {
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(GPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1", Settings.builder().put(GPUPlugin.VECTORS_INDEXING_USE_GPU_SETTING.getKey(), GPUPlugin.GpuMode.TRUE).build());
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomGpuSupportedIndexOptions();

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> vectorsFormatProvider.getKnnVectorsFormat(settings, indexOptions, randomGPUSupportedSimilarity(indexOptions.getType()))
        );
        assertThat(
            ex.getMessage(),
            equalTo("[index.vectors.indexing.use_gpu] was set to [true], but GPU resources are not accessible on the node.")
        );
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
