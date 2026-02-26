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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldTypeTests;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.gpu.TestVectorsFormatUtils.randomGPUSupportedSimilarity;

public class GPUPluginInitializationWithoutGPUIT extends ESIntegTestCase {

    static {
        TestCuVSServiceProvider.mockedGPUInfoProvider = p -> new TestCuVSServiceProvider.TestGPUInfoProvider(List.of());
    }

    public static class TestGPUPlugin extends GPUPlugin {
        public TestGPUPlugin() {
            super(Settings.EMPTY);
        }

        @Override
        public List<ActionPlugin.ActionHandler> getActions() {
            // Skip registering xpack usage/info actions in this test as they require XPackLicenseState
            return List.of();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestGPUPlugin.class);
    }

    public void testAutoModeWithoutGPU() {
        TestGPUPlugin gpuPlugin = internalCluster().getInstance(TestGPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1");
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomGpuSupportedIndexOptions();

        // With AUTO mode (default) and no GPU, should return null (fallback to CPU)
        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            randomGPUSupportedSimilarity(indexOptions.getType()),
            DenseVectorFieldMapper.ElementType.FLOAT,
            null,
            1
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
