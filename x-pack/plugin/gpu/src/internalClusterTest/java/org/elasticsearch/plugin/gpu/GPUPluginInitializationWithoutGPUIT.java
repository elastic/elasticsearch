/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.gpu;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldTypeTests;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.gpu.GPUPlugin;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GPUPluginInitializationWithoutGPUIT extends ESIntegTestCase {

    static {
        TestCuVSServiceProvider.mockedGPUInfoProvider = p -> new TestCuVSServiceProvider.TestGPUInfoProvider(List.of());
        ;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GPUPlugin.class);
    }

    public void testFFOff() {
        assumeFalse("GPU_FORMAT feature flag disabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(GPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        var format = vectorsFormatProvider.getKnnVectorsFormat(null, null, null);
        assertNull(format);
    }

    public void testFFOffIndexSettingNotSupported() {
        assumeFalse("GPU_FORMAT feature flag disabled", GPUPlugin.GPU_FORMAT.isEnabled());
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex(
                "index1",
                Settings.builder().put(GPUPlugin.VECTORS_INDEXING_USE_GPU_SETTING.getKey(), GPUPlugin.GpuMode.TRUE).build()
            )
        );
        assertThat(exception.getMessage(), containsString("unknown setting [index.vectors.indexing.use_gpu]"));
    }

    public void testIndexSettingOnGPUNotSupportedThrows() {
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(GPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1", Settings.builder().put(GPUPlugin.VECTORS_INDEXING_USE_GPU_SETTING.getKey(), GPUPlugin.GpuMode.TRUE).build());
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomGpuSupportedIndexOptions();

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> vectorsFormatProvider.getKnnVectorsFormat(
                settings,
                indexOptions,
                DenseVectorFieldTypeTests.randomGPUSupportedSimilarity(indexOptions.getType())
            )
        );
        assertThat(
            ex.getMessage(),
            equalTo("[index.vectors.indexing.use_gpu] was set to [true], but GPU resources are not accessible on the node.")
        );
    }

    public void testIndexSettingAutoGPUNotSupported() {
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());

        GPUPlugin gpuPlugin = internalCluster().getInstance(GPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1", Settings.builder().put(GPUPlugin.VECTORS_INDEXING_USE_GPU_SETTING.getKey(), GPUPlugin.GpuMode.AUTO).build());
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomGpuSupportedIndexOptions();

        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            DenseVectorFieldTypeTests.randomGPUSupportedSimilarity(indexOptions.getType())
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
