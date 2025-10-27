/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.plugin.gpu;

import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.CuVSResourcesInfo;
import com.nvidia.cuvs.GPUInfo;
import com.nvidia.cuvs.GPUInfoProvider;
import com.nvidia.cuvs.spi.CuVSProvider;
import com.nvidia.cuvs.spi.CuVSServiceProvider;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldTypeTests;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.gpu.GPUPlugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class GPUPluginInitializationIT extends ESIntegTestCase {

    private static final Function<CuVSProvider, GPUInfoProvider> SUPPORTED_GPU_PROVIDER =
        p -> new TestCuVSServiceProvider.TestGPUInfoProvider(
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

    private static final Function<CuVSProvider, GPUInfoProvider> NO_GPU_PROVIDER = p -> new TestCuVSServiceProvider.TestGPUInfoProvider(
        List.of()
    );

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GPUPlugin.class);
    }

    public static class TestCuVSServiceProvider extends CuVSServiceProvider {

        static final Function<CuVSProvider, GPUInfoProvider> BUILTIN_GPU_INFO_PROVIDER = CuVSProvider::gpuInfoProvider;
        static Function<CuVSProvider, GPUInfoProvider> mockedGPUInfoProvider = BUILTIN_GPU_INFO_PROVIDER;

        @Override
        public CuVSProvider get(CuVSProvider builtin) {
            return new CuVSProviderDelegate(builtin) {
                @Override
                public GPUInfoProvider gpuInfoProvider() {
                    return mockedGPUInfoProvider.apply(builtin);
                }
            };
        }

        private static class TestGPUInfoProvider implements GPUInfoProvider {
            private final List<GPUInfo> gpuList;

            private TestGPUInfoProvider(List<GPUInfo> gpuList) {
                this.gpuList = gpuList;
            }

            @Override
            public List<GPUInfo> availableGPUs() {
                return gpuList;
            }

            @Override
            public List<GPUInfo> compatibleGPUs() {
                return gpuList;
            }

            @Override
            public CuVSResourcesInfo getCurrentInfo(CuVSResources cuVSResources) {
                return null;
            }
        }
    }

    @After
    public void disableMock() {
        TestCuVSServiceProvider.mockedGPUInfoProvider = TestCuVSServiceProvider.BUILTIN_GPU_INFO_PROVIDER;
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

    public void testFFOffGPUFormatNull() {
        assumeFalse("GPU_FORMAT feature flag disabled", GPUPlugin.GPU_FORMAT.isEnabled());
        TestCuVSServiceProvider.mockedGPUInfoProvider = SUPPORTED_GPU_PROVIDER;

        GPUPlugin gpuPlugin = internalCluster().getInstance(GPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1", Settings.EMPTY);
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomGpuSupportedIndexOptions();

        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            DenseVectorFieldTypeTests.randomGPUSupportedSimilarity(indexOptions.getType())
        );
        assertNull(format);
    }

    public void testIndexSettingOnIndexTypeSupportedGPUSupported() {
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());
        TestCuVSServiceProvider.mockedGPUInfoProvider = SUPPORTED_GPU_PROVIDER;

        GPUPlugin gpuPlugin = internalCluster().getInstance(GPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1", Settings.builder().put(GPUPlugin.VECTORS_INDEXING_USE_GPU_SETTING.getKey(), GPUPlugin.GpuMode.TRUE).build());
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomGpuSupportedIndexOptions();

        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            DenseVectorFieldTypeTests.randomGPUSupportedSimilarity(indexOptions.getType())
        );
        assertNotNull(format);
    }

    public void testIndexSettingOnIndexTypeNotSupportedThrows() {
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());
        TestCuVSServiceProvider.mockedGPUInfoProvider = SUPPORTED_GPU_PROVIDER;

        GPUPlugin gpuPlugin = internalCluster().getInstance(GPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1", Settings.builder().put(GPUPlugin.VECTORS_INDEXING_USE_GPU_SETTING.getKey(), GPUPlugin.GpuMode.TRUE).build());
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomFlatIndexOptions();

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> vectorsFormatProvider.getKnnVectorsFormat(
                settings,
                indexOptions,
                DenseVectorFieldTypeTests.randomGPUSupportedSimilarity(indexOptions.getType())
            )
        );
        assertThat(ex.getMessage(), startsWith("[index.vectors.indexing.use_gpu] doesn't support [index_options.type] of"));
    }

    public void testIndexSettingOnGPUNotSupportedThrows() {
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());
        TestCuVSServiceProvider.mockedGPUInfoProvider = NO_GPU_PROVIDER;

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

    public void testIndexSettingOnGPUSupportThrowsRethrows() {
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());
        // Mocks a cuvs-java UnsupportedProvider
        TestCuVSServiceProvider.mockedGPUInfoProvider = p -> { throw new UnsupportedOperationException("cuvs-java UnsupportedProvider"); };

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

    public void testIndexSettingAutoIndexTypeSupportedGPUSupported() {
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());
        TestCuVSServiceProvider.mockedGPUInfoProvider = SUPPORTED_GPU_PROVIDER;

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
        assertNotNull(format);
    }

    public void testIndexSettingAutoGPUNotSupported() {
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());
        TestCuVSServiceProvider.mockedGPUInfoProvider = NO_GPU_PROVIDER;

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

    public void testIndexSettingAutoIndexTypeNotSupported() {
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());
        TestCuVSServiceProvider.mockedGPUInfoProvider = SUPPORTED_GPU_PROVIDER;

        GPUPlugin gpuPlugin = internalCluster().getInstance(GPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1", Settings.builder().put(GPUPlugin.VECTORS_INDEXING_USE_GPU_SETTING.getKey(), GPUPlugin.GpuMode.AUTO).build());
        IndexSettings settings = getIndexSettings();
        final var indexOptions = DenseVectorFieldTypeTests.randomFlatIndexOptions();

        var format = vectorsFormatProvider.getKnnVectorsFormat(
            settings,
            indexOptions,
            DenseVectorFieldTypeTests.randomGPUSupportedSimilarity(indexOptions.getType())
        );
        assertNull(format);
    }

    public void testIndexSettingOff() {
        assumeTrue("GPU_FORMAT feature flag enabled", GPUPlugin.GPU_FORMAT.isEnabled());
        TestCuVSServiceProvider.mockedGPUInfoProvider = SUPPORTED_GPU_PROVIDER;

        GPUPlugin gpuPlugin = internalCluster().getInstance(GPUPlugin.class);
        VectorsFormatProvider vectorsFormatProvider = gpuPlugin.getVectorsFormatProvider();

        createIndex("index1", Settings.builder().put(GPUPlugin.VECTORS_INDEXING_USE_GPU_SETTING.getKey(), GPUPlugin.GpuMode.FALSE).build());
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
