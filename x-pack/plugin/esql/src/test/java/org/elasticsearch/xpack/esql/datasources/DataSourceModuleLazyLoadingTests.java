/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests verifying per-plugin lazy loading behavior in DataSourceModule.
 * Uses spy plugins with AtomicBoolean flags to track when factory methods are called.
 */
public class DataSourceModuleLazyLoadingTests extends ESTestCase {

    private BlockFactory blockFactory;

    private static final AtomicBoolean SPY_STORAGE_FACTORY_CALLED = new AtomicBoolean(false);
    private static final AtomicBoolean SPY_FORMAT_FACTORY_CALLED = new AtomicBoolean(false);
    private static final AtomicBoolean OTHER_FORMAT_FACTORY_CALLED = new AtomicBoolean(false);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test"), BigArrays.NON_RECYCLING_INSTANCE);
        SPY_STORAGE_FACTORY_CALLED.set(false);
        SPY_FORMAT_FACTORY_CALLED.set(false);
        OTHER_FORMAT_FACTORY_CALLED.set(false);
    }

    public void testFactoryMethodsNotCalledAtConstruction() {
        List<DataSourcePlugin> plugins = List.of(new SpyStoragePlugin(), new SpyFormatPlugin(), new OtherFormatPlugin());
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);

        new DataSourceModule(plugins, capabilities, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        assertFalse("Storage factory should not be called at construction", SPY_STORAGE_FACTORY_CALLED.get());
        assertFalse("Spy format factory should not be called at construction", SPY_FORMAT_FACTORY_CALLED.get());
        assertFalse("Other format factory should not be called at construction", OTHER_FORMAT_FACTORY_CALLED.get());
    }

    public void testAccessingOneFormatDoesNotLoadOther() {
        List<DataSourcePlugin> plugins = List.of(new SpyFormatPlugin(), new OtherFormatPlugin());
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);

        DataSourceModule module = new DataSourceModule(
            plugins,
            capabilities,
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        module.formatReaderRegistry().byName("spy");

        assertTrue("Spy format factory should be called", SPY_FORMAT_FACTORY_CALLED.get());
        assertFalse("Other format factory should NOT be called", OTHER_FORMAT_FACTORY_CALLED.get());
    }

    public void testByExtensionOnlyLoadsRelevantPlugin() {
        List<DataSourcePlugin> plugins = List.of(new SpyFormatPlugin(), new OtherFormatPlugin());
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);

        DataSourceModule module = new DataSourceModule(
            plugins,
            capabilities,
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        module.formatReaderRegistry().byExtension("data.spy");

        assertTrue("Spy format factory should be called for .spy extension", SPY_FORMAT_FACTORY_CALLED.get());
        assertFalse("Other format factory should NOT be called for .spy extension", OTHER_FORMAT_FACTORY_CALLED.get());
    }

    public void testStorageProviderCreatedOnlyOnAccess() {
        List<DataSourcePlugin> plugins = List.of(new SpyStoragePlugin());
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);

        DataSourceModule module = new DataSourceModule(
            plugins,
            capabilities,
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        assertFalse("Storage factory should not be called yet", SPY_STORAGE_FACTORY_CALLED.get());

        module.storageProviderRegistry().provider(StoragePath.of("spy://bucket/file.txt"));

        assertTrue("Storage factory should be called after provider access", SPY_STORAGE_FACTORY_CALLED.get());
    }

    public void testCapabilitiesAvailableWithoutAnyFactoryLoading() {
        List<DataSourcePlugin> plugins = List.of(new SpyStoragePlugin(), new SpyFormatPlugin(), new OtherFormatPlugin());
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);

        DataSourceModule module = new DataSourceModule(
            plugins,
            capabilities,
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        DataSourceCapabilities caps = module.capabilities();
        assertTrue("Should support spy scheme", caps.supportsScheme("spy"));
        assertTrue("Should support spy format", caps.supportsFormat("spy"));
        assertTrue("Should support other format", caps.supportsFormat("other"));
        assertTrue("Should support .spy extension", caps.supportsExtension(".spy"));
        assertTrue("Should support .other extension", caps.supportsExtension(".other"));

        assertFalse("Storage factory should not be called", SPY_STORAGE_FACTORY_CALLED.get());
        assertFalse("Spy format factory should not be called", SPY_FORMAT_FACTORY_CALLED.get());
        assertFalse("Other format factory should not be called", OTHER_FORMAT_FACTORY_CALLED.get());
    }

    public void testUnsupportedSchemeRejectedWithoutFactoryLoading() {
        List<DataSourcePlugin> plugins = List.of(new SpyStoragePlugin(), new SpyFormatPlugin());
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);

        DataSourceModule module = new DataSourceModule(
            plugins,
            capabilities,
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        assertFalse("ftp scheme should not be supported", module.capabilities().supportsScheme("ftp"));
        assertFalse("hdfs scheme should not be supported", module.capabilities().supportsScheme("hdfs"));

        assertFalse("Storage factory should not be called", SPY_STORAGE_FACTORY_CALLED.get());
        assertFalse("Format factory should not be called", SPY_FORMAT_FACTORY_CALLED.get());
    }

    public void testStorageOnlyPluginNotRegisteredAsConnector() {
        List<DataSourcePlugin> plugins = List.of(new SpyStoragePlugin(), new SpyFormatPlugin());
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);

        DataSourceModule module = new DataSourceModule(
            plugins,
            capabilities,
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        // Storage-only plugin should NOT produce a LazyConnectorFactory entry
        for (Map.Entry<String, ExternalSourceFactory> entry : module.sourceFactories().entrySet()) {
            assertFalse(
                "sourceFactories should not contain LazyConnectorFactory for storage-only plugin, but found one at key ["
                    + entry.getKey()
                    + "]",
                entry.getValue() instanceof DataSourceModule.LazyConnectorFactory
            );
        }

        // But the storage provider should still be accessible
        assertTrue("spy scheme should have a storage provider", module.storageProviderRegistry().hasProvider("spy"));
        assertFalse("Storage factory should not be called yet", SPY_STORAGE_FACTORY_CALLED.get());
    }

    public void testConnectorSchemesInCapabilities() {
        DataSourcePlugin connectorPlugin = new DataSourcePlugin() {
            @Override
            public Set<String> supportedConnectorSchemes() {
                return Set.of("flight", "grpc");
            }

            @Override
            public Map<String, ConnectorFactory> connectors(Settings settings) {
                return Map.of();
            }
        };

        List<DataSourcePlugin> plugins = List.of(connectorPlugin);
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);

        assertTrue("flight should be in capabilities", capabilities.supportsScheme("flight"));
        assertTrue("grpc should be in capabilities", capabilities.supportsScheme("grpc"));
    }

    public void testMultiFormatWithExtensionsRejected() {
        DataSourcePlugin badPlugin = new DataSourcePlugin() {
            @Override
            public Set<String> supportedFormats() {
                return Set.of("fmt1", "fmt2");
            }

            @Override
            public Set<String> supportedExtensions() {
                return Set.of(".ext1");
            }

            @Override
            public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
                return Map.of(
                    "fmt1",
                    (s, bf) -> new StubFormatReader("fmt1", List.of(".ext1")),
                    "fmt2",
                    (s, bf) -> new StubFormatReader("fmt2", List.of(".ext1"))
                );
            }
        };

        List<DataSourcePlugin> plugins = List.of(badPlugin);
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DataSourceModule(plugins, capabilities, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE)
        );
        assertTrue(e.getMessage().contains("multiple formats"));
    }

    // ===== Spy plugin implementations =====

    private static class SpyStoragePlugin implements DataSourcePlugin {
        @Override
        public Set<String> supportedSchemes() {
            return Set.of("spy");
        }

        @Override
        public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
            SPY_STORAGE_FACTORY_CALLED.set(true);
            return Map.of("spy", s -> new StubStorageProvider());
        }
    }

    private static class SpyFormatPlugin implements DataSourcePlugin {
        @Override
        public Set<String> supportedFormats() {
            return Set.of("spy");
        }

        @Override
        public Set<String> supportedExtensions() {
            return Set.of(".spy");
        }

        @Override
        public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
            SPY_FORMAT_FACTORY_CALLED.set(true);
            return Map.of("spy", (s, bf) -> new StubFormatReader("spy", List.of(".spy")));
        }
    }

    private static class OtherFormatPlugin implements DataSourcePlugin {
        @Override
        public Set<String> supportedFormats() {
            return Set.of("other");
        }

        @Override
        public Set<String> supportedExtensions() {
            return Set.of(".other");
        }

        @Override
        public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
            OTHER_FORMAT_FACTORY_CALLED.set(true);
            return Map.of("other", (s, bf) -> new StubFormatReader("other", List.of(".other")));
        }
    }

    // ===== Stub implementations =====

    private static class StubStorageProvider implements StorageProvider {
        @Override
        public List<String> supportedSchemes() {
            return List.of("spy");
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            throw new UnsupportedOperationException("Stub");
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            throw new UnsupportedOperationException("Stub");
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            throw new UnsupportedOperationException("Stub");
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException("Stub");
        }

        @Override
        public boolean exists(StoragePath path) {
            return false;
        }

        @Override
        public void close() {}
    }

    private static class StubFormatReader implements FormatReader {
        private final String name;
        private final List<String> extensions;

        StubFormatReader(String name, List<String> extensions) {
            this.name = name;
            this.extensions = extensions;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            throw new UnsupportedOperationException("Stub");
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) {
            throw new UnsupportedOperationException("Stub");
        }

        @Override
        public String formatName() {
            return name;
        }

        @Override
        public List<String> fileExtensions() {
            return extensions;
        }

        @Override
        public void close() {}
    }
}
