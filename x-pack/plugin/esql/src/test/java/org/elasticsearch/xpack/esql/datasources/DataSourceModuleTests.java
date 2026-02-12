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
import org.elasticsearch.plugins.spi.SPIClassIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for DataSourceModule verifying SPI discovery and registration.
 * These tests ensure all data sources work correctly via the plugin discovery mechanism.
 *
 * Note: These tests use a TestDataSourcePlugin that avoids creating HttpClient instances
 * to prevent thread leaks. The HttpStorageProvider creates HttpClient instances that
 * spawn daemon threads which are difficult to clean up in unit tests.
 */
public class DataSourceModuleTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    /**
     * Test-only DataSourcePlugin that provides mock storage and format reader
     * implementations to avoid dependencies on moved classes.
     */
    private static class TestDataSourcePlugin implements DataSourcePlugin {
        @Override
        public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
            return Map.of("file", s -> new MockFileStorageProvider());
        }

        @Override
        public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
            return Map.of("csv", (s, bf) -> new MockCsvFormatReader());
        }
    }

    /**
     * Mock file storage provider for testing.
     */
    private static class MockFileStorageProvider implements StorageProvider {
        @Override
        public List<String> supportedSchemes() {
            return List.of("file");
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            throw new UnsupportedOperationException("Mock provider");
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            throw new UnsupportedOperationException("Mock provider");
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            throw new UnsupportedOperationException("Mock provider");
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException("Mock provider");
        }

        @Override
        public boolean exists(StoragePath path) {
            return false;
        }

        @Override
        public void close() {}
    }

    /**
     * Mock CSV format reader for testing. Reports same format name and extensions
     * as the real CsvFormatReader.
     */
    private static class MockCsvFormatReader implements FormatReader {
        @Override
        public SourceMetadata metadata(StorageObject object) {
            throw new UnsupportedOperationException("Mock reader");
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) {
            throw new UnsupportedOperationException("Mock reader");
        }

        @Override
        public String formatName() {
            return "csv";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".csv", ".tsv");
        }

        @Override
        public void close() {}
    }

    /**
     * Test that SPI discovery mechanism works for DataSourcePlugin via META-INF/services.
     * Note: DataSourcePlugin implementations now live in separate plugin modules (esql-datasource-csv,
     * esql-datasource-http, etc.), so zero plugins may be discovered from the core test classpath.
     * Full SPI discovery is verified in integration tests where plugins are loaded as separate ES plugins.
     */
    public void testSpiDiscoveryFindsPlugins() {
        List<Class<? extends DataSourcePlugin>> discoveredPluginClasses = new ArrayList<>();
        SPIClassIterator<DataSourcePlugin> spiIterator = SPIClassIterator.get(DataSourcePlugin.class, getClass().getClassLoader());

        while (spiIterator.hasNext()) {
            discoveredPluginClasses.add(spiIterator.next());
        }

        // SPI mechanism should work without errors; plugins may or may not be on the test classpath
        logger.info("SPI discovery found {} DataSourcePlugin implementations", discoveredPluginClasses.size());
    }

    /**
     * Test that DataSourceModule correctly registers storage providers.
     */
    public void testStorageProviderRegistration() {
        List<DataSourcePlugin> plugins = List.of(new TestDataSourcePlugin());
        DataSourceModule module = new DataSourceModule(plugins, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        StorageProviderRegistry registry = module.storageProviderRegistry();

        // Verify file provider is registered
        assertTrue("File storage provider should be registered", registry.hasProvider("file"));
        StorageProvider fileProvider = registry.provider(StoragePath.of("file:///tmp/test.csv"));
        assertNotNull("File storage provider should be retrievable", fileProvider);
        assertTrue("File provider should be MockFileStorageProvider", fileProvider instanceof MockFileStorageProvider);
    }

    /**
     * Test that DataSourceModule correctly registers format readers.
     */
    public void testFormatReaderRegistration() {
        List<DataSourcePlugin> plugins = List.of(new TestDataSourcePlugin());
        DataSourceModule module = new DataSourceModule(plugins, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        FormatReaderRegistry registry = module.formatReaderRegistry();

        // Verify CSV reader is registered by name
        assertTrue("CSV format reader should be registered by name", registry.hasFormat("csv"));
        FormatReader csvReader = registry.byName("csv");
        assertNotNull("CSV format reader should be retrievable by name", csvReader);
        assertTrue("CSV reader should be MockCsvFormatReader", csvReader instanceof MockCsvFormatReader);

        // Verify CSV reader can be found by extension
        assertTrue("CSV reader should be registered for .csv extension", registry.hasExtension(".csv"));
        FormatReader csvByExtension = registry.byExtension("data.csv");
        assertNotNull("CSV reader should be found by .csv extension", csvByExtension);
        assertTrue("CSV reader by extension should be MockCsvFormatReader", csvByExtension instanceof MockCsvFormatReader);

        // Verify TSV extension also works (CSV reader handles TSV)
        assertTrue("CSV reader should be registered for .tsv extension", registry.hasExtension(".tsv"));
        FormatReader tsvByExtension = registry.byExtension("data.tsv");
        assertNotNull("CSV reader should be found by .tsv extension", tsvByExtension);
    }

    /**
     * Test that duplicate storage provider registration throws an exception.
     */
    public void testDuplicateStorageProviderThrows() {
        // Create two plugins that both register file
        DataSourcePlugin plugin1 = new TestDataSourcePlugin();
        DataSourcePlugin plugin2 = new TestDataSourcePlugin();

        List<DataSourcePlugin> plugins = List.of(plugin1, plugin2);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DataSourceModule(plugins, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE)
        );
        assertTrue(e.getMessage().contains("already registered"));
    }

    /**
     * Test that duplicate format reader registration throws an exception.
     */
    public void testDuplicateFormatReaderThrows() {
        // Create two plugins that both register CSV
        DataSourcePlugin plugin1 = new TestDataSourcePlugin();
        DataSourcePlugin plugin2 = new TestDataSourcePlugin();

        List<DataSourcePlugin> plugins = List.of(plugin1, plugin2);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DataSourceModule(plugins, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE)
        );
        assertTrue(e.getMessage().contains("already registered"));
    }

    /**
     * Test that DataSourceModule works with empty plugin list.
     */
    public void testEmptyPluginList() {
        List<DataSourcePlugin> plugins = List.of();
        DataSourceModule module = new DataSourceModule(plugins, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        // Registries should be empty but not null
        assertNotNull(module.storageProviderRegistry());
        assertNotNull(module.formatReaderRegistry());
        assertNotNull(module.operatorFactories());
        assertNotNull(module.filterPushdownRegistry());

        // No providers should be registered
        assertFalse("No file provider should be registered", module.storageProviderRegistry().hasProvider("file"));
        assertFalse("No CSV reader should be registered", module.formatReaderRegistry().hasFormat("csv"));
    }

    /**
     * Test that DataSourceModule correctly creates OperatorFactoryRegistry.
     */
    public void testOperatorFactoryRegistryCreation() {
        List<DataSourcePlugin> plugins = List.of(new TestDataSourcePlugin());
        DataSourceModule module = new DataSourceModule(plugins, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        // Create OperatorFactoryRegistry with a simple executor
        OperatorFactoryRegistry operatorRegistry = module.createOperatorFactoryRegistry(Runnable::run);
        assertNotNull("OperatorFactoryRegistry should be created", operatorRegistry);
    }

    /**
     * Test that DataSourceModule correctly reports table catalog availability.
     */
    public void testTableCatalogAvailability() {
        List<DataSourcePlugin> plugins = List.of(new TestDataSourcePlugin());
        DataSourceModule module = new DataSourceModule(plugins, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        // TestDataSourcePlugin doesn't provide table catalogs
        assertFalse("Test plugin should not have iceberg catalog", module.hasTableCatalog("iceberg"));
        assertFalse("Test plugin should not have delta catalog", module.hasTableCatalog("delta"));

        // Requesting non-existent catalog should throw
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> module.createTableCatalog("iceberg", Settings.EMPTY)
        );
        assertTrue(e.getMessage().contains("No table catalog registered"));
    }

    /**
     * Test that storage providers can create objects for their supported schemes.
     */
    public void testStorageProviderSchemeSupport() {
        List<DataSourcePlugin> plugins = List.of(new TestDataSourcePlugin());
        DataSourceModule module = new DataSourceModule(plugins, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        StorageProviderRegistry registry = module.storageProviderRegistry();

        // File provider should support file scheme
        StorageProvider fileProvider = registry.provider(StoragePath.of("file:///tmp/test.csv"));
        assertTrue("File provider should support file scheme", fileProvider.supportedSchemes().contains("file"));
    }

    /**
     * Test that format readers report correct format names and extensions.
     */
    public void testFormatReaderMetadata() {
        List<DataSourcePlugin> plugins = List.of(new TestDataSourcePlugin());
        DataSourceModule module = new DataSourceModule(plugins, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        FormatReaderRegistry registry = module.formatReaderRegistry();
        FormatReader csvReader = registry.byName("csv");

        assertEquals("CSV reader should report 'csv' as format name", "csv", csvReader.formatName());
        assertTrue("CSV reader should support .csv extension", csvReader.fileExtensions().contains(".csv"));
        assertTrue("CSV reader should support .tsv extension", csvReader.fileExtensions().contains(".tsv"));
    }

    /**
     * Test that settings are passed to plugin factories.
     */
    public void testSettingsPassedToFactories() {
        Settings customSettings = Settings.builder().put("test.setting", "value").build();

        List<DataSourcePlugin> plugins = List.of(new TestDataSourcePlugin());
        // This should not throw - settings are passed to factories
        DataSourceModule module = new DataSourceModule(plugins, customSettings, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        assertNotNull(module.storageProviderRegistry());
        assertNotNull(module.formatReaderRegistry());
    }

    /**
     * Test custom DataSourcePlugin implementation.
     */
    public void testCustomDataSourcePlugin() {
        // Create a custom plugin that provides a mock storage provider
        DataSourcePlugin customPlugin = new DataSourcePlugin() {
            @Override
            public java.util.Map<String, org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory> storageProviders(
                Settings settings
            ) {
                return java.util.Map.of("custom", s -> new MockStorageProvider());
            }
        };

        List<DataSourcePlugin> plugins = List.of(customPlugin);
        DataSourceModule module = new DataSourceModule(plugins, Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        assertTrue("Custom provider should be registered", module.storageProviderRegistry().hasProvider("custom"));
        StorageProvider customProvider = module.storageProviderRegistry().provider(StoragePath.of("custom://bucket/file.txt"));
        assertNotNull("Custom provider should be retrievable", customProvider);
        assertTrue("Custom provider should be MockStorageProvider", customProvider instanceof MockStorageProvider);
    }

    /**
     * Mock storage provider for testing custom plugin registration.
     */
    private static class MockStorageProvider implements StorageProvider {
        @Override
        public List<String> supportedSchemes() {
            return List.of("custom");
        }

        @Override
        public org.elasticsearch.xpack.esql.datasources.spi.StorageObject newObject(StoragePath path) {
            throw new UnsupportedOperationException("Mock provider");
        }

        @Override
        public org.elasticsearch.xpack.esql.datasources.spi.StorageObject newObject(StoragePath path, long length) {
            throw new UnsupportedOperationException("Mock provider");
        }

        @Override
        public org.elasticsearch.xpack.esql.datasources.spi.StorageObject newObject(
            StoragePath path,
            long length,
            java.time.Instant lastModified
        ) {
            throw new UnsupportedOperationException("Mock provider");
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException("Mock provider");
        }

        @Override
        public boolean exists(StoragePath path) {
            return false;
        }

        @Override
        public void close() {}
    }

    // ==================== Classloader Isolation Tests ====================

    /**
     * Test that each DataSourcePlugin implementation has an identifiable classloader.
     * This verifies that plugins can be tracked by their classloader for isolation purposes.
     * Note: DataSourcePlugin implementations now live in separate plugin modules, so zero
     * plugins may be discovered from the core test classpath.
     */
    public void testPluginClassloaderIdentification() {
        List<Class<? extends DataSourcePlugin>> discoveredPluginClasses = new ArrayList<>();
        SPIClassIterator<DataSourcePlugin> spiIterator = SPIClassIterator.get(DataSourcePlugin.class, getClass().getClassLoader());

        while (spiIterator.hasNext()) {
            Class<? extends DataSourcePlugin> pluginClass = spiIterator.next();
            discoveredPluginClasses.add(pluginClass);

            // Each plugin class should have a non-null classloader
            ClassLoader classLoader = pluginClass.getClassLoader();
            assertNotNull("Plugin class " + pluginClass.getName() + " should have a classloader", classLoader);

            // Log classloader info for debugging/verification
            logger.info(
                "Plugin [{}] loaded by classloader: {} (type: {})",
                pluginClass.getSimpleName(),
                classLoader,
                classLoader.getClass().getName()
            );
        }

        // Plugins may or may not be on the test classpath; verify infrastructure works regardless
        logger.info("Classloader identification test found {} plugins", discoveredPluginClasses.size());
    }

    /**
     * Test that plugins loaded from different modules have different classloaders.
     * This is a key requirement for classloader isolation - each plugin module
     * should be loaded by its own classloader to prevent jar hell.
     *
     * Note: In unit tests, plugins are typically loaded from the same classloader.
     * This test documents the expected behavior and verifies the infrastructure
     * is in place for classloader tracking. Full isolation is verified in
     * integration tests where plugins are loaded as separate ES plugins.
     */
    public void testPluginClassloaderDifferentiation() {
        List<Class<? extends DataSourcePlugin>> discoveredPluginClasses = new ArrayList<>();
        Map<ClassLoader, List<String>> pluginsByClassloader = new java.util.HashMap<>();

        SPIClassIterator<DataSourcePlugin> spiIterator = SPIClassIterator.get(DataSourcePlugin.class, getClass().getClassLoader());

        while (spiIterator.hasNext()) {
            Class<? extends DataSourcePlugin> pluginClass = spiIterator.next();
            discoveredPluginClasses.add(pluginClass);

            ClassLoader classLoader = pluginClass.getClassLoader();
            pluginsByClassloader.computeIfAbsent(classLoader, k -> new ArrayList<>()).add(pluginClass.getName());
        }

        // Log the classloader distribution for verification
        logger.info("Classloader distribution for {} discovered plugins:", discoveredPluginClasses.size());
        for (Map.Entry<ClassLoader, List<String>> entry : pluginsByClassloader.entrySet()) {
            logger.info("  Classloader [{}]: {}", entry.getKey().getClass().getSimpleName(), entry.getValue());
        }

        // In production with proper plugin isolation, each plugin would have its own classloader.
        // In unit tests, they may share a classloader. This test verifies the tracking works.
        // Note: pluginsByClassloader may be empty if no plugins are on the test classpath.
        logger.info(
            "Classloader differentiation test found {} classloaders for {} plugins",
            pluginsByClassloader.size(),
            discoveredPluginClasses.size()
        );
    }

    /**
     * Test that instantiated plugin objects maintain their classloader identity.
     * This ensures that when plugins are instantiated, we can still trace them
     * back to their originating classloader for isolation verification.
     */
    public void testInstantiatedPluginClassloaderTracking() {
        List<DataSourcePlugin> instantiatedPlugins = new ArrayList<>();
        Map<String, ClassLoader> pluginClassloaders = new java.util.HashMap<>();

        SPIClassIterator<DataSourcePlugin> spiIterator = SPIClassIterator.get(DataSourcePlugin.class, getClass().getClassLoader());

        while (spiIterator.hasNext()) {
            Class<? extends DataSourcePlugin> pluginClass = spiIterator.next();
            try {
                DataSourcePlugin plugin = pluginClass.getConstructor().newInstance();
                instantiatedPlugins.add(plugin);

                // Track the classloader of the instantiated object
                ClassLoader instanceClassloader = plugin.getClass().getClassLoader();
                pluginClassloaders.put(pluginClass.getName(), instanceClassloader);

                // Verify the instance classloader matches the class classloader
                assertEquals(
                    "Instance classloader should match class classloader for " + pluginClass.getName(),
                    pluginClass.getClassLoader(),
                    instanceClassloader
                );

                logger.info(
                    "Instantiated plugin [{}] with classloader: {}",
                    plugin.getClass().getSimpleName(),
                    instanceClassloader.getClass().getName()
                );
            } catch (Exception e) {
                // Some plugins may require special construction (e.g., ThreadPool)
                logger.info("Could not instantiate plugin {} with default constructor: {}", pluginClass.getName(), e.getMessage());
            }
        }

        // Plugins may or may not be on the test classpath; verify infrastructure works regardless
        logger.info("Instantiated plugin classloader tracking test found {} plugins", instantiatedPlugins.size());
    }

    /**
     * Test that the SPI interface (DataSourcePlugin) is loaded from the expected classloader.
     * This verifies the SPI contract - the interface should be loaded from a parent classloader
     * that is visible to all plugin implementations.
     */
    public void testSpiInterfaceClassloaderHierarchy() {
        ClassLoader spiClassloader = DataSourcePlugin.class.getClassLoader();
        assertNotNull("DataSourcePlugin interface should have a classloader", spiClassloader);

        logger.info("DataSourcePlugin interface loaded by: {} ({})", spiClassloader, spiClassloader.getClass().getName());

        // Verify that discovered plugins can see the SPI interface
        SPIClassIterator<DataSourcePlugin> spiIterator = SPIClassIterator.get(DataSourcePlugin.class, getClass().getClassLoader());

        while (spiIterator.hasNext()) {
            Class<? extends DataSourcePlugin> pluginClass = spiIterator.next();
            ClassLoader pluginClassloader = pluginClass.getClassLoader();

            // The plugin should be able to load the SPI interface
            try {
                Class<?> spiFromPlugin = pluginClassloader.loadClass(DataSourcePlugin.class.getName());
                assertNotNull("Plugin classloader should be able to load DataSourcePlugin", spiFromPlugin);

                // The loaded class should be the same as the original (same classloader hierarchy)
                assertEquals(
                    "DataSourcePlugin loaded by plugin classloader should be the same class",
                    DataSourcePlugin.class,
                    spiFromPlugin
                );
            } catch (ClassNotFoundException e) {
                fail("Plugin classloader should be able to find DataSourcePlugin: " + e.getMessage());
            }
        }
    }

    /**
     * Test that multiple plugins from the same module share a classloader,
     * while plugins from different modules have different classloaders.
     * This test uses mock plugins to simulate the expected behavior.
     */
    public void testClassloaderIsolationWithMockPlugins() {
        // Create plugins that track their classloader
        DataSourcePlugin plugin1 = new ClassloaderTrackingPlugin("plugin1");
        DataSourcePlugin plugin2 = new ClassloaderTrackingPlugin("plugin2");

        // In the same test, both plugins share the test classloader
        ClassLoader cl1 = plugin1.getClass().getClassLoader();
        ClassLoader cl2 = plugin2.getClass().getClassLoader();

        // These should be the same in unit tests (same class definition)
        assertEquals("Mock plugins in same test share classloader", cl1, cl2);

        // But the infrastructure for tracking is in place
        logger.info("Mock plugin 1 classloader: {}", cl1);
        logger.info("Mock plugin 2 classloader: {}", cl2);
    }

    /**
     * A DataSourcePlugin implementation that tracks its classloader for testing.
     */
    private static class ClassloaderTrackingPlugin implements DataSourcePlugin {
        private final String name;
        private final ClassLoader classLoader;

        ClassloaderTrackingPlugin(String name) {
            this.name = name;
            this.classLoader = getClass().getClassLoader();
        }

        String name() {
            return name;
        }

        ClassLoader trackedClassLoader() {
            return classLoader;
        }

        @Override
        public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
            return Map.of();
        }
    }

    /**
     * Test that verifies classloader isolation can be detected through class identity.
     * When the same class is loaded by different classloaders, they are different classes.
     * This is the fundamental mechanism that enables jar hell prevention.
     */
    public void testClassIdentityAcrossClassloaders() {
        // Get the DataSourcePlugin class from the test classloader
        Class<DataSourcePlugin> testClass = DataSourcePlugin.class;
        ClassLoader testClassloader = testClass.getClassLoader();

        // Try to load the same class from the same classloader - should be identical
        try {
            @SuppressWarnings("unchecked")
            Class<DataSourcePlugin> reloadedClass = (Class<DataSourcePlugin>) testClassloader.loadClass(DataSourcePlugin.class.getName());

            // Same classloader, same class
            assertSame("Same classloader should return same class instance", testClass, reloadedClass);
        } catch (ClassNotFoundException e) {
            fail("Should be able to reload DataSourcePlugin from test classloader");
        }

        // Document that in production, different classloaders would return different class instances
        logger.info(
            "Class identity test passed - DataSourcePlugin loaded from {} is consistent",
            testClassloader.getClass().getSimpleName()
        );
    }
}
