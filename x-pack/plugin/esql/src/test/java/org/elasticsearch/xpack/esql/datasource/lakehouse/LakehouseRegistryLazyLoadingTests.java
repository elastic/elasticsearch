/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.lakehouse;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatPlugin;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StoragePlugin;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.datasource.spi.CloseableIterator;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;

public class LakehouseRegistryLazyLoadingTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("test"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    public void testFactoriesNotCalledAtConstruction() {
        AtomicBoolean storageFactoryCalled = new AtomicBoolean(false);
        AtomicBoolean formatFactoryCalled = new AtomicBoolean(false);

        StoragePlugin storagePlugin = new SpyStoragePlugin("s3", storageFactoryCalled);
        FormatPlugin formatPlugin = new SpyFormatPlugin("parquet", List.of(".parquet"), formatFactoryCalled);

        new LakehouseRegistry(List.of(storagePlugin), List.of(formatPlugin), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        assertThat("Storage factory should not be called at construction", storageFactoryCalled.get(), is(false));
        assertThat("Format factory should not be called at construction", formatFactoryCalled.get(), is(false));
    }

    public void testAccessingOneFormatDoesNotLoadAnother() {
        AtomicBoolean parquetFactoryCalled = new AtomicBoolean(false);
        AtomicBoolean csvFactoryCalled = new AtomicBoolean(false);

        FormatPlugin parquetPlugin = new SpyFormatPlugin("parquet", List.of(".parquet"), parquetFactoryCalled);
        FormatPlugin csvPlugin = new SpyFormatPlugin("csv", List.of(".csv"), csvFactoryCalled);

        LakehouseRegistry registry = new LakehouseRegistry(
            List.of(),
            List.of(parquetPlugin, csvPlugin),
            List.of(),
            Settings.EMPTY,
            BLOCK_FACTORY
        );

        // Access parquet
        registry.formatReaderRegistry().byName("parquet");

        assertThat("Parquet factory should be called", parquetFactoryCalled.get(), is(true));
        assertThat("CSV factory should NOT be called", csvFactoryCalled.get(), is(false));
    }

    public void testByExtensionOnlyLoadsRelevantPlugin() {
        AtomicBoolean parquetFactoryCalled = new AtomicBoolean(false);
        AtomicBoolean csvFactoryCalled = new AtomicBoolean(false);

        FormatPlugin parquetPlugin = new SpyFormatPlugin("parquet", List.of(".parquet"), parquetFactoryCalled);
        FormatPlugin csvPlugin = new SpyFormatPlugin("csv", List.of(".csv"), csvFactoryCalled);

        LakehouseRegistry registry = new LakehouseRegistry(
            List.of(),
            List.of(parquetPlugin, csvPlugin),
            List.of(),
            Settings.EMPTY,
            BLOCK_FACTORY
        );

        // Access parquet by name first (to register extensions)
        registry.formatReaderRegistry().byName("parquet");

        // Access by extension
        FormatReader reader = registry.formatReaderRegistry().byExtension("data.parquet");
        assertEquals("parquet", reader.formatName());

        assertThat("Parquet factory should be called", parquetFactoryCalled.get(), is(true));
        assertThat("CSV factory should NOT be called", csvFactoryCalled.get(), is(false));
    }

    public void testStorageProviderCreatedOnlyOnFirstAccess() {
        AtomicBoolean factoryCalled = new AtomicBoolean(false);

        StoragePlugin plugin = new SpyStoragePlugin("s3", factoryCalled);

        LakehouseRegistry registry = new LakehouseRegistry(List.of(plugin), List.of(), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        assertThat("Factory should not be called yet", factoryCalled.get(), is(false));

        // Access the provider
        registry.storageProviderRegistry().provider(StoragePath.of("s3://bucket/key"));

        assertThat("Factory should now be called", factoryCalled.get(), is(true));
    }

    public void testHasProviderDoesNotTriggerCreation() {
        AtomicBoolean factoryCalled = new AtomicBoolean(false);

        StoragePlugin plugin = new SpyStoragePlugin("s3", factoryCalled);

        LakehouseRegistry registry = new LakehouseRegistry(List.of(plugin), List.of(), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        // hasProvider should NOT trigger factory
        assertThat(registry.storageProviderRegistry().hasProvider("s3"), is(true));
        assertThat("Factory should not be called by hasProvider", factoryCalled.get(), is(false));
    }

    public void testHasFormatDoesNotTriggerCreation() {
        AtomicBoolean factoryCalled = new AtomicBoolean(false);

        FormatPlugin plugin = new SpyFormatPlugin("parquet", List.of(".parquet"), factoryCalled);

        LakehouseRegistry registry = new LakehouseRegistry(List.of(), List.of(plugin), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        // hasFormat should NOT trigger factory
        assertThat(registry.formatReaderRegistry().hasFormat("parquet"), is(true));
        assertThat("Factory should not be called by hasFormat", factoryCalled.get(), is(false));
    }

    public void testSecondAccessReusesProvider() {
        AtomicBoolean factoryCalled = new AtomicBoolean(false);
        int[] callCount = { 0 };

        StoragePlugin plugin = new StoragePlugin() {
            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("s3", s -> {
                    factoryCalled.set(true);
                    callCount[0]++;
                    return new SpyStorageProvider("s3");
                });
            }
        };

        LakehouseRegistry registry = new LakehouseRegistry(List.of(plugin), List.of(), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        StorageProvider first = registry.storageProviderRegistry().provider(StoragePath.of("s3://bucket/key1"));
        StorageProvider second = registry.storageProviderRegistry().provider(StoragePath.of("s3://bucket/key2"));

        assertSame("Should reuse the same provider instance", first, second);
        assertEquals("Factory should be called exactly once", 1, callCount[0]);
    }

    // --- Spy implementations ---

    private static class SpyStoragePlugin implements StoragePlugin {
        private final String scheme;
        private final AtomicBoolean factoryCalled;

        SpyStoragePlugin(String scheme, AtomicBoolean factoryCalled) {
            this.scheme = scheme;
            this.factoryCalled = factoryCalled;
        }

        @Override
        public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
            return Map.of(scheme, s -> {
                factoryCalled.set(true);
                return new SpyStorageProvider(scheme);
            });
        }
    }

    private static class SpyFormatPlugin implements FormatPlugin {
        private final String formatName;
        private final List<String> extensions;
        private final AtomicBoolean factoryCalled;

        SpyFormatPlugin(String formatName, List<String> extensions, AtomicBoolean factoryCalled) {
            this.formatName = formatName;
            this.extensions = extensions;
            this.factoryCalled = factoryCalled;
        }

        @Override
        public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
            return Map.of(formatName, (s, bf) -> {
                factoryCalled.set(true);
                return new SpyFormatReader(formatName, extensions);
            });
        }
    }

    private static class SpyStorageProvider implements StorageProvider {
        private final String scheme;

        SpyStorageProvider(String scheme) {
            this.scheme = scheme;
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return null;
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return null;
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return null;
        }

        @Override
        public org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            return null;
        }

        @Override
        public boolean exists(StoragePath path) {
            return false;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of(scheme);
        }

        @Override
        public void close() {}
    }

    private static class SpyFormatReader implements FormatReader {
        private final String name;
        private final List<String> extensions;

        SpyFormatReader(String name, List<String> extensions) {
            this.name = name;
            this.extensions = extensions;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<org.elasticsearch.compute.data.Page> read(
            StorageObject object,
            List<String> projectedColumns,
            int batchSize
        ) {
            return null;
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
        public void close() throws IOException {}
    }
}
