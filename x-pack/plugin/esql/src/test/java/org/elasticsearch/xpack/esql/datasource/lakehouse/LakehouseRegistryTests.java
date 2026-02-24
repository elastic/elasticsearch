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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class LakehouseRegistryTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("test"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    public void testStorageProviderRegistration() {
        StoragePlugin plugin = new StoragePlugin() {
            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("s3", s -> new MockStorageProvider("s3"));
            }
        };

        LakehouseRegistry registry = new LakehouseRegistry(List.of(plugin), List.of(), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        assertThat(registry.storageProviderRegistry().hasProvider("s3"), is(true));
        StorageProvider provider = registry.storageProviderRegistry().provider(StoragePath.of("s3://bucket/key"));
        assertThat(provider, is(notNullValue()));
    }

    public void testFormatReaderRegistrationByName() {
        FormatPlugin plugin = new FormatPlugin() {
            @Override
            public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
                return Map.of("parquet", (s, bf) -> new MockFormatReader("parquet", List.of(".parquet")));
            }
        };

        LakehouseRegistry registry = new LakehouseRegistry(List.of(), List.of(plugin), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        assertThat(registry.formatReaderRegistry().hasFormat("parquet"), is(true));
        FormatReader reader = registry.formatReaderRegistry().byName("parquet");
        assertThat(reader, is(notNullValue()));
        assertEquals("parquet", reader.formatName());
    }

    public void testFormatReaderRegistrationByExtension() {
        FormatPlugin plugin = new FormatPlugin() {
            @Override
            public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
                return Map.of("csv", (s, bf) -> new MockFormatReader("csv", List.of(".csv")));
            }
        };

        LakehouseRegistry registry = new LakehouseRegistry(List.of(), List.of(plugin), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        // First access by name to trigger lazy creation (which registers extensions)
        registry.formatReaderRegistry().byName("csv");
        FormatReader reader = registry.formatReaderRegistry().byExtension("data.csv");
        assertThat(reader, is(notNullValue()));
        assertEquals("csv", reader.formatName());
    }

    public void testMultipleStoragePlugins() {
        StoragePlugin s3Plugin = new StoragePlugin() {
            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("s3", s -> new MockStorageProvider("s3"));
            }
        };
        StoragePlugin gcsPlugin = new StoragePlugin() {
            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("gs", s -> new MockStorageProvider("gs"));
            }
        };

        LakehouseRegistry registry = new LakehouseRegistry(
            List.of(s3Plugin, gcsPlugin),
            List.of(),
            List.of(),
            Settings.EMPTY,
            BLOCK_FACTORY
        );

        assertThat(registry.storageProviderRegistry().hasProvider("s3"), is(true));
        assertThat(registry.storageProviderRegistry().hasProvider("gs"), is(true));
    }

    public void testMultipleFormatPlugins() {
        FormatPlugin parquetPlugin = new FormatPlugin() {
            @Override
            public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
                return Map.of("parquet", (s, bf) -> new MockFormatReader("parquet", List.of(".parquet")));
            }
        };
        FormatPlugin csvPlugin = new FormatPlugin() {
            @Override
            public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
                return Map.of("csv", (s, bf) -> new MockFormatReader("csv", List.of(".csv")));
            }
        };

        LakehouseRegistry registry = new LakehouseRegistry(
            List.of(),
            List.of(parquetPlugin, csvPlugin),
            List.of(),
            Settings.EMPTY,
            BLOCK_FACTORY
        );

        assertThat(registry.formatReaderRegistry().hasFormat("parquet"), is(true));
        assertThat(registry.formatReaderRegistry().hasFormat("csv"), is(true));
    }

    public void testEmptyPluginLists() {
        LakehouseRegistry registry = new LakehouseRegistry(List.of(), List.of(), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        assertThat(registry.storageProviderRegistry().hasProvider("s3"), is(false));
        assertThat(registry.formatReaderRegistry().hasFormat("parquet"), is(false));
    }

    public void testUnregisteredSchemeThrows() {
        LakehouseRegistry registry = new LakehouseRegistry(List.of(), List.of(), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.storageProviderRegistry().provider(StoragePath.of("s3://bucket/key"))
        );
        assertThat(e.getMessage(), containsString("No storage provider registered for scheme"));
    }

    public void testUnregisteredFormatThrows() {
        LakehouseRegistry registry = new LakehouseRegistry(List.of(), List.of(), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> registry.formatReaderRegistry().byName("parquet"));
        assertThat(e.getMessage(), containsString("No format reader registered for format"));
    }

    public void testSettingsPassthrough() {
        Settings nodeSettings = Settings.builder().put("custom.key", "custom-value").build();

        StoragePlugin plugin = new StoragePlugin() {
            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                assertEquals("custom-value", settings.get("custom.key"));
                return Map.of("test", s -> new MockStorageProvider("test"));
            }
        };

        LakehouseRegistry registry = new LakehouseRegistry(List.of(plugin), List.of(), List.of(), nodeSettings, BLOCK_FACTORY);

        assertThat(registry.settings(), is(nodeSettings));
        assertThat(registry.storageProviderRegistry().hasProvider("test"), is(true));
    }

    public void testCloseClosesProviders() throws IOException {
        MockStorageProvider[] providerHolder = new MockStorageProvider[1];
        StoragePlugin plugin = new StoragePlugin() {
            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("s3", s -> {
                    providerHolder[0] = new MockStorageProvider("s3");
                    return providerHolder[0];
                });
            }
        };

        LakehouseRegistry registry = new LakehouseRegistry(List.of(plugin), List.of(), List.of(), Settings.EMPTY, BLOCK_FACTORY);

        // Trigger lazy creation
        registry.storageProviderRegistry().provider(StoragePath.of("s3://bucket/key"));
        assertThat(providerHolder[0], is(notNullValue()));
        assertThat(providerHolder[0].closed, is(false));

        registry.close();
        assertThat(providerHolder[0].closed, is(true));
    }

    // --- Test helpers ---

    private static class MockStorageProvider implements StorageProvider {
        final String scheme;
        boolean closed = false;

        MockStorageProvider(String scheme) {
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
        public void close() {
            closed = true;
        }
    }

    private static class MockFormatReader implements FormatReader {
        private final String name;
        private final List<String> extensions;

        MockFormatReader(String name, List<String> extensions) {
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
        public void close() {}
    }
}
