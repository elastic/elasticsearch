/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.DataSourceCapabilities;
import org.elasticsearch.xpack.esql.datasources.DataSourceCredentials;
import org.elasticsearch.xpack.esql.datasources.DataSourceModule;
import org.elasticsearch.xpack.esql.datasources.TestConnectionResult;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.Split;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link DataSourceModule#testConnection} and the {@link ConnectorFactory#testConnection} default.
 */
public class TestDataSourceConnectionTests extends ESTestCase {

    // No real EncryptionService implementation is available in unit-test scope, and decryption is
    // never exercised here because all test data sources use empty settings with no encrypted secrets.
    private static final EncryptionService ENCRYPTION_SERVICE = mock(EncryptionService.class);

    private DataSourceModule buildModule(DataSourcePlugin plugin) {
        List<DataSourcePlugin> plugins = List.of(plugin);
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        return new DataSourceModule(
            plugins,
            DataSourceCapabilities.build(plugins),
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new DataSourceCredentials(ENCRYPTION_SERVICE),
            () -> false
        );
    }

    // ---- ConnectorFactory.testConnection default ----

    public void testConnectorFactoryDefaultOpensAndClosesConnector() throws IOException {
        AtomicBoolean opened = new AtomicBoolean(false);
        AtomicBoolean closed = new AtomicBoolean(false);

        Connector connector = new Connector() {
            @Override
            public ResultCursor execute(QueryRequest request, Split split) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };

        ConnectorFactory factory = new ConnectorFactory() {
            @Override
            public String type() {
                return "test";
            }

            @Override
            public boolean canHandle(String location) {
                return true;
            }

            @Override
            public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void validateConfig(String location, Map<String, Object> config) {}

            @Override
            public Connector open(Map<String, Object> config) {
                opened.set(true);
                return connector;
            }
        };

        factory.testConnection(Map.of());

        assertTrue("open() must be called", opened.get());
        assertTrue("close() must be called even on success", closed.get());
    }

    public void testConnectorFactoryDefaultSuppressesCloseException() throws IOException {
        // close() throwing after a successful open() must not surface as a connection failure.
        ConnectorFactory factory = new ConnectorFactory() {
            @Override
            public String type() {
                return "test";
            }

            @Override
            public boolean canHandle(String location) {
                return true;
            }

            @Override
            public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void validateConfig(String location, Map<String, Object> config) {}

            @Override
            public Connector open(Map<String, Object> config) {
                return new Connector() {
                    @Override
                    public ResultCursor execute(QueryRequest request, Split split) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() throws IOException {
                        throw new IOException("network reset on close");
                    }
                };
            }
        };

        // Must not throw — close() failure is suppressed.
        factory.testConnection(Map.of());
    }

    public void testConnectorFactoryDefaultThrowsOnNullConnector() {
        ConnectorFactory factory = new ConnectorFactory() {
            @Override
            public String type() {
                return "test";
            }

            @Override
            public boolean canHandle(String location) {
                return true;
            }

            @Override
            public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void validateConfig(String location, Map<String, Object> config) {}

            @Override
            public Connector open(Map<String, Object> config) {
                return null;
            }
        };

        // open() returning null must not silently succeed — IllegalStateException signals a false-positive.
        expectThrows(IllegalStateException.class, () -> factory.testConnection(Map.of()));
    }

    public void testConnectorFactoryDefaultPropagatesOpenException() {
        ConnectorFactory factory = new ConnectorFactory() {
            @Override
            public String type() {
                return "test";
            }

            @Override
            public boolean canHandle(String location) {
                return true;
            }

            @Override
            public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void validateConfig(String location, Map<String, Object> config) {}

            @Override
            public Connector open(Map<String, Object> config) {
                throw new RuntimeException("connection refused");
            }
        };

        RuntimeException ex = expectThrows(RuntimeException.class, () -> factory.testConnection(Map.of()));
        assertEquals("connection refused", ex.getMessage());
    }

    // ---- DataSourceModule.testConnection ----

    public void testModuleTestConnectionSucceedsForConnectorType() throws IOException {
        AtomicBoolean opened = new AtomicBoolean(false);
        AtomicBoolean closed = new AtomicBoolean(false);

        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Set<String> supportedConnectorSchemes() {
                return Set.of("flight");
            }

            @Override
            public Map<String, ConnectorFactory> connectors(Settings settings) {
                return Map.of("flight", new ConnectorFactory() {
                    @Override
                    public String type() {
                        return "flight";
                    }

                    @Override
                    public boolean canHandle(String location) {
                        return location != null && location.startsWith("flight://");
                    }

                    @Override
                    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void validateConfig(String location, Map<String, Object> config) {}

                    @Override
                    public Connector open(Map<String, Object> config) {
                        opened.set(true);
                        return new Connector() {
                            @Override
                            public ResultCursor execute(QueryRequest request, Split split) {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public void close() {
                                closed.set(true);
                            }
                        };
                    }
                });
            }
        };

        try (DataSourceModule module = buildModule(plugin)) {
            TestConnectionResult result = module.testConnection("flight", Map.of());
            assertThat(result, instanceOf(TestConnectionResult.Success.class));
        }

        assertTrue("connector must have been opened", opened.get());
        assertTrue("connector must have been closed", closed.get());
    }

    public void testModuleTestConnectionPropagatesConnectorFailure() {
        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Set<String> supportedConnectorSchemes() {
                return Set.of("flight");
            }

            @Override
            public Map<String, ConnectorFactory> connectors(Settings settings) {
                return Map.of("flight", new ConnectorFactory() {
                    @Override
                    public String type() {
                        return "flight";
                    }

                    @Override
                    public boolean canHandle(String location) {
                        return true;
                    }

                    @Override
                    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void validateConfig(String location, Map<String, Object> config) {}

                    @Override
                    public Connector open(Map<String, Object> config) {
                        throw new RuntimeException("auth failure");
                    }
                });
            }
        };

        try (DataSourceModule module = buildModule(plugin)) {
            TestConnectionResult result = module.testConnection("flight", Map.of());
            assertThat(result, instanceOf(TestConnectionResult.Failure.class));
            assertEquals("auth failure", ((TestConnectionResult.Failure) result).error());
        } catch (IOException e) {
            fail("unexpected IOException from module close: " + e.getMessage());
        }
    }

    public void testModuleTestConnectionReturnsUntestableForFormatType() {
        // The "file" type is always registered by DataSourceModule via the built-in FileSourceFactory
        // (a non-connector ExternalSourceFactory). It has no probe, so testConnection returns UNTESTABLE —
        // the type is valid, it simply cannot be probed.
        try (DataSourceModule module = buildModule(new DataSourcePlugin() {})) {
            TestConnectionResult result = module.testConnection("file", Map.of());
            assertThat(result, instanceOf(TestConnectionResult.Untestable.class));
        } catch (IOException e) {
            fail("unexpected IOException from module close: " + e.getMessage());
        }
    }

    public void testModuleTestConnectionThrowsForUnknownType() {
        DataSourcePlugin plugin = new DataSourcePlugin() {};

        try (DataSourceModule module = buildModule(plugin)) {
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> module.testConnection("unknown-type", Map.of())
            );
            assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("unknown-type"));
        } catch (IOException e) {
            fail("unexpected IOException from module close: " + e.getMessage());
        }
    }

    public void testModuleTestConnectionRoutesToStorageProviderFactory() throws IOException {
        AtomicBoolean probed = new AtomicBoolean(false);

        StorageProvider stub = mock(StorageProvider.class);
        StorageProviderFactory baseFactory = StorageProviderFactory.noConfigKeys(() -> stub);
        StorageProviderFactory factory = StorageProviderFactory.withTestConnection(baseFactory, config -> probed.set(true));

        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Set<String> supportedSchemes() {
                return Set.of("test-storage");
            }

            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("test-storage", factory);
            }
        };

        try (DataSourceModule module = buildModule(plugin)) {
            TestConnectionResult result = module.testConnection("test-storage", Map.of());
            assertThat(result, instanceOf(TestConnectionResult.Success.class));
        }

        assertTrue("factory testConnection must have been invoked", probed.get());
    }

    public void testModuleTestConnectionUsesTypeToSchemeMapping() throws IOException {
        // Verifies the mapped-lookup path: PUT type name ("logical") differs from URI scheme ("scheme").
        AtomicBoolean probed = new AtomicBoolean(false);

        StorageProvider stub = mock(StorageProvider.class);
        StorageProviderFactory baseFactory = StorageProviderFactory.noConfigKeys(() -> stub);
        StorageProviderFactory factory = StorageProviderFactory.withTestConnection(baseFactory, config -> probed.set(true));

        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Set<String> supportedSchemes() {
                return Set.of("scheme");
            }

            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("scheme", factory);
            }

            @Override
            public Map<String, String> testConnectionSchemes() {
                return Map.of("logical", "scheme");
            }
        };

        try (DataSourceModule module = buildModule(plugin)) {
            TestConnectionResult result = module.testConnection("logical", Map.of());
            assertThat(result, instanceOf(TestConnectionResult.Success.class));
        }

        assertTrue("factory testConnection must have been invoked via type-to-scheme mapping", probed.get());
    }

    public void testWithTestConnectionDelegatesCreateAndOverridesTestConnection() throws IOException {
        AtomicBoolean probed = new AtomicBoolean(false);
        StorageProvider sentinel = mock(StorageProvider.class);

        StorageProviderFactory base = new StorageProviderFactory() {
            @Override
            public StorageProvider create(Settings settings) {
                return sentinel;
            }

            @Override
            public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
                return Configured.empty(sentinel);
            }
        };

        StorageProviderFactory wrapped = StorageProviderFactory.withTestConnection(base, config -> probed.set(true));

        assertSame("create() must delegate to base", sentinel, wrapped.create(Settings.EMPTY));
        assertSame(
            "createTrackingConsumedKeys() must delegate to base",
            sentinel,
            wrapped.createTrackingConsumedKeys(Settings.EMPTY, Map.of()).value()
        );
        wrapped.testConnection(Map.of());
        assertTrue("testConnection must call probe", probed.get());
    }
}
