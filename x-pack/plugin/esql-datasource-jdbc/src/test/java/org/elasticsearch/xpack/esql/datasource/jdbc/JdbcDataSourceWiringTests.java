/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

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
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Fail-fast wiring assertions for the two integration invariants, exercised as unit tests so a scheme/sourceType/cache mismatch
 * surfaces here rather than only when an integration test stands up a real cluster.
 * <ul>
 *   <li><b>Compound scheme:</b> {@link DataSourceCapabilities#supportsScheme} (exact match),
 *       {@code DataSourceModule.LazyConnectorFactory.canHandle}, and operator dispatch by {@code metadata.sourceType()}
 *       all key off the full compound scheme (e.g. {@code jdbc:postgresql}), so the plugin must enumerate compound
 *       schemes and {@code resolveMetadata} must stamp the compound scheme as {@code sourceType}.</li>
 *   <li><b>Cache bypass:</b> {@link JdbcStorageProvider#supportsStableMetadata()} must be {@code false} so the
 *       resolver treats JDBC sources as non-cacheable.</li>
 * </ul>
 */
public class JdbcDataSourceWiringTests extends ESTestCase {

    private static final EncryptionService ENCRYPTION_SERVICE = mock(EncryptionService.class);

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }

    /** the aggregated capability allow-list (built exactly as the node builds it) must contain every compound scheme. */
    public void testCapabilitiesAllowlistUsesCompoundSchemes() throws Exception {
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin()) {
            DataSourceCapabilities capabilities = DataSourceCapabilities.build(List.of(plugin));
            assertTrue(capabilities.supportsScheme("jdbc:postgresql"));
            assertTrue(capabilities.supportsScheme("jdbc:redshift"));
            assertTrue(capabilities.supportsScheme("jdbc:redshift:iam"));
            assertTrue(capabilities.supportsScheme("jdbc:h2"));
            // The bare "jdbc" scheme must NOT be in the allow-list; StoragePath parses the compound scheme from a URL
            // and DataSourceCapabilities.supportsScheme is an exact match, so a bare "jdbc" entry would never match.
            assertFalse(capabilities.supportsScheme("jdbc"));
        }
    }

    /** through the module's lazy connector wrapper, a {@code jdbc:postgresql://} URL must be claimed. */
    public void testLazyConnectorClaimsPostgresUrl() throws Exception {
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin(); DataSourceModule module = newModule(plugin)) {
            ExternalSourceFactory factory = module.sourceFactories().get("jdbc:postgresql");
            assertNotNull("a lazy connector must be registered under the compound scheme jdbc:postgresql", factory);
            assertTrue(
                "the lazy connector must claim a jdbc:postgresql:// URL",
                factory.canHandle("jdbc:postgresql://db.example.com:5432/mydb")
            );
        }
    }

    /**
     * the {@code sourceType} that {@code resolveMetadata} stamps onto its metadata must be a key in
     * {@code sourceFactories()} -- this is the exact lookup {@code OperatorFactoryRegistry} performs at execution
     * time. A bare-"jdbc" sourceType would miss the map (keyed on the compound schemes) and fail the query.
     */
    public void testSourceTypeResolvesBackToTheConnector() throws Exception {
        String jdbcUrl = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        try (Connection conn = DriverManager.getConnection(jdbcUrl); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE EMPLOYEES (ID INTEGER, NAME VARCHAR(100))");
        }
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin(); DataSourceModule module = newModule(plugin)) {
            ExternalSourceFactory byScheme = module.sourceFactories().get("jdbc:h2");
            assertNotNull("a lazy connector must be registered under the compound scheme jdbc:h2", byScheme);

            SourceMetadata metadata = byScheme.resolveMetadata(jdbcUrl, Map.of(JdbcConnectorFactory.CONFIG_TABLE, "EMPLOYEES"));
            assertEquals("jdbc:h2", metadata.sourceType());
            assertSame(
                "sourceFactories().get(metadata.sourceType()) must resolve back to the same connector",
                byScheme,
                module.sourceFactories().get(metadata.sourceType())
            );
        }
    }

    /** the storage stub must declare unstable metadata so the resolver bypasses the schema cache. */
    public void testStorageProviderBypassesSchemaCache() {
        JdbcStorageProvider provider = new JdbcStorageProvider();
        assertFalse("JDBC sources have no stable metadata token; the schema cache must be bypassed", provider.supportsStableMetadata());
        // Sanity-check the storage stub is registered under the compound schemes too.
        assertTrue(provider.supportedSchemes().contains("jdbc:postgresql"));
        assertTrue(provider.supportedSchemes().contains("jdbc:h2"));
    }

    /** sanity: StoragePath parses the compound scheme (not the bare "jdbc") out of a {@code jdbc:postgresql://} URL. */
    public void testStoragePathParsesCompoundScheme() {
        StoragePath path = StoragePath.of("jdbc:postgresql://db.example.com:5432/mydb");
        assertEquals("jdbc:postgresql", path.scheme());
        assertFalse("a clean jdbc URL must not be classified as a glob pattern", path.isPattern());
    }

    private DataSourceModule newModule(DataSourcePlugin plugin) {
        List<DataSourcePlugin> plugins = List.of(plugin);
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
}
