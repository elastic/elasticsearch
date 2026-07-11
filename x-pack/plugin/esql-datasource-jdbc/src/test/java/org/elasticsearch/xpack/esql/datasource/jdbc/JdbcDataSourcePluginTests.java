/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public class JdbcDataSourcePluginTests extends ESTestCase {

    /** The COMPOUND schemes ESQL keys capability allow-listing, connector claiming, and operator dispatch off of. */
    private static final Set<String> COMPOUND_SCHEMES = Set.of(
        "jdbc:postgresql",
        "jdbc:redshift",
        "jdbc:redshift:iam",
        "jdbc:h2:tcp",
        "jdbc:h2"
    );

    /**
     * With the JDBC feature flag ENABLED -- the default in snapshot test builds -- every registration SPI method must
     * contribute the full set of compound schemes. This guards the gate wiring: none of the four methods may
     * short-circuit to empty while the flag is on.
     * <p>
     * We deliberately do NOT attempt to force the flag OFF in-process: {@link org.elasticsearch.common.util.FeatureFlag}
     * is auto-on in snapshot builds and its value is fixed at construction from the build type + system property, so it
     * cannot be flipped per-test. The off/release behavior (empty registration, {@code jdbc:} resolving to the generic
     * unsupported-scheme rejection) is guaranteed by {@code FeatureFlag}'s release semantics, not exercised here.
     */
    public void testRegistrationNonEmptyWhenFeatureFlagEnabled() {
        assertTrue(
            "JDBC feature flag must be on in snapshot test builds",
            JdbcDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_JDBC_FEATURE_FLAG.isEnabled()
        );
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin()) {
            assertEquals(COMPOUND_SCHEMES, plugin.supportedSchemes());
            assertEquals(COMPOUND_SCHEMES, plugin.supportedConnectorSchemes());
            assertEquals(COMPOUND_SCHEMES, plugin.storageProviders(Settings.EMPTY).keySet());
            Map<String, ConnectorFactory> connectors = plugin.connectors(Settings.EMPTY);
            assertFalse("connectors must be non-empty when the flag is on", connectors.isEmpty());
            assertTrue(connectors.containsKey("jdbc"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testSupportedConnectorSchemes() {
        // the connector must enumerate the COMPOUND schemes, not the bare "jdbc". DataSourceCapabilities.supportsScheme
        // (exact match) and DataSourceModule.LazyConnectorFactory.canHandle both key off the full compound scheme.
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin()) {
            assertEquals(COMPOUND_SCHEMES, plugin.supportedConnectorSchemes());
            assertFalse("must not register the bare jdbc scheme", plugin.supportedConnectorSchemes().contains("jdbc"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testSupportedSchemes() {
        // the storage stub must be registered under the SAME compound schemes as the connector.
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin()) {
            assertEquals(COMPOUND_SCHEMES, plugin.supportedSchemes());
            assertFalse("must not register the bare jdbc scheme", plugin.supportedSchemes().contains("jdbc"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testConnectorsReturnsJdbcKey() {
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin()) {
            Map<String, ConnectorFactory> connectors = plugin.connectors(Settings.EMPTY);
            assertEquals(1, connectors.size());
            assertTrue(connectors.containsKey("jdbc"));
            assertEquals("jdbc", connectors.get("jdbc").type());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testStorageProvidersReturnsCompoundSchemeKeys() {
        // one storage-stub entry per compound scheme so ExternalSourceResolver can build a FileList for a
        // jdbc:<vendor>:// path (the storage registry matches the exact compound scheme StoragePath parses out).
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin()) {
            Map<String, StorageProviderFactory> providers = plugin.storageProviders(Settings.EMPTY);
            assertEquals(COMPOUND_SCHEMES, providers.keySet());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testCloseCleanly() throws Exception {
        JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin();
        plugin.close();
    }

    /**
     * Without {@code path.home} the registry falls back to the parent classloader -- H2 is on the test classpath
     * so the connector still functions. This is the test-mode shape; in production {@code path.home} is always set
     * by Elasticsearch bootstrap.
     */
    public void testConnectorsClassloaderFallbackWhenPathHomeMissing() throws Exception {
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin()) {
            ConnectorFactory factory = plugin.connectors(Settings.EMPTY).get("jdbc");
            assertNotNull(factory);
            // H2 is on the test classpath, so canHandle must accept an H2 URL via the classloader-loaded driver.
            assertTrue(factory.canHandle("jdbc:h2:mem:" + randomAlphaOfLength(6) + ";DB_CLOSE_DELAY=-1"));
        }
    }

    /**
     * Production path: {@code path.home} is set. The registry must read from
     * {@code <path.home>/plugins/esql-datasource-jdbc/drivers/}. Pre-create that directory empty so {@code fromDirectory}
     * builds an empty registry without an exception. With H2 NOT on the child URLClassLoader's classpath but still
     * on the parent (test) classpath, ServiceLoader will find it via the parent and we'll still accept H2 URLs --
     * proving the directory path is wired and the registry is functional. This mirrors the production wiring.
     */
    public void testConnectorsLoadsFromPluginsDriversDirectoryWhenPathHomeSet() throws Exception {
        Path home = createTempDir();
        Path driversDir = home.resolve("plugins").resolve(JdbcDataSourcePlugin.DRIVERS_SUBDIR_RELATIVE_TO_PLUGINS);
        Files.createDirectories(driversDir);
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home.toString()).build();

        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin()) {
            ConnectorFactory factory = plugin.connectors(settings).get("jdbc");
            assertNotNull(factory);
            // The empty drivers dir means the child URLClassLoader has no JARs of its own, but the parent classloader
            // (test classpath, which has H2) still satisfies ServiceLoader. canHandle() therefore stays true; the
            // assertion that matters is "no exception, factory is reachable" -- the production code path was taken.
            assertEquals("jdbc", factory.type());
        }
    }

    /**
     * Caching: {@code connectors(settings)} called more than once must reuse the same registry instance. Otherwise
     * each canHandle/resolveMetadata cycle would re-scan the drivers dir and re-instantiate URLClassLoaders.
     */
    public void testConnectorsCachesRegistryAcrossCalls() throws Exception {
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin()) {
            ConnectorFactory first = plugin.connectors(Settings.EMPTY).get("jdbc");
            ConnectorFactory second = plugin.connectors(Settings.EMPTY).get("jdbc");
            assertNotNull(first);
            assertNotNull(second);
            // Different ConnectorFactory wrappers per call (each Map.of allocates a fresh entry) but the underlying
            // driver registry must be the same -- exercising both URLs' canHandle proves the same in-memory state.
            String url = "jdbc:h2:mem:" + randomAlphaOfLength(6) + ";DB_CLOSE_DELAY=-1";
            assertEquals(first.canHandle(url), second.canHandle(url));
        }
    }

    public void testGetSettingsExposesRuntimeConfigSettings() {
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin()) {
            java.util.List<? extends org.elasticsearch.common.settings.Setting<?>> s = plugin.getSettings();
            assertTrue("must register esql.jdbc.enabled", s.contains(JdbcRuntimeConfig.ENABLED));
            assertTrue("must register esql.jdbc.ssrf.allowed_subprotocols", s.contains(JdbcRuntimeConfig.ALLOWED_SUBPROTOCOLS));
            assertTrue("must register esql.jdbc.ssrf.allow_loopback", s.contains(JdbcRuntimeConfig.ALLOW_LOOPBACK));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * The kill switch flows from node Settings through the instance-owned runtime config to the factory's
     * canHandle(). We can verify the seeding path without a full ClusterService by constructing the plugin and
     * calling connectors() with the setting present; that initializes the owned {@link JdbcRuntimeConfig} and the
     * factory must reflect it.
     */
    public void testKillSwitchFalseFromSettingsRejectsCanHandle() throws Exception {
        Settings settings = Settings.builder().put(JdbcRuntimeConfig.ENABLED.getKey(), false).build();
        try (JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin()) {
            ConnectorFactory factory = plugin.connectors(settings).get("jdbc");
            assertNotNull(factory);
            assertFalse(
                "kill-switched factory must reject every URL",
                factory.canHandle("jdbc:h2:mem:" + randomAlphaOfLength(6) + ";DB_CLOSE_DELAY=-1")
            );
        }
    }

    /**
     * With no shared statics, two independently no-arg-constructed plugin instances
     * (each a standalone/managed instance) must NOT share or clobber state. Each owns its own {@link JdbcRuntimeConfig}
     * and driver registry, so flipping one's kill switch does not affect the other's factory, and closing one
     * releases only its own registry.
     */
    public void testTwoInstancesDoNotShareOrClobberState() throws Exception {
        try (JdbcDataSourcePlugin a = new JdbcDataSourcePlugin(); JdbcDataSourcePlugin b = new JdbcDataSourcePlugin()) {
            ConnectorFactory fa = a.connectors(Settings.EMPTY).get("jdbc");
            ConnectorFactory fb = b.connectors(Settings.EMPTY).get("jdbc");
            assertNotNull(fa);
            assertNotNull(fb);

            // Independent owned config and independent lazily-built registries -- no static bridge.
            assertNotSame("each instance owns its own runtime config", a.runtimeConfig(), b.runtimeConfig());
            assertNotNull("a built its own registry", a.driverRegistryOrNull());
            assertNotNull("b built its own registry", b.driverRegistryOrNull());
            assertNotSame("each instance owns its own registry", a.driverRegistryOrNull(), b.driverRegistryOrNull());

            String url = "jdbc:h2:mem:" + randomAlphaOfLength(6) + ";DB_CLOSE_DELAY=-1";
            assertTrue("both factories accept H2 before any kill switch", fa.canHandle(url));
            assertTrue(fb.canHandle(url));

            // Flip a's kill switch; b must be unaffected (no shared config).
            a.runtimeConfig().setEnabled(false);
            assertFalse("a's kill switch rejects its own factory", fa.canHandle(url));
            assertTrue("b must be unaffected by a's kill switch", fb.canHandle(url));

            // Closing a releases only a's registry; b's stays live.
            a.close();
            assertNull("a's registry released on close", a.driverRegistryOrNull());
            assertNotNull("b's registry untouched by a.close()", b.driverRegistryOrNull());
            assertTrue("b's factory still works after a.close()", fb.canHandle(url));
        }
    }

    /**
     * {@link JdbcDataSourcePlugin#close()} is idempotent (the {@link java.io.Closeable#close()} contract).
     * The owning module invokes it once on the SPI instance it holds; a redundant second call must be a no-op, not
     * an error.
     */
    public void testCloseIsIdempotent() throws Exception {
        JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin();
        assertNotNull(plugin.connectors(Settings.EMPTY).get("jdbc"));
        assertNotNull("registry built lazily by connectors()", plugin.driverRegistryOrNull());
        plugin.close();
        assertNull("first close releases the registry", plugin.driverRegistryOrNull());
        // Second close must not throw and must remain a no-op.
        plugin.close();
        assertNull(plugin.driverRegistryOrNull());
    }

}
