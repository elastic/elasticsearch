/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Registers the JDBC connector ({@code jdbc:*} URLs) for ESQL external data sources.
 * <p>
 * The vendor dialect is resolved per URL through {@link DialectRegistry#defaultRegistry()} ({@code jdbc:postgresql://}
 * → {@link PostgresDialect}, everything else → {@link GenericDialect}). JDBC drivers are resolved through
 * {@link JdbcDriverRegistry} and called via {@link java.sql.Driver#connect} directly -- {@link java.sql.DriverManager}
 * is bypassed so the plugin's child {@link ClassLoader} fully controls driver classes.
 * <p>
 * <b>Driver loading lifecycle.</b> The user-supplied {@code $ES_HOME/plugins/esql-datasource-jdbc/drivers/} directory
 * is scanned lazily on the first {@code connectors(Settings)} call: that is the earliest hook on the
 * {@link DataSourcePlugin} SPI that exposes node-level {@link Settings} (and therefore {@link Environment#PATH_HOME_SETTING}).
 * Construction-time scanning is not viable -- the plugin is instantiated by the SPI extension loader with a no-arg
 * constructor before {@code Plugin#createComponents} runs, so the install directory is unknown at that point.
 * <p>
 * If {@code path.home} is unset (typically only in unit tests that exercise this class without bootstrapping a node),
 * the registry falls back to the plugin's own {@link ClassLoader} so H2-on-the-test-classpath keeps working without
 * a fake plugin install layout.
 * <p>
 * <b>Registry + runtime-config ownership (no statics).</b> Elasticsearch instantiates this class more than once:
 * once as a managed {@link Plugin} (loaded by {@code PluginsService.loadPlugin}, which requires a <em>single</em>
 * public constructor, so this class exposes only the no-arg ctor), and separately as a {@link DataSourcePlugin} SPI
 * extension -- the SPI object is the one whose {@code connectors(Settings)} the {@code DataSourceModule} calls to
 * build the query-serving {@link JdbcConnectorFactory}, and whose {@link #close()} the module invokes on node
 * shutdown (the module closes every {@link Closeable} {@code DataSourcePlugin}). The two are distinct objects with
 * no link between them.
 * <p>
 * Each instance <em>owns its own</em> {@link JdbcRuntimeConfig} + driver registry (per-instance fields, no
 * class-level static). The runtime config (kill switch + SSRF allowlist + pool sizing) is seeded once from node
 * {@link Settings} on the first {@code connectors(Settings)} call; on current {@code main} the {@link DataSourcePlugin}
 * SPI exposes no per-plugin dynamic cluster-settings hook, so these values are construction-time (a dynamic reload
 * would need a new SPI seam and is deferred). The registry it builds lazily is released once in {@link #close()}
 * ({@link AtomicReference#getAndSet}-guarded, so double-close is a no-op). The managed {@link Plugin} instance
 * contributes only {@link #getSettings()} (the {@link Setting} declarations, read once at registration) and holds no
 * query-time state.
 * <p>
 * <b>Compound schemes.</b> ESQL keys scheme routing, capability allow-listing, and operator dispatch off the
 * <em>full</em> URL scheme, which for JDBC is compound (e.g. {@code jdbc:postgresql}). So this plugin enumerates the
 * compound schemes it accepts in {@link JdbcConnectorFactory#SUPPORTED_SCHEMES} and registers both its connector and
 * its storage stub under every one of them; the single {@link JdbcConnectorFactory} handles all subprotocols.
 * <p>
 * <b>Feature-flag gate.</b> All registration is gated behind {@link #ESQL_EXTERNAL_DATASOURCES_JDBC_FEATURE_FLAG}
 * (snapshot-on, release-off). When the gate is off the schemes are not registered, so a {@code jdbc:} query resolves
 * to the generic unsupported-scheme rejection.
 * <p>
 * <b>Entitlement note.</b> {@code FileAccessTree} implicitly grants every plugin {@code READ} on its own install
 * directory (see {@code componentPaths} in {@code FileAccessTree}). The drivers directory lives under that path,
 * so we do not declare a separate {@code files} entitlement for it -- adding one would be redundant and would
 * obscure the implicit grant that actually carries the read.
 */
public class JdbcDataSourcePlugin extends Plugin implements DataSourcePlugin, ReloadablePlugin, Closeable {

    static final String DRIVERS_SUBDIR_RELATIVE_TO_PLUGINS = "esql-datasource-jdbc/drivers";

    /**
     * Gates provisioning JDBC ({@code jdbc:*}) data sources/datasets. Snapshot-on, release-off; override in release
     * with {@code -Des.esql_external_datasources_jdbc_feature_flag_enabled=true}. When off, none of the registration
     * SPI methods contribute anything, so a {@code jdbc:} query resolves to the generic unsupported-scheme rejection.
     */
    public static final FeatureFlag ESQL_EXTERNAL_DATASOURCES_JDBC_FEATURE_FLAG = new FeatureFlag("esql_external_datasources_jdbc");

    private static final Logger logger = LogManager.getLogger(JdbcDataSourcePlugin.class);

    private static boolean jdbcEnabled() {
        return ESQL_EXTERNAL_DATASOURCES_JDBC_FEATURE_FLAG.isEnabled();
    }

    /**
     * Per-instance runtime config (kill switch + SSRF guard + pool sizing). Seeded once from node {@link Settings}
     * on the first {@link #connectors} call, on the same instance the factory reads. There is no dynamic
     * cluster-settings hook on the current {@link DataSourcePlugin} SPI, so these values are construction-time.
     */
    private final JdbcRuntimeConfig runtimeConfig = new JdbcRuntimeConfig();

    /** Lazily-built driver registry, owned by this instance and released in {@link #close()}. */
    private final AtomicReference<JdbcDriverRegistry> registry = new AtomicReference<>();

    /**
     * Lazily-built HikariCP connection pool, owned by this instance and released in {@link #close()}. An
     * instance field, never a static -- all connector state is per-instance and this must not reintroduce a static.
     * Closed <em>before</em> {@link #registry} because pooled connection teardown needs the registry's driver classes.
     */
    private final AtomicReference<JdbcHikariPool> hikariPool = new AtomicReference<>();

    /**
     * Once-only seed flag so a second {@code connectors(Settings)} call does not re-seed the runtime config. First
     * seed wins; there is no dynamic cluster-settings channel on the current SPI.
     */
    private final AtomicBoolean configInitialized = new AtomicBoolean(false);

    private final ClassLoader pluginClassLoader = JdbcDataSourcePlugin.class.getClassLoader();

    @Override
    public Set<String> supportedSchemes() {
        if (jdbcEnabled() == false) {
            return Set.of();
        }
        // the storage stub must be registered under the SAME compound schemes as the connector, so
        // ExternalSourceResolver can build a FileList for a jdbc:<vendor>:// path. Keying on the bare "jdbc"
        // would miss DataSourceCapabilities.supportsScheme / StorageProviderRegistry, which match the exact
        // (compound) scheme StoragePath.of(...) parses out of the URL.
        return Set.copyOf(JdbcConnectorFactory.SUPPORTED_SCHEMES);
    }

    @Override
    public Set<String> supportedConnectorSchemes() {
        if (jdbcEnabled() == false) {
            return Set.of();
        }
        // enumerate the compound schemes the connector accepts. DataSourceCapabilities.supportsScheme (exact
        // match) and DataSourceModule.LazyConnectorFactory.canHandle (declaredSchemes.contains(extractScheme))
        // both key off the full compound scheme, and OperatorFactoryRegistry dispatches by metadata.sourceType()
        // which JdbcConnectorFactory.resolveMetadata sets to the same compound scheme. NOT the bare "jdbc".
        return Set.copyOf(JdbcConnectorFactory.SUPPORTED_SCHEMES);
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        if (jdbcEnabled() == false) {
            return Map.of();
        }
        // JDBC sources are not byte-addressable, but the resolver needs a placeholder StorageProvider so it can
        // register a single-entry "file list" against the URL. See JdbcStorageProvider for the contract. One entry
        // per compound scheme so the storage registry resolves a jdbc:<vendor>:// path.
        StorageProviderFactory factory = StorageProviderFactory.noConfigKeys(JdbcStorageProvider::new);
        Map<String, StorageProviderFactory> providers = new LinkedHashMap<>();
        for (String scheme : JdbcConnectorFactory.SUPPORTED_SCHEMES) {
            providers.put(scheme, factory);
        }
        return Map.copyOf(providers);
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        if (jdbcEnabled() == false) {
            // Return BEFORE seeding runtimeConfig / building the registry, so a disabled connector allocates nothing.
            return Map.of();
        }
        // Seed the instance-owned runtime config so the SSRF guard / kill switch / pool sizing reflect node.yaml.
        // A once-only flag makes a second connectors() call a no-op. This is the sole seeding point on the current
        // SPI (no dynamic cluster-settings hook). connectors() returns exactly ONE ConnectorFactory: the module's
        // LazyConnectorFactory throws on more than one, and the single factory handles every jdbc: subprotocol.
        if (configInitialized.compareAndSet(false, true)) {
            runtimeConfig.initialize(settings);
        }
        JdbcDriverRegistry driverRegistry = driverRegistry(settings);
        return Map.of(
            "jdbc",
            new JdbcConnectorFactory(
                driverRegistry,
                DialectRegistry.defaultRegistry(),
                runtimeConfig::guard,
                runtimeConfig::enabled,
                runtimeConfig::pushdownEnabled,
                hikariPool(driverRegistry),
                runtimeConfig::credentialEpoch
            )
        );
    }

    /**
     * Lazily builds the instance-owned {@link JdbcHikariPool} over the given driver registry. The pool object itself
     * is cheap and starts no threads until the first per-endpoint {@code getConnection} creates a
     * {@link com.zaxxer.hikari.HikariDataSource}. On a lost CAS race the loser is discarded (it holds no resources yet).
     */
    private JdbcHikariPool hikariPool(JdbcDriverRegistry driverRegistry) {
        JdbcHikariPool current = hikariPool.get();
        if (current != null) {
            return current;
        }
        JdbcHikariPool built = new JdbcHikariPool(driverRegistry, runtimeConfig);
        if (hikariPool.compareAndSet(null, built)) {
            return built;
        }
        return hikariPool.get();
    }

    private JdbcDriverRegistry driverRegistry(Settings settings) {
        JdbcDriverRegistry current = registry.get();
        if (current != null) {
            return current;
        }
        // Build outside any lock; on a lost CAS race the loser closes its build and returns the winner. URLClassLoader
        // construction is not free but happens at most twice across the JVM in the rare race window.
        JdbcDriverRegistry built = buildRegistry(settings);
        if (registry.compareAndSet(null, built)) {
            return built;
        }
        IOUtils.closeWhileHandlingException(built);
        return registry.get();
    }

    @SuppressForbidden(
        reason = "Resolving path.home -> plugins/<plugin>/drivers requires PathUtils.get because the DataSourcePlugin "
            + "SPI surface is invoked before any Environment is available; the SPI signature is "
            + "connectors(Settings), not connectors(Environment)."
    )
    private JdbcDriverRegistry buildRegistry(Settings settings) {
        if (Environment.PATH_HOME_SETTING.exists(settings) == false) {
            // Tests path: H2 (or any other test driver) is on the plugin classpath. Nothing to load from disk.
            logger.debug("[path.home] is not set; loading JDBC drivers via parent classloader only (test mode)");
            return JdbcDriverRegistry.fromClassLoader(pluginClassLoader);
        }
        Path driversDir = PathUtils.get(Environment.PATH_HOME_SETTING.get(settings))
            .resolve("plugins")
            .resolve(DRIVERS_SUBDIR_RELATIVE_TO_PLUGINS);
        JdbcDriverRegistry fromDir;
        try {
            fromDir = JdbcDriverRegistry.fromDirectory(driversDir, pluginClassLoader);
        } catch (IOException e) {
            throw new IllegalStateException("failed to load JDBC drivers from [" + driversDir + "]", e);
        }
        if (fromDir.driverCount() > 0) {
            return fromDir;
        }
        // No drivers under the install directory: fall back to the plugin classloader. In production the plugin
        // bundles no JDBC driver, so this yields the same empty registry as before; in tests the driver (H2) is on
        // the plugin classloader's classpath and is discovered here. Mirrors the path.home-missing fallback above.
        IOUtils.closeWhileHandlingException(fromDir);
        logger.debug("no JDBC drivers under install directory [{}]; falling back to the plugin classloader", driversDir);
        return JdbcDriverRegistry.fromClassLoader(pluginClassLoader);
    }

    @Override
    public List<Setting<?>> getSettings() {
        // Owned settings: kill switch + SSRF allowlist + loopback toggle. Returned from the managed Plugin instance
        // (the SPI surface has no equivalent), which is enough because Elasticsearch reads getSettings() exactly
        // once at registration -- via the managed instance, not the SPI one.
        return JdbcRuntimeConfig.settings();
    }

    /**
     * {@link ReloadablePlugin} hook: bumps the credential epoch on this instance's owned
     * {@link JdbcRuntimeConfig} (never a static). This is the minimal, forward-looking seam for a
     * credential refresh: a connector reads the epoch before building its credential {@link java.util.Properties} and
     * can retry a single {@code AUTH_FAILED} once against a re-resolved credential generation.
     * <p>
     * <b>Scope on this branch.</b> The JDBC connector declares <em>no</em> node-keystore {@code SecureSetting}s;
     * credentials are per-query {@link org.elasticsearch.common.settings.SecureString}s decrypted from the data-source
     * definition, which are immutable for a query's lifetime and cannot be re-fetched fresher. So the production
     * credential source is not refreshable and {@code reload()} does not change any live credential today — it only
     * advances the epoch. The {@code POST _nodes/reload_secure_settings} API invokes this on the <em>managed</em>
     * {@link Plugin} instance, whereas queries are served by the distinct SPI-extension instance (the
     * two-instance model); wiring the epoch cross-instance is deferred until a real keystore-backed credential
     * source exists (doing it now would be dead code).
     */
    @Override
    public void reload(Settings settings) {
        long epoch = runtimeConfig.bumpCredentialEpoch();
        logger.info("reloaded JDBC datasource plugin; credential epoch is now [{}]", epoch);
    }

    /**
     * Releases this instance's driver registry. Invoked by {@code DataSourceModule} on the SPI instance it owns (the
     * lifecycle hook); the managed {@link Plugin} instance holds no registry of its own, so there is no second
     * release to reconcile. {@link AtomicReference#getAndSet} still makes a redundant call a no-op, so {@code close()}
     * is idempotent as the {@link Closeable#close()} contract requires.
     * <p>
     * <b>Quiescence assumption.</b> The owning module closes SPI plugins only after the framework-owned closeables,
     * and node shutdown quiesces the query path before components are closed, so no in-flight query is still holding
     * a JDBC connection from the pool/registry when they are released.
     * <p>
     * <b>Close ordering.</b> The HikariCP pool is closed <em>before</em> the driver registry:
     * {@link com.zaxxer.hikari.HikariDataSource#close()} returns/evicts physical connections whose
     * {@link java.sql.Connection#close()} needs the driver classes that live in the registry's child
     * {@link ClassLoader}. Closing the registry (and its classloader) first could break connection teardown. Both
     * releases are {@link AtomicReference#getAndSet}-guarded so a redundant {@code close()} is a no-op.
     */
    @Override
    public void close() throws IOException {
        JdbcHikariPool pool = hikariPool.getAndSet(null);
        if (pool != null) {
            pool.close();
        }
        JdbcDriverRegistry current = registry.getAndSet(null);
        if (current != null) {
            current.close();
        }
        // Reset the once-only seed flag so a subsequent connectors() call (in tests or a fresh node) re-seeds the
        // runtime config from the new Settings. In production this is a no-op (the JVM dies with the node).
        configInitialized.set(false);
    }

    /** Test-only accessor for the instance-owned runtime config, so tests can simulate a dynamic cluster-settings update. */
    JdbcRuntimeConfig runtimeConfig() {
        return runtimeConfig;
    }

    /** Test-only accessor for the lazily-built driver registry (may be {@code null} before the first connectors call). */
    JdbcDriverRegistry driverRegistryOrNull() {
        return registry.get();
    }

    /** Test-only accessor for the lazily-built HikariCP pool (may be {@code null} before the first connectors call). */
    JdbcHikariPool hikariPoolOrNull() {
        return hikariPool.get();
    }
}
