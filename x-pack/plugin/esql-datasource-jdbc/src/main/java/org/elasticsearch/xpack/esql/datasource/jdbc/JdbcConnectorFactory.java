/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * {@link ConnectorFactory} for {@code jdbc:*} URLs.
 * <p>
 * The dialect is resolved per URL through a {@link DialectRegistry} ({@code jdbc:postgresql://} → the
 * {@link PostgresDialect}, everything else → {@link GenericDialect}), so type mapping and per-connection setup match
 * the target vendor. The factory is gated on {@link JdbcDriverRegistry#canConnect(String)} so requests for URLs no
 * registered driver can handle are rejected at planning time, before any I/O.
 * <p>
 * <b>Config contract</b>: per-query options arrive in the {@code WITH (...)} map. The factory recognizes
 * {@code table}, {@code schema}, {@code catalog} (identifiers) and {@code user}, {@code password} (credentials --
 * decrypted by {@code DataSourceCredentials} before reaching {@link #open}). {@link ConfigKeyValidator} rejects
 * anything else, so a misspelled key does not silently disappear.
 * <p>
 * <b>matchPriority</b>: a fixed {@code 10} for any URL that starts with {@code jdbc:}. A single {@code jdbc:} bucket
 * is enough because no other connector claims this prefix; the dialect is resolved from the precise vendor scheme
 * (e.g. {@code jdbc:postgresql://}) downstream.
 */
public final class JdbcConnectorFactory implements ConnectorFactory, java.io.Closeable {

    /**
     * The compound JDBC schemes this connector accepts. ESQL keys capability allow-listing
     * ({@link org.elasticsearch.xpack.esql.datasources.DataSourceCapabilities#supportsScheme}), lazy-connector
     * claiming ({@code DataSourceModule.LazyConnectorFactory.canHandle}), and operator dispatch
     * ({@code OperatorFactoryRegistry} by {@code metadata.sourceType()}) all off the <em>full compound</em> scheme
     * of the URL, so this is the single source of truth for both {@code JdbcDataSourcePlugin.supportedConnectorSchemes}
     * / {@code supportedSchemes} and the {@code sourceType} {@link #resolveMetadata} stamps onto its metadata.
     * <p>
     * Ordered most-specific-first so {@link #resolveScheme} picks {@code jdbc:redshift:iam} over {@code jdbc:redshift}
     * for an IAM URL and {@code jdbc:h2:tcp} over {@code jdbc:h2} for a TCP-server H2 URL. Postgres-compatible
     * stores ride the {@code jdbc:postgresql} pgwire path (no dedicated scheme).
     * <p>
     * <b>Why both {@code jdbc:h2:tcp} and {@code jdbc:h2}.</b> ESQL's external-source resolver parses every dataset
     * resource through {@code StoragePath.of}, which requires a {@code ://} authority separator, and
     * {@code DataSourceModule.LazyConnectorFactory} claims a URL by the substring <em>before</em> {@code ://}. So the
     * only H2 form that can flow through the resolver end-to-end is the TCP-server URL {@code jdbc:h2:tcp://host:port/…}
     * — whose compound scheme is {@code jdbc:h2:tcp}. The opaque {@code jdbc:h2:mem:…} / {@code jdbc:h2:file:…} forms
     * have no {@code ://} and can never reach a connector via the resolver; {@code jdbc:h2} is retained for those forms
     * so the connector's own {@link #canHandle}/{@link #resolveMetadata}/{@link #resolveScheme} (exercised directly by
     * unit tests) still classify an in-process {@code jdbc:h2:mem:} URL. {@code jdbc:h2:tcp} is listed first so a
     * {@code jdbc:h2:tcp://} URL resolves to that scheme (matching what the resolver's {@code extractScheme} yields)
     * rather than the broader {@code jdbc:h2} prefix.
     */
    static final List<String> SUPPORTED_SCHEMES = List.of(
        "jdbc:redshift:iam",
        "jdbc:redshift",
        "jdbc:postgresql",
        "jdbc:h2:tcp",
        "jdbc:h2"
    );

    static final String CONFIG_TABLE = "table";
    static final String CONFIG_SCHEMA = "schema";
    static final String CONFIG_CATALOG = "catalog";
    static final String CONFIG_USER = "user";
    static final String CONFIG_PASSWORD = "password";
    static final String CONFIG_CONNECTION_PROPERTIES = JdbcConnectionProperties.CONFIG_KEY;
    /**
     * Typed AWS-credential config keys for Redshift IAM explicit-credentials mode. These are
     * SECRETS: they arrive as {@link SecureString}, ride their own typed channel exactly like {@code user}/
     * {@code password} (never the non-secret {@code connection_properties} map), and are forwarded to the driver as
     * the {@code AccessKeyID}/{@code SecretAccessKey}/{@code SessionToken} properties (see {@link #AWS_DRIVER_KEYS}).
     * When absent the connector forms the {@code iam://} URL with only the non-secret IAM params and the driver falls
     * back to the ambient AWS credential chain (env / instance-profile / web-identity) -- the "ambient-chain mode".
     */
    static final String CONFIG_ACCESS_KEY_ID = "access_key_id";
    static final String CONFIG_SECRET_ACCESS_KEY = "secret_access_key";
    static final String CONFIG_SESSION_TOKEN = "session_token";
    static final String RESOLVED_JDBC_URL = "jdbc_url";

    /** Driver {@link Properties} names for the typed AWS credentials, aligned index-for-index with nothing but used for cleanup. */
    private static final String DRIVER_ACCESS_KEY_ID = "AccessKeyID";
    private static final String DRIVER_SECRET_ACCESS_KEY = "SecretAccessKey";
    private static final String DRIVER_SESSION_TOKEN = "SessionToken";

    /**
     * Every secret driver-property name we may write into a connection {@link Properties} object, so the metadata
     * connect can strip them all in a finally block after the borrow (defense-in-depth: keeps a stray
     * {@code toString()} elsewhere from leaking them, exactly as we already do for {@code user}/{@code password}).
     */
    private static final String[] AWS_DRIVER_KEYS = { DRIVER_ACCESS_KEY_ID, DRIVER_SECRET_ACCESS_KEY, DRIVER_SESSION_TOKEN };

    private static final Set<String> CLAIMED_KEYS = Set.of(
        CONFIG_TABLE,
        CONFIG_SCHEMA,
        CONFIG_CATALOG,
        CONFIG_USER,
        CONFIG_PASSWORD,
        CONFIG_CONNECTION_PROPERTIES,
        CONFIG_ACCESS_KEY_ID,
        CONFIG_SECRET_ACCESS_KEY,
        CONFIG_SESSION_TOKEN
    );

    private static final Logger logger = LogManager.getLogger(JdbcConnectorFactory.class);

    private final JdbcDriverRegistry driverRegistry;
    private final DialectRegistry dialectRegistry;
    private final FilterPushdownSupport pushdownSupport;
    private final java.util.function.Supplier<SsrfGuard> ssrfGuardSupplier;
    private final java.util.function.BooleanSupplier enabledSupplier;
    /**
     * Read of the {@code esql.jdbc.pushdown.enabled} kill switch. Consulted on every {@link #filterPushdownSupport()}
     * call so turning WHERE pushdown off returns {@code null} support -> the optimizer leaves filters in the engine,
     * without disabling the connector. The setting is node-scoped and seeded once at node start (there is no
     * dynamic-settings delivery hook on the SPI), so a flip requires a node restart; the per-call read simply keeps
     * this factory decoupled from the config's storage. Defaults to always-on in the test-convenience constructors.
     */
    private final java.util.function.BooleanSupplier pushdownEnabledSupplier;
    private final JdbcHikariPool hikariPool;
    /**
     * Supplies the current credential epoch from the plugin's instance-owned {@link JdbcRuntimeConfig}. The
     * connector reads it before building credential {@link Properties} and again on an {@code AUTH_FAILED} to detect
     * a reload that happened mid-query. Defaults to a constant {@code 0} in the test-convenience constructors (no
     * reload plumbing there).
     */
    private final java.util.function.LongSupplier credentialEpochSupplier;
    /**
     * {@code true} when this factory created its own pool (test-convenience constructors) and is therefore
     * responsible for closing it via {@link #close()}. {@code false} for the production constructor, where the pool
     * is owned by {@link JdbcDataSourcePlugin} and closed through its lifecycle hook.
     */
    private final boolean ownsPool;

    public JdbcConnectorFactory(JdbcDriverRegistry driverRegistry) {
        this(driverRegistry, DialectRegistry.defaultRegistry(), SsrfGuard::defaultGuard, () -> true);
    }

    JdbcConnectorFactory(JdbcDriverRegistry driverRegistry, DialectRegistry dialectRegistry) {
        this(driverRegistry, dialectRegistry, SsrfGuard::defaultGuard, () -> true);
    }

    JdbcConnectorFactory(
        JdbcDriverRegistry driverRegistry,
        DialectRegistry dialectRegistry,
        java.util.function.Supplier<SsrfGuard> ssrfGuardSupplier,
        java.util.function.BooleanSupplier enabledSupplier
    ) {
        // Convenience overload for unit tests: a self-owned pool over the given registry with default pool settings.
        // Pushdown defaults to always-on; the overload below lets a kill-switch test flip it. The pool starts no
        // threads until a connection is borrowed; a test that exercises execute() must close this factory (Closeable)
        // to tear its pool down. Production wiring supplies the plugin-owned pool via the production constructor so
        // pool lifecycle (close) is managed by JdbcDataSourcePlugin instead.
        this(driverRegistry, dialectRegistry, ssrfGuardSupplier, enabledSupplier, () -> true);
    }

    JdbcConnectorFactory(
        JdbcDriverRegistry driverRegistry,
        DialectRegistry dialectRegistry,
        java.util.function.Supplier<SsrfGuard> ssrfGuardSupplier,
        java.util.function.BooleanSupplier enabledSupplier,
        java.util.function.BooleanSupplier pushdownEnabledSupplier
    ) {
        // Test-convenience overload that also takes the pushdown kill-switch supplier, so a unit test can assert
        // filterPushdownSupport() returns null when pushdown is disabled. Self-owned pool, as above.
        this(
            driverRegistry,
            dialectRegistry,
            ssrfGuardSupplier,
            enabledSupplier,
            pushdownEnabledSupplier,
            new JdbcHikariPool(driverRegistry, new JdbcRuntimeConfig()),
            () -> 0L,
            true
        );
    }

    public JdbcConnectorFactory(
        JdbcDriverRegistry driverRegistry,
        DialectRegistry dialectRegistry,
        java.util.function.Supplier<SsrfGuard> ssrfGuardSupplier,
        java.util.function.BooleanSupplier enabledSupplier,
        java.util.function.BooleanSupplier pushdownEnabledSupplier,
        JdbcHikariPool hikariPool,
        java.util.function.LongSupplier credentialEpochSupplier
    ) {
        this(
            driverRegistry,
            dialectRegistry,
            ssrfGuardSupplier,
            enabledSupplier,
            pushdownEnabledSupplier,
            hikariPool,
            credentialEpochSupplier,
            false
        );
    }

    private JdbcConnectorFactory(
        JdbcDriverRegistry driverRegistry,
        DialectRegistry dialectRegistry,
        java.util.function.Supplier<SsrfGuard> ssrfGuardSupplier,
        java.util.function.BooleanSupplier enabledSupplier,
        java.util.function.BooleanSupplier pushdownEnabledSupplier,
        JdbcHikariPool hikariPool,
        java.util.function.LongSupplier credentialEpochSupplier,
        boolean ownsPool
    ) {
        if (driverRegistry == null) {
            throw new IllegalArgumentException("driverRegistry must not be null");
        }
        if (dialectRegistry == null) {
            throw new IllegalArgumentException("dialectRegistry must not be null");
        }
        if (ssrfGuardSupplier == null) {
            throw new IllegalArgumentException("ssrfGuardSupplier must not be null");
        }
        if (enabledSupplier == null) {
            throw new IllegalArgumentException("enabledSupplier must not be null");
        }
        if (pushdownEnabledSupplier == null) {
            throw new IllegalArgumentException("pushdownEnabledSupplier must not be null");
        }
        if (hikariPool == null) {
            throw new IllegalArgumentException("hikariPool must not be null");
        }
        if (credentialEpochSupplier == null) {
            throw new IllegalArgumentException("credentialEpochSupplier must not be null");
        }
        this.driverRegistry = driverRegistry;
        this.dialectRegistry = dialectRegistry;
        this.ssrfGuardSupplier = ssrfGuardSupplier;
        this.enabledSupplier = enabledSupplier;
        this.pushdownEnabledSupplier = pushdownEnabledSupplier;
        this.hikariPool = hikariPool;
        this.credentialEpochSupplier = credentialEpochSupplier;
        this.ownsPool = ownsPool;
        // Pushdown support is stateless and dialect-independent for our vendors: filter rendering and identifier
        // quoting are identical across GenericDialect and its subclasses (PostgresDialect inherits both). It also has
        // no URL to resolve a per-vendor dialect against, so build it once from the generic dialect and reuse it,
        // avoiding an allocation on every EsRelation rewrite.
        this.pushdownSupport = new JdbcFilterPushdownSupport(GenericDialect.INSTANCE);
    }

    @Override
    public String type() {
        return "jdbc";
    }

    /**
     * Closes the connection pool <em>only</em> when this factory created it (the test-convenience constructors). In
     * production the pool is owned by {@link JdbcDataSourcePlugin} and released through its lifecycle hook, so this is
     * a no-op there. Provided so unit tests that exercise {@code execute()} can deterministically tear down the pool
     * (and its HikariCP housekeeping threads) rather than leaking them past the test.
     */
    @Override
    public void close() throws java.io.IOException {
        if (ownsPool) {
            hikariPool.close();
        }
    }

    @Override
    public boolean canHandle(String location) {
        if (location == null || location.startsWith("jdbc:") == false) {
            return false;
        }
        GuardDecision decision = evaluateGuard(location);
        if (decision.allowed() == false) {
            decision.log(logger);
            return false;
        }
        return driverRegistry.canConnect(location);
    }

    /**
     * Re-evaluates the kill switch + SSRF guard at a post-resolution entry point and throws if rejected.
     * The resolver's lazy connector wrapper ({@code DataSourceModule.LazyConnectorFactory.canHandle}) returns
     * true on a scheme-prefix match alone, never consulting this factory's {@link #canHandle}. The lazy wrapper
     * then forwards {@link #resolveMetadata} and {@link #open} straight through to us, so we must re-apply the
     * guard here. The same recheck protects against a dynamic settings flip between planning ({@code resolveMetadata})
     * and execution ({@code open}).
     */
    private void assertAllowed(String location) {
        if (location == null || location.startsWith("jdbc:") == false) {
            throw new IllegalArgumentException("JDBC location must start with [jdbc:]");
        }
        GuardDecision decision = evaluateGuard(location);
        if (decision.allowed() == false) {
            decision.log(logger);
            throw new IllegalStateException("JDBC URL [" + JdbcUrlSanitizer.sanitize(location) + "] is not allowed: " + decision.reason());
        }
    }

    /**
     * Single source of truth for the kill switch + SSRF guard. Both the silent boolean-shaped
     * {@link #canHandle} and the throwing {@link #assertAllowed} hand decisions to {@link GuardDecision#log}
     * so log levels and wording stay consistent across the two entry points.
     */
    private GuardDecision evaluateGuard(String location) {
        if (enabledSupplier.getAsBoolean() == false) {
            return GuardDecision.killSwitchOff(location);
        }
        // SSRF guard runs BEFORE the registry check so a hostile URL never reaches Driver.acceptsURL() (some
        // drivers attempt name resolution there). The guard is fetched through a supplier so a dynamic update
        // to esql.jdbc.ssrf.* settings takes effect immediately.
        SsrfGuard.Decision ssrf = ssrfGuardSupplier.get().evaluate(location);
        if (ssrf.allowed() == false) {
            return GuardDecision.ssrfDenied(location, ssrf.reason());
        }
        return GuardDecision.ALLOWED;
    }

    /**
     * Legacy scheme-priority hint. Not part of the current {@code ExternalSourceFactory} SPI (the lazy wrapper
     * governs claiming), so this is no longer an override; retained for direct unit coverage of the prefix contract.
     */
    public int matchPriority(String location) {
        return location != null && location.startsWith("jdbc:") ? 10 : 0;
    }

    @Override
    public void validateConfig(String location, Map<String, Object> config) {
        ConfigKeyValidator.check(config, List.of(CLAIMED_KEYS));
        // Fail fast at validation time on a bad connection_properties key (blocked/secret/not-allowlisted) rather than
        // deferring to the first connect in resolveMetadata. parse() enforces the allowlist; the result is discarded.
        JdbcConnectionProperties.parse(stringConfig(config, CONFIG_CONNECTION_PROPERTIES));
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
        if (location == null || location.isEmpty()) {
            throw new IllegalArgumentException("JDBC location must not be null or empty");
        }
        // The resolver reached us through DataSourceModule.LazyConnectorFactory, whose canHandle() returns true
        // on a scheme-prefix match alone without consulting our canHandle(). Re-apply the kill switch + SSRF
        // guard here so a hostile URL cannot slip through to driverRegistry.connect().
        assertAllowed(location);
        String table = stringConfig(config, CONFIG_TABLE);
        if (table == null) {
            throw new IllegalArgumentException("JDBC source requires WITH (table=\"<name>\")");
        }
        String schema = stringConfig(config, CONFIG_SCHEMA);
        String catalog = stringConfig(config, CONFIG_CATALOG);
        // Resolve the vendor dialect from the URL so type mapping (e.g. Postgres NUMERIC scoping) matches the target.
        JdbcDialect dialect = dialectRegistry.resolve(location);

        // Allowlist-filter the optional connection_properties passthrough (non-secret tuning: sslmode, ApplicationName,
        // options=endpoint=..., timeouts). parse() rejects blocked footguns, credentials, and anything not on the
        // allowlist. Keep the raw string to forward through resolvedConfig so open() re-derives the identical map.
        String rawConnectionProperties = stringConfig(config, CONFIG_CONNECTION_PROPERTIES);
        Map<String, String> connectionProperties = JdbcConnectionProperties.parse(rawConnectionProperties);

        // Probe the schema synchronously. resolveMetadata runs at planning time on the coordinator -- the connection
        // must not leak past this method, hence try-with-resources around everything.
        Properties props = credentialProperties(config);
        // Apply the tuning props for the metadata connect too, so a store that NEEDS them to connect (e.g. Neon
        // sslmode/endpoint) can be probed. applyTo never overwrites user/password (typed credentials always win).
        JdbcConnectionProperties.applyTo(props, connectionProperties);
        try (Connection conn = driverRegistry.connect(location, props)) {
            dialect.configureConnection(conn);
            DatabaseMetaData md = conn.getMetaData();
            List<Attribute> attributes = resolveColumns(dialect, md, catalog, schema, table);
            if (attributes.isEmpty()) {
                throw new IllegalArgumentException(
                    "JDBC table ["
                        + (catalog == null ? "" : catalog + ".")
                        + (schema == null ? "" : schema + ".")
                        + table
                        + "] has no columns mappable to ESQL types"
                );
            }
            Map<String, Object> resolvedConfig = new LinkedHashMap<>();
            resolvedConfig.put(RESOLVED_JDBC_URL, location);
            resolvedConfig.put(CONFIG_TABLE, table);
            if (schema != null) {
                resolvedConfig.put(CONFIG_SCHEMA, schema);
            }
            if (catalog != null) {
                resolvedConfig.put(CONFIG_CATALOG, catalog);
            }
            // Pass credentials through unchanged: SecureString wrappers are decrypted by DataSourceCredentials in the
            // module's lazy wrapper before open() is called. We do NOT log them.
            forwardSecret(config, resolvedConfig, CONFIG_USER);
            forwardSecret(config, resolvedConfig, CONFIG_PASSWORD);
            // Typed AWS credentials (Redshift IAM explicit-creds mode) ride the same secret channel as user/password.
            forwardSecret(config, resolvedConfig, CONFIG_ACCESS_KEY_ID);
            forwardSecret(config, resolvedConfig, CONFIG_SECRET_ACCESS_KEY);
            forwardSecret(config, resolvedConfig, CONFIG_SESSION_TOKEN);
            // Forward the (non-secret) connection_properties string so open() -> JdbcConnector -> the pool see the
            // same tuning props. Scalar String, so it survives plan serialization like table/schema.
            if (rawConnectionProperties != null) {
                resolvedConfig.put(CONFIG_CONNECTION_PROPERTIES, rawConnectionProperties);
            }
            // sourceType MUST be the COMPOUND scheme of the location (e.g. "jdbc:postgresql"), NOT the bare
            // type() "jdbc". OperatorFactoryRegistry looks the connector up by metadata.sourceType() against the
            // sourceFactoryMap keyed on supportedConnectorSchemes() (the compound schemes). Using "jdbc" would make
            // that lookup miss at execution ("No operator factory for sourceType: jdbc").
            return new SimpleSourceMetadata(attributes, resolveScheme(location), location, null, null, null, resolvedConfig);
        } catch (SQLException e) {
            // See JdbcConnector.doExecute -- driver messages may carry credentials. Strip them. SQLState surfaces in
            // the wrapper for diagnostics; the original message lives only on the sanitized cause.
            String sqlState = e.getSQLState();
            String suffix = sqlState == null ? "" : " (sqlstate=" + sqlState + ")";
            throw new IllegalStateException(
                "failed to resolve JDBC metadata for [" + sanitizedUrl(location) + "]" + suffix,
                JdbcUrlSanitizer.sanitizeException(e)
            );
        } finally {
            // Drop the credential entries; the SecureString backing arrays are owned by the caller and were
            // not zeroed (clone()-based copy in asString takes care of the temporary char[] only).
            props.remove("user");
            props.remove("password");
            for (String awsKey : AWS_DRIVER_KEYS) {
                props.remove(awsKey);
            }
        }
    }

    @Override
    public Connector open(Map<String, Object> config) {
        String jdbcUrl = stringConfig(config, RESOLVED_JDBC_URL);
        if (jdbcUrl == null) {
            throw new IllegalArgumentException("JDBC connector configuration is missing [" + RESOLVED_JDBC_URL + "]");
        }
        // Re-apply the guard at execution time too. A kill switch flip (or SSRF allowlist tighten) between
        // resolveMetadata() and open() must be honored -- the resolved URL was captured at planning time and
        // is forwarded here verbatim, so without this re-check a query in flight would still complete after
        // an operator hit the kill switch.
        assertAllowed(jdbcUrl);
        SecureString user = secureStringConfig(config, CONFIG_USER);
        SecureString password = secureStringConfig(config, CONFIG_PASSWORD);
        // Typed AWS credentials for Redshift IAM explicit-creds mode. Null when unset (ambient-chain mode).
        SecureString accessKeyId = secureStringConfig(config, CONFIG_ACCESS_KEY_ID);
        SecureString secretAccessKey = secureStringConfig(config, CONFIG_SECRET_ACCESS_KEY);
        SecureString sessionToken = secureStringConfig(config, CONFIG_SESSION_TOKEN);
        // Re-derive the allowlisted tuning props from the forwarded scalar string (re-parsing re-enforces the
        // allowlist, so a config assembled outside resolveMetadata cannot smuggle a blocked key past open()).
        Map<String, String> connectionProperties = JdbcConnectionProperties.parse(stringConfig(config, CONFIG_CONNECTION_PROPERTIES));
        JdbcDialect dialect = dialectRegistry.resolve(jdbcUrl);
        return new JdbcConnector(
            hikariPool,
            dialect,
            jdbcUrl,
            user,
            password,
            accessKeyId,
            secretAccessKey,
            sessionToken,
            connectionProperties,
            credentialEpochSupplier
        );
    }

    @Override
    public FilterPushdownSupport filterPushdownSupport() {
        // Consulted by the optimizer's PushFiltersToSource (reached through DataSourceModule.LazyConnectorFactory,
        // which delegates this call, and ExternalOptimizerContext's sourceFactories map). Returning null makes the
        // optimizer leave every filter in the engine-side FilterExec, so the connector emits an unfiltered scan.
        // The esql.jdbc.pushdown.enabled kill switch turns WHERE pushdown off by returning null here, independent of
        // the whole-connector esql.jdbc.enabled switch. The supplier is read live per call, but the underlying
        // esql.jdbc.pushdown.enabled setting is node-scoped and seeded once at node start (no dynamic-settings
        // delivery hook on the DataSourcePlugin SPI), so flipping it requires a node restart (rolling restart).
        if (pushdownEnabledSupplier.getAsBoolean() == false) {
            return null;
        }
        return pushdownSupport;
    }

    /**
     * Resolves the COMPOUND JDBC scheme (one of {@link #SUPPORTED_SCHEMES}) for a location by longest-prefix match,
     * lower-cased. This is what {@link #resolveMetadata} stamps as {@code sourceType} so the operator lookup key
     * matches the {@code supportedConnectorSchemes()} registration keys. Unlike a bare {@code indexOf("://")} split,
     * this also classifies driver-only URLs with no authority component (e.g. {@code jdbc:h2:mem:db}).
     *
     * @throws IllegalArgumentException if the location is not one of the supported compound schemes
     */
    static String resolveScheme(String location) {
        if (location == null) {
            throw new IllegalArgumentException("JDBC location must not be null");
        }
        String lower = location.toLowerCase(Locale.ROOT);
        for (String scheme : SUPPORTED_SCHEMES) {
            if (lower.startsWith(scheme)) {
                return scheme;
            }
        }
        throw new IllegalArgumentException("unsupported JDBC scheme for location [" + JdbcUrlSanitizer.sanitize(location) + "]");
    }

    private List<Attribute> resolveColumns(JdbcDialect dialect, DatabaseMetaData md, String catalog, String schema, String table)
        throws SQLException {
        List<Attribute> attributes = new ArrayList<>();
        // DatabaseMetaData.getColumns treats schemaPattern, tableNamePattern, and columnNamePattern as JDBC LIKE
        // patterns where '%' matches any sequence and '_' matches any single character. A user-supplied
        // table='log_2024' or table='ev%' would silently merge columns from multiple tables into one ESQL
        // schema. Escape '%' and '_' using DatabaseMetaData.getSearchStringEscape() so the patterns match
        // identifiers exactly. The catalog argument is NOT a pattern per JDBC spec; pass through verbatim.
        String esc = md.getSearchStringEscape();
        String escapedSchema = escapePattern(schema, esc);
        String escapedTable = escapePattern(table, esc);
        try (ResultSet rs = md.getColumns(catalog, escapedSchema, escapedTable, "%")) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                int jdbcType = rs.getInt("DATA_TYPE");
                int size = rs.getInt("COLUMN_SIZE");
                int decimal = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    decimal = 0;
                }
                DataType esqlType = dialect.mapJdbcType(jdbcType, size, decimal);
                if (esqlType == null) {
                    logger.warn("skipping JDBC column [{}] with unsupported type code [{}]", columnName, jdbcType);
                    continue;
                }
                attributes.add(toAttribute(columnName, esqlType));
            }
        }
        return attributes;
    }

    private static Attribute toAttribute(String name, DataType dataType) {
        // The external-source resolver requires a data-source schema of ReferenceAttribute (a FieldAttribute is
        // rejected: "Data source schema must contain only ReferenceAttribute"), so mirror the Flight connector's
        // shape. JDBC column nullability is not tracked here; default to nullable (Nullability.TRUE) so planner rules
        // that special-case non-nullable columns (IS NULL / COALESCE rewriting) never drop legitimate null rows.
        return new ReferenceAttribute(Source.EMPTY, null, name, dataType, Nullability.TRUE, null, false);
    }

    private static String stringConfig(Map<String, Object> config, String key) {
        if (config == null) {
            return null;
        }
        Object value = config.get(key);
        if (value == null) {
            return null;
        }
        // For SecureString we must not allocate a permanent String -- but for non-credential keys (table, schema, ...)
        // the WITH map already gave us a String. SecureString here is unexpected and we won't read it.
        if (value instanceof SecureString) {
            throw new IllegalArgumentException("config key [" + key + "] must be a plain string");
        }
        return Objects.toString(value, null);
    }

    private static SecureString secureStringConfig(Map<String, Object> config, String key) {
        if (config == null) {
            return null;
        }
        Object value = config.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof SecureString secure) {
            return secure;
        }
        if (value instanceof CharSequence cs) {
            // Test paths sometimes pass a plain String. Wrap into a SecureString so the connector path is uniform.
            return new SecureString(cs.toString().toCharArray());
        }
        throw new IllegalArgumentException("config key [" + key + "] must be a SecureString or string");
    }

    private static void forwardSecret(Map<String, Object> source, Map<String, Object> sink, String key) {
        if (source == null) {
            return;
        }
        Object value = source.get(key);
        if (value != null) {
            sink.put(key, value);
        }
    }

    private static Properties credentialProperties(Map<String, Object> config) {
        Properties props = new Properties();
        if (config == null) {
            return props;
        }
        Object user = config.get(CONFIG_USER);
        Object password = config.get(CONFIG_PASSWORD);
        if (user != null) {
            props.setProperty("user", asString(user));
        }
        if (password != null) {
            props.setProperty("password", asString(password));
        }
        // Typed AWS credentials for Redshift IAM explicit-creds mode: forwarded to the driver under its documented
        // property names. Absent => ambient AWS credential chain (nothing set here). Never logged; stripped by the
        // caller's finally after the borrow.
        setAwsCredential(props, config, CONFIG_ACCESS_KEY_ID, DRIVER_ACCESS_KEY_ID);
        setAwsCredential(props, config, CONFIG_SECRET_ACCESS_KEY, DRIVER_SECRET_ACCESS_KEY);
        setAwsCredential(props, config, CONFIG_SESSION_TOKEN, DRIVER_SESSION_TOKEN);
        return props;
    }

    private static void setAwsCredential(Properties props, Map<String, Object> config, String configKey, String driverKey) {
        Object value = config.get(configKey);
        if (value != null) {
            props.setProperty(driverKey, asString(value));
        }
    }

    private static String asString(Object value) {
        if (value instanceof SecureString secure) {
            // SecureString.getChars() returns the live backing array; zeroing it would corrupt the original
            // SecureString (which is owned by the caller and reused across resolveMetadata + open). Use clone()
            // per the SecureString javadoc -- closing the clone zeros only the copy.
            try (SecureString copy = secure.clone()) {
                return new String(copy.getChars());
            }
        }
        return value.toString();
    }

    /**
     * Escapes the JDBC LIKE-pattern metacharacters {@code %} and {@code _} in a metadata search argument so it
     * matches the identifier exactly. {@code escape} is the driver-reported escape sequence from
     * {@link DatabaseMetaData#getSearchStringEscape()}.
     */
    static String escapePattern(String identifier, String escape) {
        if (identifier == null || identifier.isEmpty() || escape == null || escape.isEmpty()) {
            return identifier;
        }
        StringBuilder sb = new StringBuilder(identifier.length() + 4);
        for (int i = 0; i < identifier.length(); i++) {
            char c = identifier.charAt(i);
            if (c == '%' || c == '_' || c == escape.charAt(0)) {
                sb.append(escape);
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private static String sanitizedUrl(String url) {
        return url == null ? "" : JdbcUrlSanitizer.sanitize(url);
    }

    // Visible for testing -- assertion that we did not silently reorder claimed keys.
    static Map<String, Object> resolvedConfigFor(String url, String table, String schema, String catalog) {
        Map<String, Object> m = new HashMap<>();
        m.put(RESOLVED_JDBC_URL, url);
        m.put(CONFIG_TABLE, table);
        if (schema != null) {
            m.put(CONFIG_SCHEMA, schema);
        }
        if (catalog != null) {
            m.put(CONFIG_CATALOG, catalog);
        }
        return m;
    }

    /**
     * Outcome of {@link #evaluateGuard(String)}. {@link #log(Logger)} centralizes severity + wording so the
     * silent {@link #canHandle} path and the throwing {@link #assertAllowed} path agree on what an operator
     * sees in the log. Kill switch hits log at DEBUG (high-volume, expected during planned outages); SSRF
     * denials log at WARN (low-volume, audit-worthy).
     */
    private record GuardDecision(boolean allowed, String reason, String sanitizedLocation, boolean ssrf) {
        static final GuardDecision ALLOWED = new GuardDecision(true, null, null, false);

        static GuardDecision killSwitchOff(String location) {
            return new GuardDecision(false, "esql.jdbc.enabled is false", JdbcUrlSanitizer.sanitize(location), false);
        }

        static GuardDecision ssrfDenied(String location, String reason) {
            return new GuardDecision(false, reason, JdbcUrlSanitizer.sanitize(location), true);
        }

        void log(Logger logger) {
            if (allowed) {
                return;
            }
            if (ssrf) {
                logger.warn("rejecting JDBC URL [{}] -- {}", sanitizedLocation, reason);
            } else {
                logger.debug("rejecting [{}] -- {}", sanitizedLocation, reason);
            }
        }
    }
}
