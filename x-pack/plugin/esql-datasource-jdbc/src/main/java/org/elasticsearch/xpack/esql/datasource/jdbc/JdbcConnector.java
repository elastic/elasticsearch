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
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Connector for a single JDBC URL. It is coordinator-only and single-split: each {@link #execute} call borrows a
 * {@link Connection} from the per-endpoint HikariCP pool ({@link JdbcHikariPool}) and hands it (along with
 * the {@link PreparedStatement} and {@link ResultSet}) to a {@link JdbcResultCursor} that owns the resource lifecycle
 * for the duration of the query -- closing the cursor returns the connection to the pool.
 * <p>
 * The connector itself is immutable -- pushdown configuration arrives via the {@link QueryRequest} (table / schema /
 * row limit / pushed filter), not via mutators on the connector.
 * <p>
 * <b>Credentials policy.</b> Pass credentials through the {@code user}/{@code password} config keys (which arrive as
 * {@link SecureString} and never reach a log appender). URL-embedded credentials of the shape
 * {@code jdbc:vendor://user:pass@host} or {@code ?password=...}/{@code ;password=...} are NOT recommended: while
 * {@link JdbcUrlSanitizer} redacts what we log ourselves, vendor JDBC drivers frequently echo the raw URL in their
 * own exception text and connection diagnostics. We sanitize those messages on best-effort grounds before they
 * reach this connector's wrapper exception, but a defense-in-depth posture is to keep credentials out of the URL
 * altogether.
 */
final class JdbcConnector implements Connector {

    private static final Logger logger = LogManager.getLogger(JdbcConnector.class);

    /**
     * URLs (sanitized) for which the unsupported-database-major-version WARN has already been logged, so the warning
     * fires at most once per endpoint rather than once per query. Keyed by sanitized URL (never the raw, possibly
     * credential-bearing URL). Static because a fresh {@link JdbcConnector} is built per query -- an
     * instance-scoped guard would warn on every query. Backed by a {@link BoundedWarnOnceMap} so a node that reaches
     * an unbounded number of distinct endpoints cannot grow this guard without limit: past the cap the eldest entry
     * is evicted (at worst re-emitting the one-time WARN for a long-idle endpoint).
     */
    static final Map<String, Boolean> VERSION_WARNED = BoundedWarnOnceMap.create();

    /**
     * URLs (sanitized) for which the product-name advisory WARN has already been logged, so the advisory fires at most
     * once per endpoint rather than once per query. Keyed by sanitized URL (never the raw, credential-bearing URL),
     * static, and backed by a {@link BoundedWarnOnceMap} with FIFO eviction past its cap -- exactly like
     * {@link #VERSION_WARNED}. Kept separate from {@code VERSION_WARNED} so the two independent once-per-URL warnings
     * never suppress each other.
     */
    static final Map<String, Boolean> PRODUCT_ADVISORY_WARNED = BoundedWarnOnceMap.create();

    /**
     * Canonical {@link DatabaseMetaData#getDatabaseProductName()} substrings (lowercase) → the store they indicate,
     * scanned in this fixed priority order (the tokens are mutually non-overlapping, so first-match is deterministic).
     * Each entry pairs the vendor token used to build the recommended {@code jdbc:<vendor>://} scheme with the dialect
     * {@link JdbcDialect#name() name} that store is <em>natively</em> served by, so a product that agrees with the
     * resolved dialect stays silent (PostgreSQL + {@code postgresql}, H2 + {@code generic}) while a meaningful mismatch
     * -- a pg-wire store such as Redshift reached through the {@code postgresql} dialect -- yields a
     * one-time advisory. This is intentionally NOT a dialect-selection mechanism (see {@link #adviseOnDatabaseProduct}
     * for why we advise rather than auto-switch); adding a store here is a one-line, additive edit.
     */
    private static final List<ProductProfile> PRODUCT_PROFILES = List.of(
        new ProductProfile("redshift", "redshift", "redshift"),
        new ProductProfile("postgresql", "postgresql", "postgresql"),
        new ProductProfile("h2", "h2", "generic")
    );

    /**
     * A recognized database product: the lowercase {@code getDatabaseProductName()} {@code substring} that identifies
     * it, the {@code vendorToken} used to suggest a {@code jdbc:<vendorToken>://} scheme, and the {@code dialectName}
     * of the dialect that natively serves it (used to decide whether the resolved dialect and the connected product
     * agree).
     */
    private record ProductProfile(String substring, String vendorToken, String dialectName) {}

    private final ConnectionSource connectionSource;
    private final JdbcDialect dialect;
    private final String jdbcUrl;
    private final CredentialSource credentials;
    /**
     * Allowlist-filtered, non-secret {@code connection_properties}: tuning props such as
     * {@code sslmode}/{@code ApplicationName}/{@code options=endpoint=...} that {@link #openConnection} layers into the
     * per-borrow {@link Properties} after the credentials. Never contains {@code user}/{@code password} (rejected at
     * {@link JdbcConnectionProperties#parse}); immutable + never overwrites the typed credentials.
     */
    private final Map<String, String> connectionProperties;
    private final LongSupplier credentialEpoch;

    /**
     * Production constructor: borrows physical connections from the per-endpoint {@link JdbcHikariPool} and carries
     * per-query {@link SecureString} credentials (which are immutable for the query's lifetime and therefore not
     * refreshable — see {@link PerQueryCredentials}). {@code credentialEpoch} exposes the current credential epoch
     * from the plugin's instance-owned {@link JdbcRuntimeConfig} so the retry path can tell whether a reload
     * happened mid-flight.
     */
    JdbcConnector(
        JdbcHikariPool hikariPool,
        JdbcDialect dialect,
        String jdbcUrl,
        SecureString user,
        SecureString password,
        Map<String, String> connectionProperties,
        LongSupplier credentialEpoch
    ) {
        this(hikariPool, dialect, jdbcUrl, user, password, null, null, null, connectionProperties, credentialEpoch);
    }

    /**
     * Production constructor including the typed AWS credentials for Redshift IAM explicit-credentials mode.
     * {@code accessKeyId}/{@code secretAccessKey}/{@code sessionToken} are per-query {@link SecureString}s
     * forwarded to the driver under its {@code AccessKeyID}/{@code SecretAccessKey}/{@code SessionToken} property
     * names; any of them may be {@code null} (all null == ambient AWS credential chain). Like {@code user}/
     * {@code password} they are captured for the query's lifetime, are not refreshable, and are never logged.
     */
    JdbcConnector(
        JdbcHikariPool hikariPool,
        JdbcDialect dialect,
        String jdbcUrl,
        SecureString user,
        SecureString password,
        SecureString accessKeyId,
        SecureString secretAccessKey,
        SecureString sessionToken,
        Map<String, String> connectionProperties,
        LongSupplier credentialEpoch
    ) {
        this(
            requireHikariPool(hikariPool)::getConnection,
            dialect,
            jdbcUrl,
            new PerQueryCredentials(user, password, accessKeyId, secretAccessKey, sessionToken),
            connectionProperties,
            credentialEpoch
        );
    }

    /**
     * Canonical constructor with no {@code connection_properties} passthrough. Retained for unit tests that exercise
     * the credential/retry seams without tuning props; delegates with an empty map.
     */
    JdbcConnector(
        ConnectionSource connectionSource,
        JdbcDialect dialect,
        String jdbcUrl,
        CredentialSource credentials,
        LongSupplier credentialEpoch
    ) {
        this(connectionSource, dialect, jdbcUrl, credentials, Map.of(), credentialEpoch);
    }

    /**
     * Canonical constructor. Package-private and seam-based so unit tests can inject a {@link ConnectionSource} that
     * simulates a driver failure/recovery and a {@link CredentialSource} that models a refreshable (keystore-backed)
     * source, without a real pool or a real database.
     */
    JdbcConnector(
        ConnectionSource connectionSource,
        JdbcDialect dialect,
        String jdbcUrl,
        CredentialSource credentials,
        Map<String, String> connectionProperties,
        LongSupplier credentialEpoch
    ) {
        if (connectionSource == null) {
            throw new IllegalArgumentException("connectionSource must not be null");
        }
        if (dialect == null) {
            throw new IllegalArgumentException("dialect must not be null");
        }
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new IllegalArgumentException("jdbcUrl must not be null or empty");
        }
        if (credentials == null) {
            throw new IllegalArgumentException("credentials must not be null");
        }
        if (credentialEpoch == null) {
            throw new IllegalArgumentException("credentialEpoch must not be null");
        }
        this.connectionSource = connectionSource;
        this.dialect = dialect;
        this.jdbcUrl = jdbcUrl;
        this.credentials = credentials;
        this.connectionProperties = connectionProperties == null ? Map.of() : Map.copyOf(connectionProperties);
        this.credentialEpoch = credentialEpoch;
    }

    private static JdbcHikariPool requireHikariPool(JdbcHikariPool hikariPool) {
        if (hikariPool == null) {
            throw new IllegalArgumentException("hikariPool must not be null");
        }
        return hikariPool;
    }

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        return doExecute(request);
    }

    @Override
    public ResultCursor execute(QueryRequest request, ExternalSplit split) {
        return doExecute(request);
    }

    private ResultCursor doExecute(QueryRequest request) {
        // Pull table / schema / catalog from the per-query config, decoded by the framework before we see them.
        String table = stringConfig(request, "table");
        String schema = stringConfig(request, "schema");
        String catalog = stringConfig(request, "catalog");
        if (table == null) {
            throw new IllegalArgumentException("JDBC source requires WITH (table=\"<name>\")");
        }
        JdbcQueryBuilder builder = new JdbcQueryBuilder(dialect);
        // Predicate pushdown is DEFERRED (see JdbcConnectorFactory#filterPushdownSupport): main's QueryRequest carries
        // no pushed filter field and wiring one is a 5-surface core change out of scope here. Projection + LIMIT
        // pushdown still flow through request.projectedColumns() / request.rowLimit(). Pass null so the builder emits
        // a plain SELECT <projected> FROM <table> [LIMIT n]; the class is retained for the follow-up.
        JdbcPushedQuery pushedQuery = null;
        JdbcQueryBuilder.BuiltScan built = builder.buildScan(
            request.projectedColumns(),
            catalog,
            schema,
            table,
            request.rowLimit(),
            pushedQuery
        );

        // INFO observability: announce the query start with sanitized URL, table, projected column count, and a
        // pushdown flag. This is the only log line operators get for a JDBC query; keep it terse and free of
        // anything that could carry credentials (no params, no driver messages -- those flow through the
        // sanitized-exception path on failure).
        // Chattiness note: high-QPS deployments can downgrade this logger (org.elasticsearch.xpack.esql.datasource.jdbc.JdbcConnector)
        // to WARN via standard log4j2 config; the matching end-of-query line lives on JdbcResultCursor's logger so
        // both can be muted independently.
        boolean pushdownActive = pushedQuery != null;
        int projectedColumnCount = request.projectedColumns() != null ? request.projectedColumns().size() : 0;
        String sanitized = sanitizedUrl();
        logger.info(
            "JDBC query start url=[{}] table=[{}] projected_columns=[{}] pushdown=[{}] row_limit=[{}]",
            sanitized,
            table,
            projectedColumnCount,
            pushdownActive,
            request.rowLimit()
        );
        // DEBUG: emit the generated SQL. SqlRenderer already enforces parameterised placeholders, so the string
        // contains only `?` markers -- no bound values land in logs even at DEBUG.
        if (logger.isDebugEnabled()) {
            logger.debug("JDBC SQL [{}] params_count=[{}]", built.sql(), built.params().size());
        }

        long startNanos = System.nanoTime();
        // Read the credential epoch BEFORE building any credential Properties (attemptOnce builds them per attempt).
        // A change between here and an AUTH_FAILED response means a credential reload happened mid-flight, which --
        // for a refreshable credential source -- justifies the single retry with re-resolved credentials.
        long epochAtBuild = credentialEpoch.getAsLong();

        // Classifier-driven single retry: TRANSIENT_NETWORK gets a fresh pooled connection; AUTH_FAILED gets a
        // credential-refresh retry only when the source is refreshable AND a reload advanced the epoch (see
        // shouldRetry). Everything else propagates on the first attempt. attempt is 0 or 1 -- never an unbounded loop.
        SQLException lastFailure = null;
        JdbcSqlStateCategory lastCategory = JdbcSqlStateCategory.UNKNOWN;
        for (int attempt = 0; attempt <= 1; attempt++) {
            try {
                return attemptOnce(request, built, table, pushdownActive, startNanos, sanitized);
            } catch (SQLException e) {
                lastFailure = e;
                lastCategory = JdbcSqlStateClassifier.classify(e);
                if (attempt == 0 && shouldRetry(lastCategory, epochAtBuild)) {
                    // WARN so a retried failure is visible; message carries only the sanitized URL + SQLState +
                    // category (no driver text, no credentials).
                    logger.warn(
                        "JDBC query against [{}] failed with category [{}]{}; retrying once",
                        sanitized,
                        lastCategory,
                        sqlStateSuffix(e)
                    );
                    continue;
                }
                break;
            }
        }
        // Reached only after a non-retryable failure or an exhausted single retry. Drivers commonly include the
        // connection URL or named credential properties verbatim in their exception text, so we do NOT include
        // e.getMessage(); we replace the cause with a sanitized clone so SQLState/vendor diagnostics survive but
        // credentials never reach a log appender. SQLState (when present) + the classifier category are appended for
        // grep-ability and operator triage.
        throw new IllegalStateException(
            "failed to execute JDBC query against [" + sanitized + "]" + sqlStateSuffix(lastFailure) + " category=[" + lastCategory + "]",
            JdbcUrlSanitizer.sanitizeException(lastFailure)
        );
    }

    /**
     * Runs one full open + execute attempt, transferring resource ownership to the returned {@link JdbcResultCursor}
     * on success and closing any partially-opened resources (reverse order) on failure. Throws the raw
     * {@link SQLException} so the caller can classify it for the retry decision; a non-{@link SQLException} (e.g. the
     * {@link IllegalStateException} the pool raises on an acquisition timeout) propagates uncaught and is never
     * retried.
     */
    private ResultCursor attemptOnce(
        QueryRequest request,
        JdbcQueryBuilder.BuiltScan built,
        String table,
        boolean pushdownActive,
        long startNanos,
        String sanitized
    ) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = openConnection();
            // Read the connection's DatabaseMetaData ONCE, immediately after connecting, and drive both once-per-URL
            // observability checks off it BEFORE any dialect session setup. getMetaData() needs only a live connection
            // (not configureConnection / initStatements), and ordering the advisory FIRST is deliberate: a store
            // reached via the wrong prefix (e.g. Redshift via jdbc:postgresql://) may reject a Postgres init statement
            // such as `SET statement_timeout`, and if the advisory ran after initStatements that failure would
            // propagate before the "consider jdbc:redshift://" hint was ever logged -- defeating the advisory in
            // exactly the scenario it exists for. Emitting it here means the operator gets the hint that EXPLAINS the
            // subsequent init failure. Single getMetaData() call -- no extra round-trip.
            DatabaseMetaData metaData = conn.getMetaData();
            // Once-per-URL advisory when the connected product suggests a different vendor scheme than the resolved
            // dialect (e.g. Redshift reached via the postgresql dialect). Advisory only -- never fails the query.
            adviseOnDatabaseProduct(dialect, metaData, sanitized);
            // Once-per-URL WARN when the server major is outside the dialect's verified set. Never fails the query.
            checkDatabaseVersion(dialect, metaData, sanitized);
            dialect.configureConnection(conn);
            // Establish deterministic session state (e.g. Postgres UTC + server-side statement_timeout). A failure
            // here leaves the session half-configured and unusable, so it propagates as the same sanitized hard
            // failure as any other SQLException in this open path -- but only AFTER the advisory above has fired.
            applyInitStatements(conn);
            stmt = conn.prepareStatement(built.sql(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            dialect.configureStatement(stmt);
            bindParams(stmt, built.params());
            rs = stmt.executeQuery();
            JdbcResultCursor cursor = new JdbcResultCursor(
                conn,
                stmt,
                rs,
                request.attributes(),
                // The cursor produces pages on the connector's producer thread (AsyncConnectorSourceOperatorFactory
                // runs the drain loop off the Driver run loop), so it must allocate against the root request-level
                // factory rather than the driver-local child factory. BlockFactory.parent() returns that thread-safe
                // root (or this factory when already a root); allocating on the local child would race the Driver's
                // run loop and trip LocalCircuitBreaker's single-thread assertion. See BlockFactory#parent() Javadoc.
                request.blockFactory().parent(),
                request.batchSize(),
                request.rowLimit(),
                // The completion line goes to the cursor's logger so per-cursor observability stays attributable
                // (and a unit test using MockLog.capture(JdbcResultCursor.class) sees it). The factory keeps the
                // CompletionLogger ctor accessible for tests that want to inject a custom Logger.
                JdbcResultCursor.CompletionLogger.create(sanitized, table, pushdownActive, startNanos)
            );
            // Cursor now owns rs/stmt/conn; null them so finally doesn't double-close.
            rs = null;
            stmt = null;
            conn = null;
            return cursor;
        } finally {
            // JDBC types are AutoCloseable, not Closeable -- IOUtils' helpers want Closeable, so close inline.
            // Reverse order of open. Suppressed exceptions piggyback on the in-flight SQLException, if any.
            closeQuietly(rs);
            closeQuietly(stmt);
            closeQuietly(conn);
        }
    }

    /**
     * The classifier-driven retry decision, exactly once per query. {@link JdbcSqlStateCategory#TRANSIENT_NETWORK} is
     * always retried (a fresh borrow from the pool). {@link JdbcSqlStateCategory#AUTH_FAILED} is retried only when the
     * credential source can produce a fresh generation ({@link CredentialSource#refreshable()}) AND a reload advanced
     * the {@linkplain JdbcRuntimeConfig#credentialEpoch() epoch} since the credentials were built -- otherwise a retry
     * would re-present byte-identical credentials and fail the same way, so it fails fast (this is the honest scope
     * for the per-query credential source). Every other category is a
     * fail-fast. All enum constants are enumerated so the compiler enforces completeness.
     */
    private boolean shouldRetry(JdbcSqlStateCategory category, long epochAtBuild) {
        return switch (category) {
            case TRANSIENT_NETWORK -> true;
            case AUTH_FAILED -> credentials.refreshable() && credentialEpoch.getAsLong() != epochAtBuild;
            case DEADLOCK, RESOURCE_EXHAUSTED, SYNTAX_ERROR, DATA_ERROR, INTEGRITY_VIOLATION, PERMISSION, CANCELLED_BY_USER, UNKNOWN ->
                false;
        };
    }

    private static String sqlStateSuffix(SQLException e) {
        String sqlState = e == null ? null : e.getSQLState();
        return sqlState == null ? "" : " (sqlstate=" + sqlState + ")";
    }

    private void bindParams(PreparedStatement stmt, List<SqlParam> params) throws SQLException {
        // 1-based JDBC indexing. Empty list is the common case (no pushdown); the loop is a no-op.
        for (int i = 0; i < params.size(); i++) {
            SqlParam p = params.get(i);
            dialect.bindParam(stmt, i + 1, p.value(), p.esqlType());
        }
    }

    /**
     * Runs the dialect's {@link JdbcDialect#initStatements()} once, on a scratch {@link Statement} closed in the same
     * try-with-resources. No-op for a dialect (e.g. {@link GenericDialect}) that declares none. Any driver error
     * propagates so the caller turns it into a sanitized hard failure -- a half-applied init leaves the session in an
     * inconsistent state and must not serve the query.
     */
    private void applyInitStatements(Connection conn) throws SQLException {
        List<String> statements = dialect.initStatements();
        if (statements.isEmpty()) {
            return;
        }
        try (Statement init = conn.createStatement()) {
            for (String sql : statements) {
                init.execute(sql);
            }
        }
    }

    /**
     * Logs a WARN, at most once per {@code sanitizedUrl}, when the connected database's major version falls outside
     * the dialect's {@link JdbcDialect#supportedDatabaseMajorVersions() verified set}. An empty verified set (the
     * generic dialect) disables the check entirely. Never throws for a version mismatch: the query proceeds, we just
     * flag that behavior on that major is unverified. Package-private + static so it can be unit-tested against a
     * mock {@link DatabaseMetaData} without opening a real connection.
     */
    static void checkDatabaseVersion(JdbcDialect dialect, DatabaseMetaData metaData, String sanitizedUrl) throws SQLException {
        Set<Integer> supported = dialect.supportedDatabaseMajorVersions();
        if (supported.isEmpty()) {
            return;
        }
        int major = metaData.getDatabaseMajorVersion();
        if (supported.contains(major) == false && VERSION_WARNED.putIfAbsent(sanitizedUrl, Boolean.TRUE) == null) {
            logger.warn(
                "JDBC database at [{}] reports major version [{}], outside the versions verified for dialect [{}] {}; "
                    + "proceeding, but behavior on this major is unverified",
                sanitizedUrl,
                major,
                dialect.name(),
                supported
            );
        }
    }

    /**
     * Logs a WARN, at most once per {@code sanitizedUrl}, when the connected database's
     * {@link DatabaseMetaData#getDatabaseProductName() product name} indicates a store that is natively served by a
     * different dialect than the one the URL prefix selected -- e.g. connecting to Amazon Redshift
     * (pg-wire compatible) through {@code jdbc:postgresql://}, which resolves to {@link PostgresDialect} and so
     * silently applies Postgres deltas rather than the vendor's. The advisory names the better-fitting
     * {@code jdbc:<vendor>://} scheme so an operator can opt into vendor-specific handling; it stays silent when the
     * product and the resolved dialect agree (PostgreSQL + {@code postgresql}, H2 + {@code generic}) or when the
     * product is unrecognized (we only speak up when we can name a concrete alternative).
     * <p>
     * <b>Why advise, not auto-switch.</b> The dialect is chosen from the URL prefix <em>before</em> connecting because
     * it is needed to {@link JdbcDialect#quoteIdentifier quote identifiers} while building the SQL; the product name is
     * only known <em>after</em> connecting, by which point the SQL has already been quoted with (and the session
     * already configured for) the prefix-chosen dialect. Swapping dialects mid-flight would therefore risk an
     * inconsistent statement, so the safe, honest behavior is a one-time operator advisory rather than a risky
     * post-connect switch. A real vendor dialect (e.g. {@code RedshiftDialect}) plus its own {@code jdbc:redshift://}
     * prefix is the correct fix, and is deferred to a later iteration.
     * <p>
     * <b>Ordering.</b> This runs immediately after the connection opens and BEFORE the dialect's
     * {@link JdbcDialect#initStatements() init statements} (see {@link #attemptOnce}). That order matters precisely for
     * the headline case: a pg-wire store reached via {@code jdbc:postgresql://} may reject a Postgres init statement
     * (Redshift, for one, rejects session {@code SET statement_timeout}); if the advisory ran after
     * {@code initStatements} that failure would propagate before this WARN was logged, defeating the advisory in the
     * exact scenario it exists for. Emitting it first means the operator gets the {@code consider jdbc:redshift://}
     * hint that EXPLAINS the subsequent init failure.
     * <p>
     * <b>Known limitation (product-name false-negative).</b> Matching is by {@code getDatabaseProductName()} substring,
     * so it only fires when the driver actually reports the vendor's name. Amazon Redshift reached
     * through the PLAIN pgjdbc driver reports {@code getDatabaseProductName()} = {@code "PostgreSQL"} (pgjdbc 42.7.3
     * hard-codes it): that case matches the {@code postgresql} profile, agrees with the resolved
     * {@code postgresql} dialect, and stays SILENT -- an undetectable-by-product-name false-negative.
     * The advisory therefore fires only when a vendor-native driver (Redshift's {@code redshift-jdbc} reporting
     * {@code "Redshift"}) is used. This is fail-silent and safe
     * (no wrong WARN, never fails the query); the robust fix is a dedicated {@code jdbc:redshift://} (shipped)
     * prefix + dialect, not product sniffing.
     * <p>
     * Reuses the {@link DatabaseMetaData} already fetched for {@link #checkDatabaseVersion} (no extra round-trip) and
     * is strictly advisory: any driver error reading the product name is swallowed at DEBUG so it can never fail the
     * query. Package-private + static so it can be unit-tested against a mock {@link DatabaseMetaData}.
     */
    static void adviseOnDatabaseProduct(JdbcDialect dialect, DatabaseMetaData metaData, String sanitizedUrl) {
        final String product;
        try {
            product = metaData.getDatabaseProductName();
        } catch (SQLException | RuntimeException e) {
            // Advisory only: a driver that refuses getDatabaseProductName() must never turn this into a query failure.
            logger.debug("could not read database product name for the product-name advisory; skipping", e);
            return;
        }
        if (product == null || product.isEmpty()) {
            return;
        }
        String lower = product.toLowerCase(Locale.ROOT);
        for (ProductProfile profile : PRODUCT_PROFILES) {
            if (lower.contains(profile.substring())) {
                // Warn only when the connected product is natively served by a different dialect than the resolved
                // one, and only once per endpoint.
                if (dialect.name().equals(profile.dialectName()) == false
                    && PRODUCT_ADVISORY_WARNED.putIfAbsent(sanitizedUrl, Boolean.TRUE) == null) {
                    logger.warn(
                        "JDBC endpoint [{}] connected to [{}] via dialect [{}]; consider jdbc:{}:// for {}-specific handling",
                        sanitizedUrl,
                        product,
                        dialect.name(),
                        profile.vendorToken(),
                        profile.vendorToken()
                    );
                }
                return;
            }
        }
    }

    private Connection openConnection() throws SQLException {
        // Build a fresh Properties per call: Driver implementations are free to retain a reference to the map, so we
        // cannot share one across executions. The CredentialSource copies credentials in defensively (per-query
        // SecureStrings are clone()d so the caller-owned originals are never zeroed). The Properties entries
        // themselves are removed in the finally block so a stray toString() elsewhere cannot leak them.
        Properties props = new Properties();
        try {
            credentials.writeInto(props);
            // Layer the allowlisted, non-secret tuning props on top. applyTo never overwrites an existing entry, so the
            // typed user/password just written by the credential source always win. These props are part of the pool
            // key (see JdbcHikariPool#poolKey), so a different sslmode/ApplicationName gets its own physical pool.
            JdbcConnectionProperties.applyTo(props, connectionProperties);
            // Borrow from the per-endpoint HikariCP pool (via the ConnectionSource seam). The pool captures these
            // credentials for the endpoint on first creation (see JdbcHikariPool); a pool-acquisition timeout is
            // translated there into a sanitized, fail-fast IllegalStateException rather than blocking the esql_worker
            // thread indefinitely -- and, because it is an IllegalStateException (not a SQLException), it is never a
            // retry candidate here (pool exhaustion is not a network blip).
            return connectionSource.getConnection(jdbcUrl, props);
        } finally {
            // Drop every secret entry; no-op if a key was never set. Includes the typed AWS credentials (Redshift
            // IAM explicit-creds mode) alongside user/password so a stray toString() elsewhere cannot leak them.
            props.remove("user");
            props.remove("password");
            props.remove("AccessKeyID");
            props.remove("SecretAccessKey");
            props.remove("SessionToken");
        }
    }

    /**
     * Seam over the physical-connection source. Production binds {@link JdbcHikariPool#getConnection}; unit tests
     * inject a fake that simulates driver failures (an {@code AUTH_FAILED}/{@code TRANSIENT_NETWORK} SQLException on
     * the first borrow) and recovery (a real connection on the second) to exercise the retry policy without a pool.
     */
    @FunctionalInterface
    interface ConnectionSource {
        Connection getConnection(String jdbcUrl, Properties props) throws SQLException;
    }

    /**
     * Seam over credential resolution. {@link #writeInto} populates the {@code user}/{@code password} keys of the
     * connection {@link Properties}; {@link #refreshable} states whether an {@code AUTH_FAILED} can be resolved by
     * re-resolving credentials after a reload epoch bump. The production source ({@link PerQueryCredentials}) is not
     * refreshable; a future node-keystore-backed source would be.
     */
    interface CredentialSource {
        void writeInto(Properties props);

        boolean refreshable();
    }

    /**
     * Production {@link CredentialSource}: the per-query {@link SecureString} {@code user}/{@code password} (and, for
     * Redshift IAM explicit-creds mode, the AWS {@code AccessKeyID}/{@code SecretAccessKey}/{@code SessionToken})
     * decrypted from the data-source definition and captured for the query's lifetime. They are immutable and cannot
     * be re-fetched from a fresher source, so {@link #refreshable()} is {@code false} -- an {@code AUTH_FAILED} fails
     * fast rather than re-presenting byte-identical credentials (a fake refresh).
     * Package-private (not private) so a unit test can construct it directly and assert, via a capturing
     * {@link ConnectionSource}, that every secret reaches the driver {@link Properties} from its SecureString.
     */
    static final class PerQueryCredentials implements CredentialSource {
        private final SecureString user;
        private final SecureString password;
        private final SecureString accessKeyId;
        private final SecureString secretAccessKey;
        private final SecureString sessionToken;

        PerQueryCredentials(SecureString user, SecureString password) {
            this(user, password, null, null, null);
        }

        PerQueryCredentials(
            SecureString user,
            SecureString password,
            SecureString accessKeyId,
            SecureString secretAccessKey,
            SecureString sessionToken
        ) {
            this.user = user;
            this.password = password;
            this.accessKeyId = accessKeyId;
            this.secretAccessKey = secretAccessKey;
            this.sessionToken = sessionToken;
        }

        @Override
        public void writeInto(Properties props) {
            // SecureString.getChars() returns the live backing array, so we MUST NOT zero it here -- that would
            // corrupt the caller-owned SecureString reused across executions. clone() + close() zeros only the copy;
            // the char[] is materialized into an unwipeable Properties String (a JDBC API limitation).
            writeSecret(props, "user", user);
            writeSecret(props, "password", password);
            // Typed AWS credentials (Redshift IAM explicit-creds mode). All null in the common user/password and
            // ambient-chain cases, in which nothing AWS-related is written and the driver uses its default chain.
            writeSecret(props, "AccessKeyID", accessKeyId);
            writeSecret(props, "SecretAccessKey", secretAccessKey);
            writeSecret(props, "SessionToken", sessionToken);
        }

        private static void writeSecret(Properties props, String key, SecureString value) {
            if (value != null) {
                try (SecureString copy = value.clone()) {
                    props.setProperty(key, new String(copy.getChars()));
                }
            }
        }

        @Override
        public boolean refreshable() {
            return false;
        }
    }

    /**
     * Redacts credential-bearing components from the JDBC URL for use in log lines and exception messages.
     * Handles all four common credential carriers in JDBC URLs:
     * <ul>
     *   <li>{@code user:pass@host} userinfo (Postgres, MySQL, etc.) -- replaced with {@code REDACTED@}</li>
     *   <li>Oracle thin form {@code jdbc:oracle:thin:user/pass@host:port:sid} -- userinfo before the {@code @} dropped</li>
     *   <li>{@code ?user=...&password=...} query parameters (any vendor) -- entire query string dropped</li>
     *   <li>{@code ;user=...;password=...} property suffix (SQL Server, Sybase) -- key=value pairs whose key matches
     *       a credential name redacted in place; non-credential properties preserved</li>
     * </ul>
     * The result preserves the host/port/db so an operator can still locate the failing endpoint.
     */
    String sanitizedUrl() {
        return JdbcUrlSanitizer.sanitize(jdbcUrl);
    }

    @Override
    public void close() throws IOException {
        // Per-query connections are owned by the cursor, not by this connector. Nothing to close here today.
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception e) {
            // Best-effort cleanup of a partially-opened JDBC resource. The caller is already propagating the
            // primary failure; downgrading this to debug avoids drowning logs with secondary noise.
            logger.debug("ignoring close failure", e);
        }
    }

    /**
     * Reads a non-credential string config key ({@code table}, {@code schema}, {@code catalog}). Explicitly
     * refuses a {@link SecureString} to defend against future plumbing accidents that route credentials through
     * this method -- {@code SecureString.toString()} would otherwise leak the password into log lines that
     * concatenate the returned value. Credentials live in their own SecureString-typed channel
     * ({@code user}/{@code password}) handled by {@link JdbcConnectorFactory#secureStringConfig}.
     */
    private static String stringConfig(QueryRequest request, String key) {
        Object value = request.config() == null ? null : request.config().get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof SecureString) {
            throw new IllegalArgumentException("config key [" + key + "] must be a plain string, not a SecureString");
        }
        if (value instanceof String s) {
            return s;
        }
        if (value instanceof CharSequence cs) {
            return cs.toString();
        }
        if (value instanceof List<?> list && list.isEmpty() == false) {
            Object first = list.get(0);
            if (first instanceof SecureString) {
                throw new IllegalArgumentException("config key [" + key + "] must not contain a SecureString");
            }
            return first.toString();
        }
        return value.toString();
    }
}
