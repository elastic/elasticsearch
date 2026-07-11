/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.zaxxer.hikari.pool.HikariPool;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.Closeable;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTransientConnectionException;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

/**
 * Per-endpoint-and-credential {@link HikariDataSource} cache for the JDBC connector.
 * <p>
 * <b>Keying.</b> A pool is created lazily per {@code (normalized-endpoint, credential-fingerprint)} pair:
 * <pre>{@code pool key = normalizeKey(url) + ":" + sha256(user + '\u0000' + password)}</pre>
 * The normalized endpoint drops the query string ({@code ?...}) and the URL userinfo ({@code user:pass@}) and
 * lower-cases the remainder; the credential fingerprint is a one-way SHA-256 over the per-query {@code user}/
 * {@code password}. Consequences:
 * <ul>
 *   <li>Same endpoint + same credentials &rarr; the same pool is reused (physical connections are shared).</li>
 *   <li>Same endpoint + <em>different</em> credentials &rarr; <em>separate</em> pools, each authenticating with its
 *       own captured credentials. This preserves the per-query credential contract: a second caller can never run
 *       under a first caller's DB identity (no silent "first-caller-wins" identity swap).</li>
 * </ul>
 * The fingerprint is a non-reversible hash used only as an in-memory map key; the raw credentials and the
 * fingerprint itself are <em>never</em> logged and never appear in pool names, thread names, or exception messages
 * (pool names carry only the {@link JdbcUrlSanitizer sanitized} URL). A null/absent {@code user} or {@code password}
 * is hashed deterministically as the empty string, so credential-less endpoints all key to one stable fingerprint.
 * <p>
 * <b>Connection source.</b> HikariCP obtains physical connections through {@link DriverRegistryDataSource}, which
 * delegates to {@link JdbcDriverRegistry#connect(String, Properties)} and therefore uses the plugin's isolated
 * driver {@link ClassLoader} rather than {@link java.sql.DriverManager}.
 * <p>
 * <b>Ownership + quiescence.</b> This object is an instance-owned field of {@link JdbcDataSourcePlugin} (never a
 * static) and is released in {@link JdbcDataSourcePlugin#close()} <em>before</em> the driver registry, because
 * closing pooled physical connections needs the driver classes the registry's classloader holds.
 * <p>
 * <b>Sizing / fail-fast.</b> JDBC producers run on {@code esql_worker} threads,
 * so a producer blocked in {@link #getConnection} occupies its worker thread. A short {@code connectionTimeout}
 * (default 5s) is the key lever: a producer that cannot obtain a connection fails fast with a translated
 * {@link IllegalStateException} instead of parking a worker thread indefinitely.
 * <p>
 * <b>Config immutability at pool creation.</b> The sizing/timeout knobs (all {@code esql.jdbc.pool.*} settings) are
 * read from {@link JdbcRuntimeConfig} once, when an endpoint's pool is CREATED (see {@link #applyPoolSizingAndTimeouts}).
 * A later dynamic settings update does NOT re-tune live pools; it takes effect only on pools created afterwards
 * (consistent with the documented pool behaviour). An existing pool therefore keeps its construction-time values
 * until it is evicted and re-created.
 */
final class JdbcHikariPool implements Closeable {

    private static final Logger logger = LogManager.getLogger(JdbcHikariPool.class);

    private final JdbcDriverRegistry driverRegistry;
    private final JdbcRuntimeConfig config;
    private final ConcurrentHashMap<String, HikariDataSource> pools = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    /**
     * Per-endpoint dedupe for the pool keepalive/validation clamp/disable WARNs. Keyed by
     * {@code sanitizedUrl + NUL + category} so a mis-ordered setting logs once per sanitized URL rather than once per
     * {@code (url × credential)} pool, while the independent {@code validation} and {@code keepalive} categories never
     * suppress each other. Backed by a {@link BoundedWarnOnceMap} so unbounded endpoint churn cannot grow it.
     */
    private final Map<String, Boolean> clampWarned = BoundedWarnOnceMap.create();

    JdbcHikariPool(JdbcDriverRegistry driverRegistry, JdbcRuntimeConfig config) {
        if (driverRegistry == null) {
            throw new IllegalArgumentException("driverRegistry must not be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.driverRegistry = driverRegistry;
        this.config = config;
    }

    /**
     * Borrows a pooled {@link Connection} for {@code jdbcUrl}, creating the endpoint's pool on first use. On a
     * pool-acquisition timeout, HikariCP's {@link SQLTransientConnectionException} is translated to a sanitized,
     * operator-actionable {@link IllegalStateException} (never blocks indefinitely).
     */
    Connection getConnection(String jdbcUrl, Properties props) throws SQLException {
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new IllegalArgumentException("jdbcUrl must not be null or empty");
        }
        if (closed) {
            throw new IllegalStateException("JDBC connection pool is closed");
        }
        String key = poolKey(jdbcUrl, props);
        HikariDataSource dataSource;
        try {
            dataSource = pools.computeIfAbsent(key, k -> createDataSource(jdbcUrl, props));
        } catch (HikariPool.PoolInitializationException e) {
            // Pool fail-fast case: HikariCP validates a single physical connection
            // at pool-creation time (initializationFailTimeout). A FIRST-borrow AUTHENTICATION failure (bad Redshift
            // IAM / token / user-password credential) therefore surfaces HERE as a RuntimeException that would
            // otherwise propagate to the connector UNCLASSIFIED and UNSANITIZED (bypassing the SQLException catch in
            // JdbcConnector). Unwrap the cause and, if it is an auth failure, surface the sanitized underlying
            // SQLException so the connector classifies it AUTH_FAILED and fails fast. Non-auth pool-init failures keep
            // their prior behaviour (rethrown unchanged).
            SQLException authCause = authFailureCause(e);
            if (authCause != null) {
                throw JdbcUrlSanitizer.sanitizeException(authCause);
            }
            throw e;
        }
        // Lost race with close(): a pool created after close() must not linger. Evict + close it and refuse.
        if (closed && pools.remove(key, dataSource)) {
            closeQuietly(dataSource);
            throw new IllegalStateException("JDBC connection pool is closed");
        }
        try {
            return dataSource.getConnection();
        } catch (SQLTransientConnectionException e) {
            // Acquisition case: HikariCP reports a connection-CREATION failure
            // during acquisition (e.g. credentials that turned invalid after the pool was already initialized) as a
            // generic SQLTransientConnectionException (pool-acquisition timeout). When the underlying cause is an
            // AUTHENTICATION failure (SQLState 28xxx), masking it as a pool-timeout hides the actionable cause. Unwrap
            // the cause chain and, if the real failure classifies as AUTH_FAILED, surface that underlying SQLException
            // (SANITIZED) so the connector turns it into an actionable AUTH_FAILED and fails fast (no retry for a
            // non-refreshable per-query credential). A genuine pool-exhaustion timeout (no auth cause) is unchanged --
            // still the fast-fail, sanitized IllegalStateException with pool_max/in_use diagnostics.
            SQLException authCause = authFailureCause(e);
            if (authCause != null) {
                throw JdbcUrlSanitizer.sanitizeException(authCause);
            }
            throw translateTimeout(e, jdbcUrl, dataSource);
        }
    }

    /**
     * Walks the cause chain BELOW {@code wrapper} (skipping HikariCP's own wrapper, whose {@code SQLState} may be a
     * generic connection-class {@code 08xxx} that would otherwise mask a deeper auth failure) looking for the first
     * {@link SQLException} that {@link JdbcSqlStateClassifier classifies} as {@link JdbcSqlStateCategory#AUTH_FAILED}.
     * Returns it, or {@code null} when nothing in the chain is an authentication failure (e.g. the ordinary
     * pool-exhaustion timeout, or a transient network failure). Handles both HikariCP masking shapes: the acquisition
     * {@link SQLTransientConnectionException} and the pool-init {@link HikariPool.PoolInitializationException}.
     * Depth-bounded as a defensive belt against a pathological driver chain. Package-private + static so it can be
     * unit-tested with a hand-built wrapped {@code 28000} without standing up a real pool.
     */
    static SQLException authFailureCause(Throwable wrapper) {
        Throwable cause = wrapper == null ? null : wrapper.getCause();
        int depth = 0;
        while (cause != null && depth++ < 16) {
            if (cause instanceof SQLException sqlCause && JdbcSqlStateClassifier.classify(sqlCause) == JdbcSqlStateCategory.AUTH_FAILED) {
                return sqlCause;
            }
            cause = cause.getCause();
        }
        return null;
    }

    /** HikariCP's hard floor for {@code keepaliveTime} when enabled: it silently disables anything below 30s. */
    static final long HIKARI_KEEPALIVE_FLOOR_MS = 30_000L;

    /**
     * Once-per-URL gate for the pool-config clamp/disable WARNs. {@link #shouldWarn} returns
     * {@code true} the first time a given {@code category} ({@code "validation"} / {@code "keepalive"}) is seen for a
     * pool's endpoint and {@code false} thereafter, so a mis-ordered setting logs once per sanitized URL rather than
     * once per {@code (url × credential)} pool. {@link #ALWAYS} suppresses nothing and is used by the package-private,
     * directly-unit-tested static appliers (which log unconditionally, one call per test).
     */
    @FunctionalInterface
    interface WarnOnce {
        boolean shouldWarn(String category);

        WarnOnce ALWAYS = category -> true;
    }

    private HikariDataSource createDataSource(String jdbcUrl, Properties props) {
        HikariConfig hc = new HikariConfig();
        // Route physical connections through the plugin's isolated driver classloader, NOT DriverManager.
        hc.setDataSource(new DriverRegistryDataSource(driverRegistry, jdbcUrl, copyOf(props)));
        // Pool name is sanitized (userinfo/query already stripped from the key; sanitize defensively) so it never
        // leaks credentials into HikariCP thread names or its own log lines.
        String sanitizedUrl = JdbcUrlSanitizer.sanitize(jdbcUrl);
        hc.setPoolName("esql-jdbc[" + sanitizedUrl + "]");
        // Sizing + timeouts, with the keepalive/validation ordering invariant enforced BEFORE HikariCP's own
        // validateNumerics runs (so a mis-ordered value is reported with an actionable WARN, not silently reset).
        // The clamp/disable WARN is deduped once per sanitized URL: distinct credentials on one
        // endpoint create distinct pools, but a mis-ordered setting is the endpoint's, so it must log once, not N times.
        applyPoolSizingAndTimeouts(
            hc,
            config,
            category -> clampWarned.putIfAbsent(sanitizedUrl + '\u0000' + category, Boolean.TRUE) == null
        );
        logger.info(
            "creating JDBC connection pool [{}] max_per_url=[{}] connection_timeout_ms=[{}] idle_timeout_ms=[{}] "
                + "max_lifetime_ms=[{}] keepalive_ms=[{}] validation_timeout_ms=[{}]",
            hc.getPoolName(),
            hc.getMaximumPoolSize(),
            hc.getConnectionTimeout(),
            hc.getIdleTimeout(),
            hc.getMaxLifetime(),
            hc.getKeepaliveTime(),
            hc.getValidationTimeout()
        );
        return new HikariDataSource(hc);
    }

    /**
     * Applies the pool sizing and all timeout knobs from {@code config} onto {@code hc}, enforcing the keepalive/
     * validation ordering invariant so HikariCP never has to silently correct a mis-ordered value. Package-private
     * + static so a unit test
     * can assert the resulting {@link HikariConfig} getters (including the clamped/disabled cases) without constructing
     * a real {@link HikariDataSource} (which would open a physical connection). The invariant:
     * <ul>
     *   <li>{@code validationTimeout <= connectionTimeout} — the per-borrow {@code isValid()} budget must not exceed
     *       the overall acquisition budget; a larger value is <b>clamped down</b> to {@code connectionTimeout}.</li>
     *   <li>{@code keepalive} is either {@code 0} (disabled) or satisfies
     *       {@code 30000 <= keepalive < idleTimeout < maxLifetime}. A positive-but-mis-ordered keepalive is
     *       <b>disabled</b> (left unset, HikariCP default {@code 0}) with an actionable WARN, rather than handed to
     *       HikariCP which would reset it silently.</li>
     * </ul>
     * {@code idleTimeout < maxLifetime} is HikariCP's own long-standing concern and is left to HikariCP; it is only
     * documented here as the middle term of the full ordering.
     * <p>
     * All knobs are read from {@code config} at this pool-CREATION point only; a later dynamic settings update applies
     * on the next (re)creation of an endpoint's pool, never to a live pool. This 2-arg overload
     * logs the clamp/disable WARN unconditionally (used by unit tests); the production path
     * ({@link #createDataSource}) passes a {@link WarnOnce} that dedupes the WARN once per sanitized URL.
     */
    static void applyPoolSizingAndTimeouts(HikariConfig hc, JdbcRuntimeConfig config) {
        applyPoolSizingAndTimeouts(hc, config, WarnOnce.ALWAYS);
    }

    /**
     * As {@link #applyPoolSizingAndTimeouts(HikariConfig, JdbcRuntimeConfig)}, but routes the clamp/disable WARNs
     * through {@code warnOnce} so the production path logs them at most once per sanitized URL. The
     * clamping/disabling itself is unconditional; only the WARN emission is gated.
     */
    static void applyPoolSizingAndTimeouts(HikariConfig hc, JdbcRuntimeConfig config, WarnOnce warnOnce) {
        long connectionTimeoutMs = config.poolConnectionTimeoutMs();
        long idleTimeoutMs = config.poolIdleTimeoutMs();
        long maxLifetimeMs = config.poolMaxLifetimeMs();
        long keepaliveMs = config.poolKeepaliveMs();
        long validationTimeoutMs = config.poolValidationTimeoutMs();

        hc.setMaximumPoolSize(config.poolMaxPerUrl());
        // Do not pre-open idle connections: create on demand, fail fast when saturated. Keeps a saturated pool from
        // holding DB connections a query never uses, and keeps construction thread-free until first borrow.
        hc.setMinimumIdle(0);
        hc.setConnectionTimeout(connectionTimeoutMs);
        hc.setIdleTimeout(idleTimeoutMs);
        hc.setMaxLifetime(maxLifetimeMs);

        // validationTimeout <= connectionTimeout (clamp; WARN deduped once per sanitized URL via warnOnce).
        long effectiveValidation = validationTimeoutMs;
        if (effectiveValidation > connectionTimeoutMs) {
            if (warnOnce.shouldWarn("validation")) {
                logger.warn(
                    "esql.jdbc.pool.validation_timeout_ms [{}] exceeds esql.jdbc.pool.connection_timeout_ms [{}]; "
                        + "clamping validation_timeout_ms to [{}] (the on-borrow isValid() budget must not exceed the "
                        + "connection-acquisition budget)",
                    validationTimeoutMs,
                    connectionTimeoutMs,
                    connectionTimeoutMs
                );
            }
            effectiveValidation = connectionTimeoutMs;
        }
        hc.setValidationTimeout(effectiveValidation);

        // keepalive: 0 (disabled) or 30000 <= keepalive < idleTimeout < maxLifetime; otherwise disable with a WARN.
        long effectiveKeepalive = effectiveKeepaliveMs(keepaliveMs, idleTimeoutMs, maxLifetimeMs, warnOnce);
        if (effectiveKeepalive > 0) {
            hc.setKeepaliveTime(effectiveKeepalive);
        }
    }

    /**
     * Resolves the effective HikariCP {@code keepaliveTime} for the configured value, returning {@code 0} (disabled)
     * whenever it would violate the ordering invariant so the caller leaves the HikariCP default in place. A disabled
     * result for a positive input is accompanied by an actionable WARN naming the offending relationship. Package-
     * private + static for direct unit testing of every branch.
     */
    static long effectiveKeepaliveMs(long keepaliveMs, long idleTimeoutMs, long maxLifetimeMs) {
        return effectiveKeepaliveMs(keepaliveMs, idleTimeoutMs, maxLifetimeMs, WarnOnce.ALWAYS);
    }

    /**
     * As {@link #effectiveKeepaliveMs(long, long, long)}, but gates the disable WARN through {@code warnOnce} so the
     * production path logs it at most once per sanitized URL. The returned effective value is
     * unaffected by {@code warnOnce}.
     */
    static long effectiveKeepaliveMs(long keepaliveMs, long idleTimeoutMs, long maxLifetimeMs, WarnOnce warnOnce) {
        if (keepaliveMs <= 0) {
            return 0L; // disabled — the default, and a trivially-valid configuration
        }
        if (keepaliveMs < HIKARI_KEEPALIVE_FLOOR_MS) {
            if (warnOnce.shouldWarn("keepalive")) {
                logger.warn(
                    "esql.jdbc.pool.keepalive_ms [{}] is below HikariCP's [{}]ms floor; disabling keepalive (raise it to "
                        + ">= [{}] and below idle_timeout_ms to enable proactive stale-connection eviction)",
                    keepaliveMs,
                    HIKARI_KEEPALIVE_FLOOR_MS,
                    HIKARI_KEEPALIVE_FLOOR_MS
                );
            }
            return 0L;
        }
        if (idleTimeoutMs > 0 && keepaliveMs >= idleTimeoutMs) {
            if (warnOnce.shouldWarn("keepalive")) {
                logger.warn(
                    "esql.jdbc.pool.keepalive_ms [{}] is >= esql.jdbc.pool.idle_timeout_ms [{}]; disabling keepalive "
                        + "(an idle connection would be retired before the keepalive could fire — set keepalive_ms below "
                        + "idle_timeout_ms)",
                    keepaliveMs,
                    idleTimeoutMs
                );
            }
            return 0L;
        }
        if (maxLifetimeMs > 0 && keepaliveMs >= maxLifetimeMs) {
            if (warnOnce.shouldWarn("keepalive")) {
                logger.warn(
                    "esql.jdbc.pool.keepalive_ms [{}] is >= esql.jdbc.pool.max_lifetime_ms [{}]; disabling keepalive "
                        + "(set keepalive_ms below max_lifetime_ms)",
                    keepaliveMs,
                    maxLifetimeMs
                );
            }
            return 0L;
        }
        return keepaliveMs;
    }

    private IllegalStateException translateTimeout(SQLTransientConnectionException e, String jdbcUrl, HikariDataSource dataSource) {
        int poolMax = dataSource.getMaximumPoolSize();
        int inUse = -1;
        HikariPoolMXBean mx = dataSource.getHikariPoolMXBean();
        if (mx != null) {
            inUse = mx.getActiveConnections();
        }
        // Do NOT include e.getMessage(): HikariCP echoes the pool name (sanitized) but the driver cause can carry the
        // raw URL/credentials. Keep the sanitized cause for SQLState/vendor diagnostics; the summary is clean.
        return new IllegalStateException(
            "no JDBC connection available within "
                + config.poolConnectionTimeoutMs()
                + "ms; target=["
                + JdbcUrlSanitizer.sanitize(jdbcUrl)
                + "] pool_max="
                + poolMax
                + " in_use="
                + inUse,
            JdbcUrlSanitizer.sanitizeException(e)
        );
    }

    /**
     * Normalizes a JDBC URL to an endpoint key: drops the query string, strips {@code user:pass@} userinfo inside the
     * authority, and lower-cases the result. Non-authority URLs (e.g. {@code jdbc:h2:mem:name;OPT=x}) are only
     * lower-cased. Package-private + static for unit testing.
     */
    static String normalizeKey(String url) {
        String u = url;
        int q = u.indexOf('?');
        if (q >= 0) {
            u = u.substring(0, q);
        }
        int slashSlash = u.indexOf("//");
        if (slashSlash >= 0) {
            int authorityStart = slashSlash + 2;
            int at = u.indexOf('@', authorityStart);
            if (at >= 0) {
                int slash = u.indexOf('/', authorityStart);
                // Only treat text before '@' as userinfo when the '@' is inside the authority (before the first
                // path '/'). Guards against an '@' appearing later in a path/property segment.
                if (slash < 0 || at < slash) {
                    u = u.substring(0, authorityStart) + u.substring(at + 1);
                }
            }
        }
        return u.toLowerCase(Locale.ROOT);
    }

    /**
     * Computes the pool key for {@code (jdbcUrl, props)}: the {@link #normalizeKey(String) normalized endpoint}, a
     * {@code ':'}, a {@link #credentialFingerprint(Properties) credential fingerprint}, another {@code ':'}, and a
     * {@link #connectionPropertiesFingerprint(Properties) connection-properties fingerprint}. Distinct credentials on
     * the same endpoint therefore yield distinct keys (per-query credential isolation), and distinct non-secret
     * {@code connection_properties} (e.g. a different {@code sslmode}/{@code ApplicationName}) ALSO yield distinct keys
     * — two datasets that configure physically different connections must not share a pool. Package-private + static
     * for unit testing. The returned key contains only the sanitized-endpoint text and one-way hashes, so it is safe to
     * compare in tests but must still never be logged.
     */
    static String poolKey(String jdbcUrl, Properties props) {
        return normalizeKey(jdbcUrl) + ":" + credentialFingerprint(props) + ":" + connectionPropertiesFingerprint(props);
    }

    /**
     * A non-reversible SHA-256 fingerprint of the {@code user}/{@code password} in {@code props}, hex-encoded. A NUL
     * separator between the two fields makes {@code (user="ab", password="c")} and {@code (user="a", password="bc")}
     * hash differently. A null/absent field is treated as the empty string, so credential-less borrows share one
     * stable fingerprint. This value is used only as an in-memory map key: it is never logged and never leaves the
     * pool (it is not part of pool names, thread names, or exception messages).
     */
    private static String credentialFingerprint(Properties props) {
        String user = props == null ? null : props.getProperty("user");
        String password = props == null ? null : props.getProperty("password");
        MessageDigest md = MessageDigests.sha256();
        md.update((user == null ? "" : user).getBytes(StandardCharsets.UTF_8));
        md.update((byte) 0);
        md.update((password == null ? "" : password).getBytes(StandardCharsets.UTF_8));
        return MessageDigests.toHexString(md.digest());
    }

    /**
     * A SHA-256 fingerprint of every property EXCEPT {@code user}/{@code password} (which the
     * {@link #credentialFingerprint(Properties) credential fingerprint} already covers), hex-encoded, computed over the
     * entries sorted by key so it is independent of {@link Properties} iteration order. A NUL separates key from value
     * and one pair from the next so {@code (a=b, c=d)} and {@code (a=bc, =d)} cannot collide. Empty/absent properties
     * hash to one stable value, so credential-only borrows (the common case, with no extra connection properties) share
     * a single fingerprint and their pool keys are unchanged.
     * <p>
     * <b>Secrets included in this digest.</b> Most of the hashed props are non-secret by the
     * {@code connection_properties} allowlist, but the typed Redshift IAM AWS credentials
     * ({@code AccessKeyID}/{@code SecretAccessKey}/{@code SessionToken}) are written into {@code props} before
     * {@link #poolKey} runs and are therefore folded into this SHA-256 too. That is deliberate and required for
     * correctness: it extends per-query credential isolation to explicit AWS creds, so two callers on the same endpoint
     * with different AWS credentials get SEPARATE pools (never a first-caller-wins identity swap), exactly as
     * {@code user}/{@code password} do via the credential fingerprint. Folding secrets into a SHA-256 does not leak
     * them — it is a one-way digest used only as an in-memory map key: it is NEVER logged and NEVER leaves the pool
     * (pool NAMES carry only the {@link JdbcUrlSanitizer sanitized} URL, see {@link #createDataSource}).
     */
    private static String connectionPropertiesFingerprint(Properties props) {
        MessageDigest md = MessageDigests.sha256();
        if (props != null) {
            java.util.List<String> names = new java.util.ArrayList<>();
            for (String name : props.stringPropertyNames()) {
                if (name.equals("user") || name.equals("password")) {
                    continue;
                }
                names.add(name);
            }
            java.util.Collections.sort(names);
            for (String name : names) {
                md.update(name.getBytes(StandardCharsets.UTF_8));
                md.update((byte) 0);
                md.update(props.getProperty(name).getBytes(StandardCharsets.UTF_8));
                md.update((byte) 0);
            }
        }
        return MessageDigests.toHexString(md.digest());
    }

    /** Number of live pools (one per distinct endpoint+credential-fingerprint). Test-only. */
    int poolCount() {
        return pools.size();
    }

    /** The {@link HikariDataSource} for {@code jdbcUrl}'s endpoint with no credentials, or {@code null}. Test-only. */
    HikariDataSource poolFor(String jdbcUrl) {
        return poolFor(jdbcUrl, new Properties());
    }

    /**
     * The {@link HikariDataSource} for the {@code (jdbcUrl, props)} pool key, or {@code null} if none was created.
     * Test-only; mirrors the exact key {@link #getConnection} would use so tests can assert per-credential segregation.
     */
    HikariDataSource poolFor(String jdbcUrl, Properties props) {
        return pools.get(poolKey(jdbcUrl, props));
    }

    /** Whether {@link #close()} has run. Test-only. */
    boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
        // Snapshot + clear so a concurrent getConnection's computeIfAbsent cannot resurrect a closed entry that we
        // then skip closing (the getConnection path re-checks `closed` after computeIfAbsent and evicts its own).
        for (Map.Entry<String, HikariDataSource> entry : pools.entrySet()) {
            if (pools.remove(entry.getKey(), entry.getValue())) {
                closeQuietly(entry.getValue());
            }
        }
    }

    private static void closeQuietly(HikariDataSource dataSource) {
        try {
            dataSource.close();
        } catch (RuntimeException e) {
            logger.warn("failed to close JDBC connection pool [{}]", dataSource.getPoolName(), e);
        }
    }

    private static Properties copyOf(Properties props) {
        Properties copy = new Properties();
        if (props != null) {
            copy.putAll(props);
        }
        return copy;
    }

    /**
     * {@link DataSource} adapter that makes HikariCP open physical connections through the plugin's
     * {@link JdbcDriverRegistry} (isolated child classloader) instead of {@link java.sql.DriverManager}. The captured
     * {@link Properties} carry the credentials for this pool's {@code (endpoint, credential-fingerprint)} key; because
     * the fingerprint is part of the pool key, these are exactly the borrowing query's own credentials (no
     * first-caller-wins). They live for the pool's lifetime (inherent to pooling) and are never logged.
     */
    private static final class DriverRegistryDataSource implements DataSource {

        private final JdbcDriverRegistry registry;
        private final String jdbcUrl;
        private final Properties props;
        private volatile PrintWriter logWriter;
        private volatile int loginTimeoutSeconds;

        DriverRegistryDataSource(JdbcDriverRegistry registry, String jdbcUrl, Properties props) {
            this.registry = registry;
            this.jdbcUrl = jdbcUrl;
            this.props = props;
        }

        @Override
        public Connection getConnection() throws SQLException {
            return registry.connect(jdbcUrl, props);
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            // HikariCP does not call this path (no username set on HikariConfig), but honor it defensively so a
            // caller-supplied credential overrides the captured one for this borrow.
            Properties p = copyOf(props);
            if (username != null) {
                p.setProperty("user", username);
            }
            if (password != null) {
                p.setProperty("password", password);
            }
            return registry.connect(jdbcUrl, p);
        }

        @Override
        public PrintWriter getLogWriter() {
            return logWriter;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
            this.logWriter = out;
        }

        @Override
        public void setLoginTimeout(int seconds) {
            this.loginTimeoutSeconds = seconds;
        }

        @Override
        public int getLoginTimeout() {
            return loginTimeoutSeconds;
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException("java.util.logging is not used by the JDBC driver registry data source");
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) {
                return iface.cast(this);
            }
            throw new SQLException("not a wrapper for [" + iface.getName() + "]");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return iface.isInstance(this);
        }
    }
}
