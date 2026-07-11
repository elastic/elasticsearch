/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mutable runtime configuration for the JDBC datasource plugin, exposed via cluster settings.
 * <p>
 * Each {@link JdbcDataSourcePlugin} instance owns exactly one instance of this class and updates it as
 * cluster-settings updates arrive. The {@link JdbcConnectorFactory} reads {@link #enabled()} on every
 * {@code canHandle} call so the kill switch takes effect without bouncing the node.
 * <p>
 * <b>Why a separate class.</b> The {@code DataSourcePlugin} SPI extension that {@code DataSourceModule} queries is a
 * distinct object from the {@link org.elasticsearch.plugins.Plugin}-managed instance, and the single-public-constructor
 * rule for managed plugins rules out sharing state through an extension constructor. Instead the SPI instance owns
 * its <em>own</em> config, seeded once from node {@link Settings} on the first {@code connectors(Settings)} call --
 * no class-level static bridge. Keeping the mutable state in its own thread-safe class keeps that publication
 * discipline obvious and the factory ctor trivial to test (no Plugin, no ClusterService).
 * <p>
 * <b>Defaults match production-safe behaviour.</b> {@link #enabled()} defaults {@code true} (the plugin loaded
 * because someone configured it; refusing every query at startup would be surprising). SSRF defaults to the
 * {@link SsrfGuard#defaultGuard() default guard}: production subprotocol allowlist, loopback denied.
 * <p>
 * <b>Pool settings apply at pool creation.</b> The kill switch and SSRF settings take effect immediately (read on
 * every {@code canHandle}/borrow). The pool sizing/timeout knobs ({@code esql.jdbc.pool.*}: {@code max_per_url},
 * {@code connection_timeout_ms}, {@code idle_timeout_ms}, {@code max_lifetime_ms}, {@code keepalive_ms},
 * {@code validation_timeout_ms}) are instead read by {@link JdbcHikariPool} only when an endpoint's pool is CREATED;
 * a dynamic update to them applies on the next (re)creation of that endpoint's pool, not to live pools. The
 * setters below simply update the current value the next pool creation will read.
 */
public final class JdbcRuntimeConfig {

    /**
     * Master kill switch for the JDBC connector. When {@code false}, {@link JdbcConnectorFactory#canHandle(String)}
     * returns {@code false} for every URL, so the framework falls through and the query fails with the standard
     * "no source for jdbc:..." error. Dynamic so on-call can flip it without a node restart.
     */
    public static final Setting<Boolean> ENABLED = Setting.boolSetting(
        "esql.jdbc.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Comma-separated allowlist of JDBC subprotocol prefixes. Empty / missing means the
     * {@link SsrfGuard#DEFAULT_ALLOWED_SUBPROTOCOLS production default}; otherwise we use exactly what's set (so
     * an operator who flips this on can also remove {@code jdbc:h2:mem:} for hardening). Dynamic so an operator can
     * tighten the allowlist in response to an incident without bouncing nodes.
     */
    public static final Setting<List<String>> ALLOWED_SUBPROTOCOLS = Setting.listSetting(
        "esql.jdbc.ssrf.allowed_subprotocols",
        List.of(),
        s -> s,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Allow JDBC URLs whose host parses to a loopback address (127.0.0.0/8, ::1, localhost). Default {@code false}
     * because the legitimate target audience -- production DBs -- never lives on loopback. Tests that exercise a
     * TCP-mode H2 on localhost need to flip this to {@code true} (or use {@code jdbc:h2:mem:} which has no host
     * and so is unaffected).
     */
    public static final Setting<Boolean> ALLOW_LOOPBACK = Setting.boolSetting(
        "esql.jdbc.ssrf.allow_loopback",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Maximum pooled connections per JDBC endpoint (normalized URL). Default 10. Because JDBC producers run on
     * {@code esql_worker} threads, a single endpoint can never borrow more than the esql_worker pool size at once;
     * {@link #warnIfPoolOvercommit} logs a WARN when this exceeds that size (wasted DB connection budget).
     */
    public static final Setting<Integer> POOL_MAX_PER_URL = Setting.intSetting(
        "esql.jdbc.pool.max_per_url",
        10,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * HikariCP {@code connectionTimeout}. Default 5000ms (short by design): a producer that cannot obtain a
     * connection fails fast rather than parking an esql_worker thread indefinitely. HikariCP's floor is 250ms.
     */
    public static final Setting<Long> POOL_CONNECTION_TIMEOUT_MS = Setting.longSetting(
        "esql.jdbc.pool.connection_timeout_ms",
        5000L,
        250L,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** HikariCP {@code idleTimeout}. Default 30000ms; idle connections beyond {@code minimumIdle}=0 are retired. */
    public static final Setting<Long> POOL_IDLE_TIMEOUT_MS = Setting.longSetting(
        "esql.jdbc.pool.idle_timeout_ms",
        30000L,
        0L,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** HikariCP {@code maxLifetime}. Default 900000ms (15m); a physical connection is retired after this age. */
    public static final Setting<Long> POOL_MAX_LIFETIME_MS = Setting.longSetting(
        "esql.jdbc.pool.max_lifetime_ms",
        900000L,
        0L,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * HikariCP {@code keepaliveTime}. Default {@code 0} = <b>disabled</b>. When set to a positive value
     * it makes HikariCP periodically run a lightweight JDBC4 {@code isValid()} keepalive against <em>idle</em> pooled
     * connections, so a connection that has silently rotted behind a NAT/firewall/DB idle-timeout is detected and
     * retired <em>before</em> it is ever handed to a query — the proactive, pool-integrated "stale connection" defense
     * that replaces a per-query {@code Connection.isValid(5)} probe.
     * <p>
     * <b>Ordering invariant.</b> When enabled, HikariCP requires {@code keepaliveTime >= 30000ms} and
     * {@code keepaliveTime < maxLifetime}; it is also only useful when {@code keepaliveTime < idleTimeout} (otherwise an
     * idle connection is retired before the keepalive can ever fire). The full invariant is therefore
     * {@code 30000 <= keepalive < idle_timeout < max_lifetime}. {@link JdbcHikariPool} validates this at pool-creation
     * time and <em>disables</em> a mis-ordered keepalive with an actionable WARN rather than letting HikariCP reset it
     * silently. Default {@code 0} trivially satisfies the invariant (disabled), so no operator action is required.
     */
    public static final Setting<Long> POOL_KEEPALIVE_MS = Setting.longSetting(
        "esql.jdbc.pool.keepalive_ms",
        0L,
        0L,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * HikariCP {@code validationTimeout}. Default 5000ms. This is the budget for the JDBC4
     * {@code isValid()} check HikariCP already runs <b>on every borrow</b> (governed by HikariCP's
     * {@code aliveBypassWindowMs} performance window) — i.e. the on-borrow half of the "stale connection" defense that
     * makes a manual pre-query {@code isValid(5)} redundant. HikariCP's floor is 250ms.
     * <p>
     * <b>Ordering invariant.</b> {@code validationTimeout <= connectionTimeout}: the per-borrow validation budget must
     * never exceed the overall connection-acquisition budget. {@link JdbcHikariPool} clamps a larger value down to
     * {@code connectionTimeout} with a WARN. The default (5000ms) equals the default {@code connectionTimeout}
     * (5000ms), so the invariant holds out of the box.
     */
    public static final Setting<Long> POOL_VALIDATION_TIMEOUT_MS = Setting.longSetting(
        "esql.jdbc.pool.validation_timeout_ms",
        5000L,
        250L,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(JdbcRuntimeConfig.class);

    private final AtomicBoolean enabled;
    private final AtomicReference<SsrfGuard> guard;
    private final AtomicInteger poolMaxPerUrl;
    private final AtomicLong poolConnectionTimeoutMs;
    private final AtomicLong poolIdleTimeoutMs;
    private final AtomicLong poolMaxLifetimeMs;
    private final AtomicLong poolKeepaliveMs;
    private final AtomicLong poolValidationTimeoutMs;

    /**
     * Monotonic credential epoch. Bumped by {@link JdbcDataSourcePlugin#reload} (the
     * {@link org.elasticsearch.plugins.ReloadablePlugin} hook) so a connector can detect that a credential reload
     * happened between building its connection {@link java.util.Properties} and an {@code AUTH_FAILED} response, and
     * retry once with re-resolved credentials. It is an ordinary in-memory counter, not a cluster setting, and lives
     * on this instance-owned config (never a static). Credentials are currently per-query and not
     * refreshable, so the epoch is a forward-looking seam for a future node-keystore-backed credential source.
     */
    private final AtomicLong credentialEpoch = new AtomicLong(0L);

    /**
     * The {@code esql_worker} pool size and {@code esql.external.max_concurrent_requests}, snapshotted at
     * {@link #initialize} so a dynamic {@code max_per_url} update can re-run the overcommit WARN without re-reading
     * node {@link Settings}. {@code -1} until initialized (WARN suppressed before seeding).
     */
    private volatile int esqlWorkerPoolSize = -1;
    private volatile int maxConcurrentRequestsSnapshot = -1;

    public JdbcRuntimeConfig() {
        this.enabled = new AtomicBoolean(true);
        this.guard = new AtomicReference<>(SsrfGuard.defaultGuard());
        this.poolMaxPerUrl = new AtomicInteger(POOL_MAX_PER_URL.getDefault(Settings.EMPTY));
        this.poolConnectionTimeoutMs = new AtomicLong(POOL_CONNECTION_TIMEOUT_MS.getDefault(Settings.EMPTY));
        this.poolIdleTimeoutMs = new AtomicLong(POOL_IDLE_TIMEOUT_MS.getDefault(Settings.EMPTY));
        this.poolMaxLifetimeMs = new AtomicLong(POOL_MAX_LIFETIME_MS.getDefault(Settings.EMPTY));
        this.poolKeepaliveMs = new AtomicLong(POOL_KEEPALIVE_MS.getDefault(Settings.EMPTY));
        this.poolValidationTimeoutMs = new AtomicLong(POOL_VALIDATION_TIMEOUT_MS.getDefault(Settings.EMPTY));
    }

    /**
     * Seeds the runtime config from node-level {@link Settings}. Called once per instance -- from the first
     * {@code connectors(Settings)} call. The current {@code DataSourcePlugin} SPI has no dynamic cluster-settings
     * hook, so these values are construction-time.
     */
    public void initialize(Settings nodeSettings) {
        if (nodeSettings == null) {
            throw new IllegalArgumentException("nodeSettings must not be null");
        }
        this.enabled.set(ENABLED.get(nodeSettings));
        boolean allowLoopback = ALLOW_LOOPBACK.get(nodeSettings);
        List<String> subs = ALLOWED_SUBPROTOCOLS.get(nodeSettings);
        this.guard.set(buildGuard(subs, allowLoopback));
        this.poolMaxPerUrl.set(POOL_MAX_PER_URL.get(nodeSettings));
        this.poolConnectionTimeoutMs.set(POOL_CONNECTION_TIMEOUT_MS.get(nodeSettings));
        this.poolIdleTimeoutMs.set(POOL_IDLE_TIMEOUT_MS.get(nodeSettings));
        this.poolMaxLifetimeMs.set(POOL_MAX_LIFETIME_MS.get(nodeSettings));
        this.poolKeepaliveMs.set(POOL_KEEPALIVE_MS.get(nodeSettings));
        this.poolValidationTimeoutMs.set(POOL_VALIDATION_TIMEOUT_MS.get(nodeSettings));
        // Snapshot the REAL concurrency knobs (esql_worker pool size + external max_concurrent_requests) so the
        // overcommit WARN reflects this node's configuration. These are the knobs that actually bound JDBC producer
        // concurrency (there is no dedicated external-I/O thread pool).
        int configuredWorker = EsqlPlugin.ESQL_WORKER_THREAD_POOL_SIZE.get(nodeSettings);
        this.esqlWorkerPoolSize = configuredWorker > 0
            ? configuredWorker
            : ThreadPool.searchOrGetThreadPoolSize(EsExecutors.allocatedProcessors(nodeSettings));
        this.maxConcurrentRequestsSnapshot = ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(nodeSettings);
        warnIfPoolOvercommit();
    }

    /** Returns the current kill-switch state. Reads are lock-free; cheap on the {@code canHandle} hot path. */
    public boolean enabled() {
        return enabled.get();
    }

    /** Returns the current SSRF guard. The reference is replaced atomically when either SSRF setting updates. */
    public SsrfGuard guard() {
        return guard.get();
    }

    /** Current per-endpoint max pool size (HikariCP {@code maximumPoolSize}). Read when a per-URL pool is created. */
    public int poolMaxPerUrl() {
        return poolMaxPerUrl.get();
    }

    /** Current HikariCP {@code connectionTimeout} in ms. */
    public long poolConnectionTimeoutMs() {
        return poolConnectionTimeoutMs.get();
    }

    /** Current HikariCP {@code idleTimeout} in ms. */
    public long poolIdleTimeoutMs() {
        return poolIdleTimeoutMs.get();
    }

    /** Current HikariCP {@code maxLifetime} in ms. */
    public long poolMaxLifetimeMs() {
        return poolMaxLifetimeMs.get();
    }

    /** Current HikariCP {@code keepaliveTime} in ms ({@code 0} = disabled). Validated against the ordering invariant
     * by {@link JdbcHikariPool} when a pool is created. */
    public long poolKeepaliveMs() {
        return poolKeepaliveMs.get();
    }

    /** Current HikariCP {@code validationTimeout} in ms (the on-borrow {@code isValid()} budget). */
    public long poolValidationTimeoutMs() {
        return poolValidationTimeoutMs.get();
    }

    /**
     * Current credential epoch. Read by {@link JdbcConnector} before building credential {@link java.util.Properties}
     * (recording the epoch its credentials belong to) and again on an {@code AUTH_FAILED}; a change between the two
     * signals that a reload delivered a potentially-fresh credential generation. Lock-free.
     */
    public long credentialEpoch() {
        return credentialEpoch.get();
    }

    /**
     * Bumps the credential epoch and returns the new value. Invoked from {@link JdbcDataSourcePlugin#reload} on the
     * node-keystore reload hook. Idempotency is not required: each reload is a distinct generation.
     */
    public long bumpCredentialEpoch() {
        return credentialEpoch.incrementAndGet();
    }

    // -- Cluster-settings update consumers; one per setting to mirror the framework's per-setting subscription API.

    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    public void setAllowedSubprotocols(List<String> subs) {
        // updateAndGet so a concurrent setAllowLoopback that interleaves with this call cannot lose the other
        // half of the guard config. Cluster-settings updates are normally serialized on a single thread, so the
        // race is theoretical -- but updateAndGet is a one-line preventive against a future change to that
        // assumption (e.g. parallel apply of multiple settings in one update).
        guard.updateAndGet(curr -> buildGuard(subs, curr.allowLoopback()));
    }

    public void setAllowLoopback(boolean allowLoopback) {
        // Symmetric to setAllowedSubprotocols; the snapshot read of allowedSubprotocols() and the rebuild happen
        // atomically inside updateAndGet.
        guard.updateAndGet(curr -> buildGuard(new ArrayList<>(curr.allowedSubprotocols()), allowLoopback));
    }

    public void setPoolMaxPerUrl(int maxPerUrl) {
        this.poolMaxPerUrl.set(maxPerUrl);
        // Re-run the overcommit WARN so a dynamic bump past the esql_worker pool size is surfaced immediately.
        warnIfPoolOvercommit();
    }

    public void setPoolConnectionTimeoutMs(long connectionTimeoutMs) {
        this.poolConnectionTimeoutMs.set(connectionTimeoutMs);
    }

    public void setPoolIdleTimeoutMs(long idleTimeoutMs) {
        this.poolIdleTimeoutMs.set(idleTimeoutMs);
    }

    public void setPoolMaxLifetimeMs(long maxLifetimeMs) {
        this.poolMaxLifetimeMs.set(maxLifetimeMs);
    }

    public void setPoolKeepaliveMs(long keepaliveMs) {
        this.poolKeepaliveMs.set(keepaliveMs);
    }

    public void setPoolValidationTimeoutMs(long validationTimeoutMs) {
        this.poolValidationTimeoutMs.set(validationTimeoutMs);
    }

    /**
     * Logs a WARN when {@code max_per_url} exceeds the {@code esql_worker} pool size. Because JDBC producers run on
     * esql_worker threads, at most that many threads can hold connections to one endpoint at once, so a larger
     * {@code max_per_url} provisions DB connections that can never all be in use — a single-URL overcommit and a
     * likely misconfiguration. Observational only; never fails startup. {@code esql.external.max_concurrent_requests}
     * is included for operator context but explicitly does NOT gate JDBC. Suppressed until
     * {@link #initialize} has snapshotted the worker pool size.
     */
    private void warnIfPoolOvercommit() {
        int workerPoolSize = esqlWorkerPoolSize;
        if (workerPoolSize <= 0) {
            return;
        }
        int maxPerUrl = poolMaxPerUrl.get();
        if (maxPerUrl > workerPoolSize) {
            logger.warn(
                "esql.jdbc.pool.max_per_url [{}] exceeds the esql_worker pool size [{}]: a single JDBC endpoint can "
                    + "never borrow more than [{}] connections at once (one per esql_worker thread), so the surplus "
                    + "provisions database connections that can never all be in use. Consider lowering max_per_url to "
                    + "<= [{}]. (Note: esql.external.max_concurrent_requests [{}] does NOT throttle JDBC producers.)",
                maxPerUrl,
                workerPoolSize,
                workerPoolSize,
                workerPoolSize,
                maxConcurrentRequestsSnapshot
            );
        }
    }

    private static SsrfGuard buildGuard(List<String> subs, boolean allowLoopback) {
        if (subs == null || subs.isEmpty()) {
            return new SsrfGuard(SsrfGuard.DEFAULT_ALLOWED_SUBPROTOCOLS, allowLoopback);
        }
        return new SsrfGuard(subs, allowLoopback);
    }

    /** Returns the {@link Setting}s this config owns; the plugin includes them in {@code getSettings()}. */
    public static List<Setting<?>> settings() {
        return List.of(
            ENABLED,
            ALLOWED_SUBPROTOCOLS,
            ALLOW_LOOPBACK,
            POOL_MAX_PER_URL,
            POOL_CONNECTION_TIMEOUT_MS,
            POOL_IDLE_TIMEOUT_MS,
            POOL_MAX_LIFETIME_MS,
            POOL_KEEPALIVE_MS,
            POOL_VALIDATION_TIMEOUT_MS
        );
    }
}
