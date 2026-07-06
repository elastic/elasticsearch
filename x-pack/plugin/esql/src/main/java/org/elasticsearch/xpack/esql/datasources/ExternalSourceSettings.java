/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.List;

/**
 * Cluster settings for controlling ESQL external source behavior; all node-scoped. The external-read concurrency
 * bound ({@link #MAX_CONCURRENT_REQUESTS}) and the throttle retry budget are read at node startup — the concurrency
 * bound sizes both the per-scheme permit semaphores and the backend SDK HTTP connection pools when they are built,
 * the retry budget is read when the storage-provider registry initializes — so a change to either takes effect
 * after a node restart.
 * <p>
 * Covers three areas: the external-read concurrency bound (the single {@link #MAX_CONCURRENT_REQUESTS} knob, which
 * sizes the in-flight-read permit semaphore and the SDK connection pools below it); reactive throttle handling for
 * object stores (the retry duration budget — throttling is handled by backoff, not a concurrency cap); and
 * glob/listing safety limits (max discovered files, max brace expansion) to prevent degenerate queries from
 * overwhelming storage backends.
 */
public final class ExternalSourceSettings {

    private ExternalSourceSettings() {}

    /** Blob-store access concurrency per allocated processor — the {@code snapshot_meta} thread pool's slope. */
    static final int BLOB_STORE_CONCURRENCY_PER_PROCESSOR = 3;

    /**
     * Floor for the CPU-derived blob-store access concurrency. Blob-store reads are latency-bound I/O whose threads
     * spend most of their life parked on the network, so even a small node (a handful of allocated processors, or the
     * single-processor shape of small test/CI nodes) must still drive enough in-flight requests to keep a store busy
     * and, crucially, to run the parallel-parse pipeline without starving itself. {@code processors * 3} alone bottoms
     * out at 3 on a one-processor node, which is too few to host the segment parsers plus their coordination; floor it
     * at 16 so the concurrency bound — and the {@code esql_external_io} pool it sizes — never collapses that small.
     */
    static final int BLOB_STORE_CONCURRENCY_FLOOR = 16;

    /**
     * Ceiling for the CPU-derived blob-store access concurrency. Mirrors the {@code snapshot_meta} thread pool's
     * {@code min(processors * 3, 50)} shape but lifts the cap to 100: external metadata discovery and data reads
     * fan out over many small blobs (footers, byte ranges), so they benefit from more in-flight requests than
     * snapshot metadata does, while still bounding the total against a single store's tolerance.
     */
    static final int BLOB_STORE_CONCURRENCY_CEILING = 100;

    /**
     * The default per-node concurrency for accessing an external blob store, derived from the node's allocated
     * processors using the {@code snapshot_meta} thread pool's sizing shape ({@code processors * 3}), clamped to
     * {@code [}{@value #BLOB_STORE_CONCURRENCY_FLOOR}{@code , }{@value #BLOB_STORE_CONCURRENCY_CEILING}{@code ]}. This
     * is the single source of truth for blob-store access concurrency so metadata discovery and data retrieval stay
     * consistent: both are latency-bound I/O against object stores and should scale the same way with node size rather
     * than each picking an ad-hoc constant. The floor keeps small nodes from self-throttling (and from sizing the
     * {@code esql_external_io} pool too small to run the parse pipeline); the ceiling bounds a single store's load.
     */
    public static int defaultBlobStoreConcurrency(int allocatedProcessors) {
        int scaled = allocatedProcessors * BLOB_STORE_CONCURRENCY_PER_PROCESSOR;
        return Math.min(Math.max(scaled, BLOB_STORE_CONCURRENCY_FLOOR), BLOB_STORE_CONCURRENCY_CEILING);
    }

    /** Convenience overload resolving allocated processors from the given settings. */
    public static int defaultBlobStoreConcurrency(Settings settings) {
        return defaultBlobStoreConcurrency(EsExecutors.allocatedProcessors(settings));
    }

    /**
     * The effective per-node blob-store access concurrency that every external access path reads, so one knob
     * governs metadata discovery and data reads alike: the operator's {@link #MAX_CONCURRENT_REQUESTS} value when
     * set, otherwise the CPU-bound {@link #defaultBlobStoreConcurrency(Settings)} default. The data-read path bounds
     * in-flight reads with a per-scheme permit semaphore sized by this value ({@code StorageProviderRegistry}), and
     * the metadata-discovery fan-out ({@code TransportEsqlQueryAction.externalSourceConcurrency()}) uses the same
     * value — so an operator override reaches both paths.
     */
    public static int blobStoreConcurrency(Settings settings) {
        return MAX_CONCURRENT_REQUESTS.get(settings);
    }

    /**
     * Thread count for the dedicated {@code esql_external_io} pool ({@code EsqlPlugin}). Sized to exactly the single
     * concurrency knob {@link #blobStoreConcurrency(Settings)} — no headroom — because every blocking task that lands
     * on this pool is a permit-gated reader (remote schemes) or is bounded by the pool itself ({@code file://}, which
     * is not permit-wrapped), so the pool never needs more than {@code N} threads. The parse pipeline's page consumer
     * runs on {@code esql_worker}, not here (see {@code AsyncExternalSourceOperatorFactory}), so a full pool of readers
     * can never starve its own drain; that separation is what makes {@code pool == permits} safe rather than
     * deadlock-prone. One exception to tracking the knob: {@code max_concurrent_requests=0} disables the <em>permit</em>
     * limiter (unbounded in-flight reads), but the I/O pool still needs threads to run the reads and parse pipeline, so
     * it falls back to the CPU-scaled {@link #defaultBlobStoreConcurrency(Settings)} default rather than a zero-thread
     * pool. Always {@code >= 1}.
     */
    public static int externalIoThreads(Settings settings) {
        int concurrency = blobStoreConcurrency(settings);
        return concurrency > 0 ? concurrency : defaultBlobStoreConcurrency(settings);
    }

    /**
     * The single external-read concurrency knob, per scheme, per node. It sizes both the per-scheme permit semaphore
     * that bounds in-flight data reads ({@code StorageProviderRegistry}) and the backend SDK HTTP connection pools
     * (S3 Netty {@code maxConcurrency}, Azure reactor-netty {@code ConnectionProvider}), and — via
     * {@link #blobStoreConcurrency(Settings)} — the metadata-discovery fan-out. Set to 0 to disable permit-based
     * concurrency limiting entirely.
     * <p>
     * The default is CPU-bound rather than a fixed literal: {@link #defaultBlobStoreConcurrency(Settings)} — the
     * {@code snapshot_meta} sizing shape ({@code allocatedProcessors * 3}) clamped to
     * {@code [}{@value #BLOB_STORE_CONCURRENCY_FLOOR}{@code , }{@value #BLOB_STORE_CONCURRENCY_CEILING}{@code ]}. That
     * scales in-flight reads with node size so a wide fan-out over many small blobs is not self-throttled by a low
     * fixed cap, while the floor keeps small nodes from collapsing to a handful of permits and the ceiling bounds a
     * single store's load. Operators can raise it (up to 500) for high-throughput clusters or lower it when a store
     * throttles.
     * <p>
     * Static ({@link Setting.Property#NodeScope}): the value sizes the per-scheme semaphores and SDK pools when they
     * are built and there is no settings-update consumer to resize a live {@link java.util.concurrent.Semaphore} or
     * connection pool, so a change takes effect after a node restart.
     */
    public static final Setting<Integer> MAX_CONCURRENT_REQUESTS = Setting.intSetting(
        "esql.external.max_concurrent_requests",
        s -> Integer.toString(defaultBlobStoreConcurrency(s)),
        0,
        500,
        Setting.Property.NodeScope
    );

    /**
     * Maximum total time (in seconds) to spend retrying throttled cloud API requests
     * before giving up. Bounds the cumulative retry duration regardless of the retry count,
     * ensuring queries fail cleanly when throttling is persistent rather than blocking
     * until the HTTP request timeout fires.
     * Default: 30 seconds. Set to 0 to disable the duration budget (retry count only).
     */
    public static final Setting<Integer> THROTTLE_MAX_RETRY_DURATION = Setting.intSetting(
        "esql.external.throttle_max_retry_duration",
        30,
        0,
        300,
        Setting.Property.NodeScope
    );

    /**
     * Hard cap on the number of files that glob expansion will collect before aborting.
     * Protects against degenerate globs (e.g. {@code s3://bucket/*}) on large buckets.
     * Default: 10,000 — generous for legitimate use, catches truly degenerate cases.
     */
    public static final Setting<Integer> MAX_DISCOVERED_FILES = Setting.intSetting(
        "esql.external.max_discovered_files",
        10000,
        1,
        1000000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cap on the number of concrete paths generated by brace expansion before falling
     * back to listing. Prevents patterns like {@code {1,2,...,10000}} from generating
     * excessive HeadObject calls.
     * Default: 100 — above this, the system uses normal listing instead of per-key checks.
     */
    public static final Setting<Integer> MAX_GLOB_EXPANSION = Setting.intSetting(
        "esql.external.max_glob_expansion",
        100,
        1,
        10000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Deprecated former name for {@link #MANAGED_IDENTITY_ENABLED}. Still honored for backwards compatibility — it is the
     * fallback source for the new key, so an operator's existing {@code esql.datasource.workload_identity.enabled} config
     * keeps working — and emits a deprecation warning when set. Prefer {@link #MANAGED_IDENTITY_ENABLED}.
     */
    public static final Setting<Boolean> WORKLOAD_IDENTITY_ENABLED = Setting.boolSetting(
        "esql.datasource.workload_identity.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic,
        Setting.Property.DeprecatedWarning
    );

    /**
     * Enables {@code auth=managed_identity} for external data source reads, which resolves credentials from the node's
     * ambient cloud identity (IAM instance profile / IMDS on AWS and Azure, GCE metadata server on GCP)
     * rather than requiring explicit credentials in the query or datasource.
     * <p>
     * Disabled by default. Must be explicitly enabled by an operator on self-hosted, single-cloud,
     * single-tenant deployments where the node's ambient identity is the intended credential.
     * Never enable in serverless or multi-tenant deployments: ambient credentials bypass tenant isolation.
     * <p>
     * This is an operator-dynamic setting: changes take effect immediately without a node restart. When this key is
     * not set, it falls back to the deprecated {@link #WORKLOAD_IDENTITY_ENABLED} key's value, so reads through this
     * setting see an operator's pre-rename configuration.
     */
    public static final Setting<Boolean> MANAGED_IDENTITY_ENABLED = Setting.boolSetting(
        "esql.datasource.managed_identity.enabled",
        WORKLOAD_IDENTITY_ENABLED,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    /**
     * Allowlist of local filesystem root paths from which ES|QL {@code file://} external sources are permitted to read.
     * Mirrors {@code path.repo}: the list <em>is</em> the enable — an empty list (the default) disables local-disk reads
     * entirely. When non-empty, a {@code file://} path is allowed only if it normalizes to a location under one of the
     * listed roots; {@code ..}-escapes and anything outside every root are rejected.
     * <p>
     * This is a node-scope setting; a node restart is required for changes to take effect.
     */
    public static final Setting<List<String>> LOCAL_ALLOWED_PATHS = Setting.stringListSetting(
        "esql.datasource.local_allowed_paths",
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> settings() {
        return List.of(
            MAX_CONCURRENT_REQUESTS,
            THROTTLE_MAX_RETRY_DURATION,
            MAX_DISCOVERED_FILES,
            MAX_GLOB_EXPANSION,
            WORKLOAD_IDENTITY_ENABLED,
            MANAGED_IDENTITY_ENABLED,
            LOCAL_ALLOWED_PATHS
        );
    }
}
