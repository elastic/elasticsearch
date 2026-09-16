/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExternalSourceSettingsTests extends ESTestCase {

    public void testDefaults() {
        Settings settings = Settings.EMPTY;
        assertEquals(30, (int) ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(settings));
        // The in-flight-read permit bound defaults to the heap- and CPU-scaled formula, not a fixed literal.
        assertEquals(
            ExternalSourceSettings.defaultBlobStoreConcurrency(settings),
            (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings)
        );
        assertEquals(ExternalSourceSettings.defaultBlobStoreConcurrency(settings), ExternalSourceSettings.blobStoreConcurrency(settings));
    }

    public void testMaxConcurrentRequestsDefaultTracksFormula() {
        int processors = randomIntBetween(1, Math.max(1, Runtime.getRuntime().availableProcessors()));
        Settings settings = Settings.builder().put("node.processors", processors).build();
        long heapBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        long requestLimit = HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes();
        int expected = ExternalSourceSettings.defaultBlobStoreConcurrency(processors, heapBytes, requestLimit);
        assertEquals(expected, ExternalSourceSettings.defaultBlobStoreConcurrency(settings));
        assertEquals(expected, (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings));
    }

    public void testDefaultBlobStoreConcurrencyMatrix() {
        // B = 10 MiB, M = min(heap/4, REQUEST/2), C = min(cpuClamp, max(4, M/B)).
        // Default REQUEST is 60% of heap, so REQUEST/2 is 30% and heap/4 binds.
        assertEquals(4, concurrency(1, ByteSizeValue.ofMb(256)));
        assertEquals(6, concurrency(2, ByteSizeValue.ofMb(256)));
        assertEquals(6, concurrency(8, ByteSizeValue.ofMb(256)));
        assertEquals(6, concurrency(16, ByteSizeValue.ofMb(256)));

        assertEquals(4, concurrency(1, ByteSizeValue.ofMb(512)));
        assertEquals(6, concurrency(2, ByteSizeValue.ofMb(512)));
        assertEquals(12, concurrency(8, ByteSizeValue.ofMb(512)));
        assertEquals(12, concurrency(16, ByteSizeValue.ofMb(512)));

        assertEquals(4, concurrency(1, ByteSizeValue.ofGb(4)));
        assertEquals(6, concurrency(2, ByteSizeValue.ofGb(4)));
        assertEquals(24, concurrency(8, ByteSizeValue.ofGb(4)));
        assertEquals(48, concurrency(16, ByteSizeValue.ofGb(4)));
    }

    public void testDefaultBlobStoreConcurrencyCpuClampOnRichHeap() {
        // Memory does not bind: C follows processors * 3 in [4, 100].
        ByteSizeValue richHeap = ByteSizeValue.ofGb(32);
        assertEquals(4, concurrency(1, richHeap));
        assertEquals(15, concurrency(5, richHeap));
        assertEquals(18, concurrency(6, richHeap));
        assertEquals(100, concurrency(100, richHeap));
    }

    public void testDefaultBlobStoreConcurrencyParseFloorBeatsTinyMemory() {
        // 80 MiB heap: M = 20 MiB, floor(M / 10 MiB) = 2, but gzip parse needs C >= 4.
        assertEquals(4, concurrency(1, ByteSizeValue.ofMb(80)));
    }

    public void testDefaultBlobStoreConcurrencyRequestBreakerBindsFirst() {
        long heapBytes = ByteSizeValue.ofGb(4).getBytes();
        long tightRequest = ByteSizeValue.ofMb(80).getBytes();
        // M = min(1024 MiB, 40 MiB) = 40 MiB so C = 4, even though 16 CPUs would otherwise allow 48.
        assertEquals(4, ExternalSourceSettings.defaultBlobStoreConcurrency(16, heapBytes, tightRequest));
    }

    public void testPositiveOverrideIsClampedByMemoryTerm() {
        // Leftover 16 (the old floor) must not skip M. Default REQUEST is 60% of heap, so heap/4 binds.
        assertEquals(12, ExternalSourceSettings.blobStoreConcurrency(16, ByteSizeValue.ofMb(512).getBytes(), requestLimit(512)));
        assertEquals(6, ExternalSourceSettings.blobStoreConcurrency(16, ByteSizeValue.ofMb(256).getBytes(), requestLimit(256)));
    }

    public void testPositiveOverrideCanLowerBelowMemoryCap() {
        assertEquals(2, ExternalSourceSettings.blobStoreConcurrency(2, ByteSizeValue.ofMb(512).getBytes(), requestLimit(512)));
    }

    public void testZeroOverrideSkipsMemoryCap() {
        assertEquals(0, ExternalSourceSettings.blobStoreConcurrency(0, ByteSizeValue.ofMb(256).getBytes(), requestLimit(256)));
    }

    public void testPositiveOverrideHonorsParseFloorOnTinyHeap() {
        // 80 MiB heap: M = 20 MiB, floor(M / 10 MiB) = 2, leftover 16 still gets the gzip parse floor of 4.
        assertEquals(4, ExternalSourceSettings.blobStoreConcurrency(16, ByteSizeValue.ofMb(80).getBytes(), requestLimit(80)));
    }

    public void testPositiveOverrideCanRaiseAboveCpuWhenMemoryAllows() {
        // 1 CPU would default to 4; leftover 16 on 4 GiB is memory-legal (102 slots) so it stays 16.
        assertEquals(16, ExternalSourceSettings.blobStoreConcurrency(16, ByteSizeValue.ofGb(4).getBytes(), requestLimitGb(4)));
    }

    public void testDefaultBlobStoreConcurrencySettingsHonorsRequestLimit() {
        // Wiring: a tightened request breaker must reach the Settings overload. Do not set
        // node.processors above the host's allocatedProcessors (the setting rejects that).
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "80mb")
            .build();
        long heapBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        int processors = EsExecutors.allocatedProcessors(settings);
        assertEquals(
            ExternalSourceSettings.defaultBlobStoreConcurrency(processors, heapBytes, ByteSizeValue.ofMb(80).getBytes()),
            ExternalSourceSettings.defaultBlobStoreConcurrency(settings)
        );
    }

    private static int concurrency(int processors, ByteSizeValue heap) {
        long heapBytes = heap.getBytes();
        long requestLimit = heapBytes * 6 / 10;
        return ExternalSourceSettings.defaultBlobStoreConcurrency(processors, heapBytes, requestLimit);
    }

    private static long requestLimit(int heapMb) {
        return ByteSizeValue.ofMb(heapMb).getBytes() * 6 / 10;
    }

    private static long requestLimitGb(int heapGb) {
        return ByteSizeValue.ofGb(heapGb).getBytes() * 6 / 10;
    }

    private static int effectiveConcurrency(int override, Settings settings) {
        return ExternalSourceSettings.blobStoreConcurrency(
            override,
            JvmInfo.jvmInfo().getMem().getHeapMax().getBytes(),
            HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes()
        );
    }

    public void testMaxConcurrentRequestsOverrideIsTheEffectiveKnob() {
        int override = randomIntBetween(0, 500);
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", override).build();
        assertEquals(override, (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings));
        assertEquals(effectiveConcurrency(override, settings), ExternalSourceSettings.blobStoreConcurrency(settings));
    }

    public void testMaxConcurrentRequestsLowerBoundAllowsZero() {
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", 0).build();
        assertEquals(0, (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings));
        assertEquals(0, ExternalSourceSettings.blobStoreConcurrency(settings));
    }

    public void testExternalIoThreadsTracksPositiveConcurrency() {
        int override = randomIntBetween(1, 500);
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", override).build();
        assertEquals(effectiveConcurrency(override, settings), ExternalSourceSettings.externalIoThreads(settings));
    }

    public void testExternalIoThreadsFallsBackToDefaultWhenLimiterDisabled() {
        // 0 disables the permit limiter but the I/O pool still needs threads: it must not resolve to a zero-thread
        // pool, so externalIoThreads falls back to the heap- and CPU-scaled default.
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", 0).build();
        assertEquals(ExternalSourceSettings.defaultBlobStoreConcurrency(settings), ExternalSourceSettings.externalIoThreads(settings));
        assertTrue("external I/O pool must always have at least one thread", ExternalSourceSettings.externalIoThreads(settings) >= 1);
    }

    public void testExternalIoThreadsDefaultsToFormula() {
        Settings settings = Settings.EMPTY;
        assertEquals(ExternalSourceSettings.defaultBlobStoreConcurrency(settings), ExternalSourceSettings.externalIoThreads(settings));
    }

    public void testMaxConcurrentRequestsRejectsNegativeAndOverMax() {
        expectThrows(
            IllegalArgumentException.class,
            () -> ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(
                Settings.builder().put("esql.external.max_concurrent_requests", -1).build()
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(
                Settings.builder().put("esql.external.max_concurrent_requests", 501).build()
            )
        );
        assertEquals(
            500,
            (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(
                Settings.builder().put("esql.external.max_concurrent_requests", 500).build()
            )
        );
    }

    public void testCustomValues() {
        Settings settings = Settings.builder()
            .put("esql.external.max_concurrent_requests", 100)
            .put("esql.external.throttle_max_retry_duration", 60)
            .build();

        assertEquals(100, (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings));
        assertEquals(60, (int) ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(settings));
    }

    public void testThrottleMaxRetryDurationZeroDisablesBudget() {
        Settings settings = Settings.builder().put("esql.external.throttle_max_retry_duration", 0).build();
        assertEquals(0, (int) ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(settings));
    }

    public void testThrottleMaxRetryDurationUpperBound() {
        expectThrows(IllegalArgumentException.class, () -> {
            Settings settings = Settings.builder().put("esql.external.throttle_max_retry_duration", 301).build();
            ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(settings);
        });
    }

    public void testSettingsListNotEmpty() {
        assertFalse(ExternalSourceSettings.settings().isEmpty());
        assertEquals(13, ExternalSourceSettings.settings().size());
        assertTrue(ExternalSourceSettings.settings().contains(ExternalSourceSettings.MAX_CONCURRENT_REQUESTS));
    }

    public void testMaxConcurrentSegmentatorsDefaultDerivesBelowPoolSize() {
        // Default (0) derives the cap from the pool size (externalIoThreads) and clamps it to poolSize - 1, so a
        // pool thread always remains for the one-shot parser tasks a segmentator depends on.
        Settings settings = Settings.builder().put("node.processors", 4).build();
        int poolSize = ExternalSourceSettings.externalIoThreads(settings);
        assertEquals(poolSize - 1, ExternalSourceSettings.maxConcurrentSegmentators(settings));
        assertTrue("cap must leave a pool thread for parsers", ExternalSourceSettings.maxConcurrentSegmentators(settings) < poolSize);
    }

    public void testMaxConcurrentSegmentatorsExplicitOverride() {
        // A pool large enough that the explicit value is not clamped.
        Settings settings = Settings.builder()
            .put("esql.external.max_concurrent_requests", 64)
            .put("esql.external.max_concurrent_segmenters", 8)
            .build();
        int poolSize = ExternalSourceSettings.externalIoThreads(settings);
        assumeTrue("test heap must allow a pool larger than the explicit segmentator cap", poolSize > 8);
        assertEquals(8, ExternalSourceSettings.maxConcurrentSegmentators(settings));
    }

    public void testMaxConcurrentSegmentatorsClampedBelowPoolSize() {
        // A tiny pool forces the cap down to poolSize - 1 so at least one thread stays free for parser tasks.
        Settings settings = Settings.builder()
            .put("esql.external.max_concurrent_requests", 4)
            .put("esql.external.max_concurrent_segmenters", 100)
            .build();
        assertEquals(3, ExternalSourceSettings.maxConcurrentSegmentators(settings));
    }

    public void testMaxConcurrentSegmentatorsAtLeastOne() {
        // Degenerate single-thread pool: the cap floors at 1 (streaming parallel parsing needs >= 2 threads to be
        // deadlock-free, but the cap must never be zero).
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", 1).build();
        assertEquals(1, ExternalSourceSettings.maxConcurrentSegmentators(settings));
    }

    public void testManagedIdentityDefaultFalse() {
        assertFalse(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(Settings.EMPTY));
    }

    public void testManagedIdentityCanBeEnabled() {
        Settings settings = Settings.builder().put("esql.external.managed_identity.enabled", true).build();
        assertTrue(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
    }

    public void testFederatedIdentityDefaultFalse() {
        assertFalse(ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED.get(Settings.EMPTY));
    }

    public void testFederatedIdentityCanBeEnabled() {
        Settings settings = Settings.builder().put("esql.external.federated_identity.enabled", true).build();
        assertTrue(ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED.get(settings));
    }

    // --- Backwards compatibility: the deprecated workload_identity.enabled key still works via fallback ---

    public void testDeprecatedWorkloadIdentityKeyStillEnablesManagedIdentity() {
        // An operator's pre-rename config keeps working: the new setting falls back to the deprecated key's value,
        // and using the deprecated key emits a deprecation warning.
        Settings settings = Settings.builder().put("esql.external.workload_identity.enabled", true).build();
        assertTrue(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED });
    }

    public void testManagedIdentityKeyTakesPrecedenceOverDeprecatedKey() {
        // When the new key is set it wins and the deprecated key is not consulted (so no fallback read here).
        Settings settings = Settings.builder()
            .put("esql.external.workload_identity.enabled", false)
            .put("esql.external.managed_identity.enabled", true)
            .build();
        assertTrue(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
    }

    public void testDynamicUpdateOfDeprecatedKeyFiresConsumer() {
        // EsqlPlugin gates ambient credentials on a live boolean updated by a ClusterSettings consumer registered on
        // the new setting. An operator flipping the deprecated key at runtime must still fire that consumer — in both
        // directions, including the security-critical disable — because the new setting's raw value resolves the fallback.
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED, ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED)
        );
        AtomicBoolean enabled = new AtomicBoolean(false);
        clusterSettings.addSettingsUpdateConsumer(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED, enabled::set);

        clusterSettings.applySettings(Settings.builder().put("esql.external.workload_identity.enabled", true).build());
        assertTrue("enabling the deprecated key dynamically must fire the consumer on the new setting", enabled.get());

        clusterSettings.applySettings(Settings.builder().put("esql.external.workload_identity.enabled", false).build());
        assertFalse("disabling the deprecated key dynamically must fire the consumer (security-critical)", enabled.get());

        assertSettingDeprecationsAndWarnings(new Setting<?>[] { ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED });
    }

    // --- Backwards compatibility: the pre-rename esql.datasource.* keys still work via fallback ---

    public void testPreRenameManagedIdentityKeyStillEnablesManagedIdentity() {
        // A 9.5 config (pre esql.external.* unification) keeps working: the new setting resolves through the
        // deprecated pre-rename key, which emits a deprecation warning when set.
        Settings settings = Settings.builder().put("esql.datasource.managed_identity.enabled", true).build();
        assertTrue(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { ExternalSourceSettings.MANAGED_IDENTITY_ENABLED_OLD });
    }

    public void testPreRenameFederatedIdentityKeyStillEnablesFederatedIdentity() {
        Settings settings = Settings.builder().put("esql.datasource.federated_identity.enabled", true).build();
        assertTrue(ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED.get(settings));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED_OLD });
    }

    public void testPreRenameWorkloadIdentityKeyStillEnablesManagedIdentity() {
        // The deepest fallback: the original 9.5 workload_identity spelling still enables managed identity.
        Settings settings = Settings.builder().put("esql.datasource.workload_identity.enabled", true).build();
        assertTrue(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED_OLD });
    }

    public void testNewKeysWinOverPreRenameKeys() {
        Settings settings = Settings.builder()
            .put("esql.datasource.managed_identity.enabled", false)
            .put("esql.external.managed_identity.enabled", true)
            .put("esql.datasource.federated_identity.enabled", false)
            .put("esql.external.federated_identity.enabled", true)
            .putList("esql.datasource.local_allowed_paths", "/data/old")
            .putList("esql.external.local_allowed_paths", "/data/new")
            .build();
        assertTrue(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
        assertTrue(ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED.get(settings));
        // The list setting resolves its fallback through a different Setting.listSetting overload than the booleans,
        // so cover it too.
        assertEquals(List.of("/data/new"), ExternalSourceSettings.LOCAL_ALLOWED_PATHS.get(settings));
        // No deprecation warnings: fallback resolution is lazy, so a pre-rename key that loses to the new key is
        // never read.
    }

    public void testManagedIdentityFallbackPrecedenceChain() {
        // Resolution order: external.managed > datasource.managed > external.workload > datasource.workload.
        // Each step of the chain wins over everything after it.
        Settings settings = Settings.builder()
            .put("esql.datasource.managed_identity.enabled", true)
            .put("esql.external.workload_identity.enabled", false)
            .put("esql.datasource.workload_identity.enabled", false)
            .build();
        assertTrue(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));

        settings = Settings.builder()
            .put("esql.external.workload_identity.enabled", true)
            .put("esql.datasource.workload_identity.enabled", false)
            .build();
        assertTrue(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));

        // Fallback resolution is lazy: each read stops at the first key present, so only that key warns —
        // esql.datasource.workload_identity.enabled is set in both scenarios but never reached.
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { ExternalSourceSettings.MANAGED_IDENTITY_ENABLED_OLD, ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED }
        );
    }

    public void testDynamicUpdateOfPreRenameKeysFiresConsumers() {
        // Serverless operator settings files (the reserved cluster_settings state) still carry the pre-rename keys.
        // They must be accepted as a dynamic update — this is the regression that motivated restoring them — and
        // must fire the consumers registered on the new settings, in both directions (disable is security-critical).
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(
                ExternalSourceSettings.MANAGED_IDENTITY_ENABLED,
                ExternalSourceSettings.MANAGED_IDENTITY_ENABLED_OLD,
                ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED,
                ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED_OLD,
                ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED,
                ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED_OLD
            )
        );
        AtomicBoolean managedEnabled = new AtomicBoolean(false);
        AtomicBoolean federatedEnabled = new AtomicBoolean(false);
        clusterSettings.addSettingsUpdateConsumer(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED, managedEnabled::set);
        clusterSettings.addSettingsUpdateConsumer(ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED, federatedEnabled::set);

        clusterSettings.applySettings(
            Settings.builder()
                .put("esql.datasource.managed_identity.enabled", true)
                .put("esql.datasource.federated_identity.enabled", true)
                .build()
        );
        assertTrue("enabling the pre-rename managed key dynamically must fire the consumer", managedEnabled.get());
        assertTrue("enabling the pre-rename federated key dynamically must fire the consumer", federatedEnabled.get());

        clusterSettings.applySettings(
            Settings.builder()
                .put("esql.datasource.managed_identity.enabled", false)
                .put("esql.datasource.federated_identity.enabled", false)
                .build()
        );
        assertFalse("disabling the pre-rename managed key must fire the consumer (security-critical)", managedEnabled.get());
        assertFalse("disabling the pre-rename federated key must fire the consumer (security-critical)", federatedEnabled.get());

        // The deepest fallback: a dynamic update through the original 9.5 workload_identity spelling must still
        // propagate up the whole chain to the managed-identity consumer, again in both directions.
        clusterSettings.applySettings(Settings.builder().put("esql.datasource.workload_identity.enabled", true).build());
        assertTrue("enabling the pre-rename workload key dynamically must fire the managed consumer", managedEnabled.get());

        clusterSettings.applySettings(Settings.builder().put("esql.datasource.workload_identity.enabled", false).build());
        assertFalse("disabling the pre-rename workload key must fire the managed consumer (security-critical)", managedEnabled.get());

        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] {
                ExternalSourceSettings.MANAGED_IDENTITY_ENABLED_OLD,
                ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED_OLD,
                ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED_OLD }
        );
    }

    public void testPreRenameLocalAllowedPathsKeyStillTakesEffect() {
        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", "/data/allowed").build();
        List<String> paths = ExternalSourceSettings.LOCAL_ALLOWED_PATHS.get(settings);
        assertEquals(List.of("/data/allowed"), paths);
        assertTrue("local disk access must be enabled through the pre-rename key", LocalFileAccess.create(settings).enabled());
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { ExternalSourceSettings.LOCAL_ALLOWED_PATHS_OLD });
    }

    // --- Stateless gate (mirrors the AtomicBoolean wiring in EsqlPlugin.createComponents) ---

    public void testManagedIdentityDisabledOnStatelessNodeAtStartup() {
        Settings settings = Settings.builder()
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .put("esql.external.managed_identity.enabled", true)
            .build();
        boolean isStateless = DiscoveryNode.isStateless(settings);
        AtomicBoolean enabled = new AtomicBoolean(isStateless == false && ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
        assertFalse("managed identity must be off on stateless nodes even when setting is true", enabled.get());
    }

    public void testManagedIdentityEnabledOnNonStatelessNode() {
        Settings settings = Settings.builder().put("esql.external.managed_identity.enabled", true).build();
        boolean isStateless = DiscoveryNode.isStateless(settings);
        AtomicBoolean enabled = new AtomicBoolean(isStateless == false && ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
        assertTrue("managed identity must be on when setting is true and node is not stateless", enabled.get());
    }

    public void testDynamicUpdateBlockedOnStatelessNode() {
        Settings settings = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();
        boolean isStateless = DiscoveryNode.isStateless(settings);
        AtomicBoolean enabled = new AtomicBoolean(isStateless == false && ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
        // Simulate the update consumer firing with v=true (operator enables the setting)
        enabled.set(isStateless == false && true);
        assertFalse("dynamic enable of managed identity must be blocked on stateless nodes", enabled.get());
    }

    public void testDynamicUpdateTakesEffectOnNonStatelessNode() {
        Settings settings = Settings.EMPTY;
        boolean isStateless = DiscoveryNode.isStateless(settings);
        AtomicBoolean enabled = new AtomicBoolean(isStateless == false && ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
        assertFalse(enabled.get());
        enabled.set(isStateless == false && true);
        assertTrue("dynamic enable must take effect on non-stateless nodes", enabled.get());
    }

    // --- LOCAL_ALLOWED_PATHS setting (mirrors the workload-identity block above) ---

    public void testLocalAllowedPathsDefaultEmpty() {
        List<String> paths = ExternalSourceSettings.LOCAL_ALLOWED_PATHS.get(Settings.EMPTY);
        assertTrue("LOCAL_ALLOWED_PATHS must default to empty (file:// disabled by default)", paths.isEmpty());
    }

    public void testLocalAllowedPathsCanBeSet() {
        Settings settings = Settings.builder().putList("esql.external.local_allowed_paths", "/data/allowed", "/mnt/shared").build();
        List<String> paths = ExternalSourceSettings.LOCAL_ALLOWED_PATHS.get(settings);
        assertEquals(2, paths.size());
        assertEquals("/data/allowed", paths.get(0));
        assertEquals("/mnt/shared", paths.get(1));
    }

    public void testLocalAllowedPathsEnabledWhenSet() {
        Settings settings = Settings.builder().putList("esql.external.local_allowed_paths", "/data/allowed").build();
        LocalFileAccess access = LocalFileAccess.create(settings);
        assertTrue("local disk access must be enabled when allowlist is set", access.enabled());
    }
}
