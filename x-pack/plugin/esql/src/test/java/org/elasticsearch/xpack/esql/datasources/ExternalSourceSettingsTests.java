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
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExternalSourceSettingsTests extends ESTestCase {

    public void testDefaults() {
        Settings settings = Settings.EMPTY;
        assertEquals(30, (int) ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(settings));
        // The in-flight-read permit bound defaults to the CPU-bound formula, not a fixed literal.
        assertEquals(
            ExternalSourceSettings.defaultBlobStoreConcurrency(settings),
            (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings)
        );
        assertEquals(ExternalSourceSettings.defaultBlobStoreConcurrency(settings), ExternalSourceSettings.blobStoreConcurrency(settings));
    }

    public void testMaxConcurrentRequestsDefaultTracksCpuFormula() {
        int processors = randomIntBetween(1, Math.max(1, Runtime.getRuntime().availableProcessors()));
        Settings settings = Settings.builder().put("node.processors", processors).build();
        // processors * 3 clamped to [16, 100]: the floor keeps small nodes from collapsing the I/O pool.
        int expected = Math.min(Math.max(processors * 3, 16), 100);
        assertEquals(expected, ExternalSourceSettings.defaultBlobStoreConcurrency(settings));
        assertEquals(expected, (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings));
    }

    public void testDefaultBlobStoreConcurrencyClampedToFloorAndCeiling() {
        // Below the floor: a one- or two-processor node (processors * 3 = 3 or 6) still resolves to the 16 floor,
        // so the concurrency bound — and the esql_external_io pool it sizes — never collapses too small to run the
        // parallel-parse pipeline (the multi-file glob stall).
        assertEquals(16, ExternalSourceSettings.defaultBlobStoreConcurrency(1));
        assertEquals(16, ExternalSourceSettings.defaultBlobStoreConcurrency(5)); // 15 -> floored to 16
        // On the floor boundary: processors * 3 == 18 sits above the floor and is returned as-is.
        assertEquals(18, ExternalSourceSettings.defaultBlobStoreConcurrency(6));
        // Above the ceiling: processors * 3 = 300 is capped at 100.
        assertEquals(100, ExternalSourceSettings.defaultBlobStoreConcurrency(100));
    }

    public void testMaxConcurrentRequestsOverrideIsTheEffectiveKnob() {
        int override = randomIntBetween(0, 500);
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", override).build();
        assertEquals(override, (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings));
        assertEquals(override, ExternalSourceSettings.blobStoreConcurrency(settings));
    }

    public void testMaxConcurrentRequestsLowerBoundAllowsZero() {
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", 0).build();
        assertEquals(0, (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings));
    }

    public void testExternalIoThreadsTracksPositiveConcurrency() {
        int override = randomIntBetween(1, 500);
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", override).build();
        assertEquals(override, ExternalSourceSettings.externalIoThreads(settings));
    }

    public void testExternalIoThreadsFallsBackToCpuDefaultWhenLimiterDisabled() {
        // 0 disables the permit limiter but the I/O pool still needs threads: it must not resolve to a zero-thread
        // pool, so externalIoThreads falls back to the CPU-scaled default.
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", 0).build();
        assertEquals(ExternalSourceSettings.defaultBlobStoreConcurrency(settings), ExternalSourceSettings.externalIoThreads(settings));
        assertTrue("external I/O pool must always have at least one thread", ExternalSourceSettings.externalIoThreads(settings) >= 1);
    }

    public void testExternalIoThreadsDefaultsToCpuFormula() {
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
        assertEquals(7, ExternalSourceSettings.settings().size());
        assertTrue(ExternalSourceSettings.settings().contains(ExternalSourceSettings.MAX_CONCURRENT_REQUESTS));
    }

    public void testManagedIdentityDefaultFalse() {
        assertFalse(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(Settings.EMPTY));
    }

    public void testManagedIdentityCanBeEnabled() {
        Settings settings = Settings.builder().put("esql.datasource.managed_identity.enabled", true).build();
        assertTrue(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
    }

    // --- Backwards compatibility: the deprecated workload_identity.enabled key still works via fallback ---

    public void testDeprecatedWorkloadIdentityKeyStillEnablesManagedIdentity() {
        // An operator's pre-rename config keeps working: the new setting falls back to the deprecated key's value,
        // and using the deprecated key emits a deprecation warning.
        Settings settings = Settings.builder().put("esql.datasource.workload_identity.enabled", true).build();
        assertTrue(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED });
    }

    public void testManagedIdentityKeyTakesPrecedenceOverDeprecatedKey() {
        // When the new key is set it wins and the deprecated key is not consulted (so no fallback read here).
        Settings settings = Settings.builder()
            .put("esql.datasource.workload_identity.enabled", false)
            .put("esql.datasource.managed_identity.enabled", true)
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

        clusterSettings.applySettings(Settings.builder().put("esql.datasource.workload_identity.enabled", true).build());
        assertTrue("enabling the deprecated key dynamically must fire the consumer on the new setting", enabled.get());

        clusterSettings.applySettings(Settings.builder().put("esql.datasource.workload_identity.enabled", false).build());
        assertFalse("disabling the deprecated key dynamically must fire the consumer (security-critical)", enabled.get());

        assertSettingDeprecationsAndWarnings(new Setting<?>[] { ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED });
    }

    // --- Stateless gate (mirrors the AtomicBoolean wiring in EsqlPlugin.createComponents) ---

    public void testManagedIdentityDisabledOnStatelessNodeAtStartup() {
        Settings settings = Settings.builder()
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .put("esql.datasource.managed_identity.enabled", true)
            .build();
        boolean isStateless = DiscoveryNode.isStateless(settings);
        AtomicBoolean enabled = new AtomicBoolean(isStateless == false && ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings));
        assertFalse("managed identity must be off on stateless nodes even when setting is true", enabled.get());
    }

    public void testManagedIdentityEnabledOnNonStatelessNode() {
        Settings settings = Settings.builder().put("esql.datasource.managed_identity.enabled", true).build();
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
        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", "/data/allowed", "/mnt/shared").build();
        List<String> paths = ExternalSourceSettings.LOCAL_ALLOWED_PATHS.get(settings);
        assertEquals(2, paths.size());
        assertEquals("/data/allowed", paths.get(0));
        assertEquals("/mnt/shared", paths.get(1));
    }

    public void testLocalAllowedPathsEnabledWhenSet() {
        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", "/data/allowed").build();
        LocalFileAccess access = LocalFileAccess.create(settings);
        assertTrue("local disk access must be enabled when allowlist is set", access.enabled());
    }
}
