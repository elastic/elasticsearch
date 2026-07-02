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
        assertEquals(512, (int) ExternalSourceSettings.MAX_CONNECTIONS.get(settings));
        assertEquals(30, (int) ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(settings));
    }

    public void testCustomValues() {
        Settings settings = Settings.builder()
            .put("esql.external.max_connections", 100)
            .put("esql.external.throttle_max_retry_duration", 60)
            .build();

        assertEquals(100, (int) ExternalSourceSettings.MAX_CONNECTIONS.get(settings));
        assertEquals(60, (int) ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(settings));
    }

    public void testConcurrencyLowerBound() {
        // The minimum is 1: the connection bound also sizes thread/SDK pools, which cannot be zero-width.
        expectThrows(IllegalArgumentException.class, () -> {
            Settings settings = Settings.builder().put("esql.external.max_connections", 0).build();
            ExternalSourceSettings.MAX_CONNECTIONS.get(settings);
        });
    }

    public void testConcurrencyUpperBound() {
        expectThrows(IllegalArgumentException.class, () -> {
            Settings settings = Settings.builder().put("esql.external.max_connections", 4097).build();
            ExternalSourceSettings.MAX_CONNECTIONS.get(settings);
        });
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
