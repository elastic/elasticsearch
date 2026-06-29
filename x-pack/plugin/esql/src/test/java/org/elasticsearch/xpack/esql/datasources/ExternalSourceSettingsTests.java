/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

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
        assertEquals(5, ExternalSourceSettings.settings().size());
    }

    public void testWorkloadIdentityCredentialsDefaultFalse() {
        assertFalse(ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED.get(Settings.EMPTY));
    }

    public void testWorkloadIdentityCredentialsCanBeEnabled() {
        Settings settings = Settings.builder().put("esql.datasource.workload_identity.enabled", true).build();
        assertTrue(ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED.get(settings));
    }

    // --- Stateless gate (mirrors the AtomicBoolean wiring in EsqlPlugin.createComponents) ---

    public void testWorkloadIdentityDisabledOnStatelessNodeAtStartup() {
        Settings settings = Settings.builder()
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .put("esql.datasource.workload_identity.enabled", true)
            .build();
        boolean isStateless = DiscoveryNode.isStateless(settings);
        AtomicBoolean enabled = new AtomicBoolean(isStateless == false && ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED.get(settings));
        assertFalse("workload identity must be off on stateless nodes even when setting is true", enabled.get());
    }

    public void testWorkloadIdentityEnabledOnNonStatelessNode() {
        Settings settings = Settings.builder().put("esql.datasource.workload_identity.enabled", true).build();
        boolean isStateless = DiscoveryNode.isStateless(settings);
        AtomicBoolean enabled = new AtomicBoolean(isStateless == false && ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED.get(settings));
        assertTrue("workload identity must be on when setting is true and node is not stateless", enabled.get());
    }

    public void testDynamicUpdateBlockedOnStatelessNode() {
        Settings settings = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();
        boolean isStateless = DiscoveryNode.isStateless(settings);
        AtomicBoolean enabled = new AtomicBoolean(isStateless == false && ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED.get(settings));
        // Simulate the update consumer firing with v=true (operator enables the setting)
        enabled.set(isStateless == false && true);
        assertFalse("dynamic enable of workload identity must be blocked on stateless nodes", enabled.get());
    }

    public void testDynamicUpdateTakesEffectOnNonStatelessNode() {
        Settings settings = Settings.EMPTY;
        boolean isStateless = DiscoveryNode.isStateless(settings);
        AtomicBoolean enabled = new AtomicBoolean(isStateless == false && ExternalSourceSettings.WORKLOAD_IDENTITY_ENABLED.get(settings));
        assertFalse(enabled.get());
        enabled.set(isStateless == false && true);
        assertTrue("dynamic enable must take effect on non-stateless nodes", enabled.get());
    }
}
