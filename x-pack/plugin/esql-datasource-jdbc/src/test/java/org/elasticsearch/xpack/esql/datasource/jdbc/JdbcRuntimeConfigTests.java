/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

/**
 * Behavioural tests for {@link JdbcRuntimeConfig}: defaults, initialization from {@link Settings}, and the
 * dynamic-update path used by cluster-settings consumers. Each case uses a fresh instance; the config is per-plugin
 * instance state (owned by {@link JdbcDataSourcePlugin}), not a static. The SPI instance the module queries owns its
 * own config and receives cluster-settings updates via {@code registerClusterSettings} -- there is no class-level
 * static bridge or shared instance.
 */
public class JdbcRuntimeConfigTests extends ESTestCase {

    public void testDefaultsAreEnabledAndDefaultGuard() {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        assertTrue(config.enabled());
        assertEquals(SsrfGuard.DEFAULT_ALLOWED_SUBPROTOCOLS, config.guard().allowedSubprotocols());
        assertFalse(config.guard().allowLoopback());
    }

    public void testInitializeReadsAllSettings() {
        Settings s = Settings.builder()
            .put(JdbcRuntimeConfig.ENABLED.getKey(), false)
            .putList(JdbcRuntimeConfig.ALLOWED_SUBPROTOCOLS.getKey(), List.of("jdbc:custom://"))
            .put(JdbcRuntimeConfig.ALLOW_LOOPBACK.getKey(), true)
            .build();
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        config.initialize(s);
        assertFalse(config.enabled());
        assertEquals(1, config.guard().allowedSubprotocols().size());
        assertTrue(config.guard().allowedSubprotocols().contains("jdbc:custom://"));
        assertTrue(config.guard().allowLoopback());
    }

    public void testInitializeWithEmptyAllowedSubprotocolsKeepsDefaults() {
        Settings s = Settings.builder().putList(JdbcRuntimeConfig.ALLOWED_SUBPROTOCOLS.getKey(), List.of()).build();
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        config.initialize(s);
        assertEquals(
            "empty list must fall back to the production default allowlist so we don't accidentally let everything",
            SsrfGuard.DEFAULT_ALLOWED_SUBPROTOCOLS,
            config.guard().allowedSubprotocols()
        );
    }

    public void testSetEnabledFlipsKillSwitchAtomically() {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        assertTrue(config.enabled());
        config.setEnabled(false);
        assertFalse(config.enabled());
        config.setEnabled(true);
        assertTrue(config.enabled());
    }

    public void testPushdownEnabledDefaultsOn() {
        // Pushdown defaults ON so the connector pushes WHERE clauses out of the box; the setting only exists to turn
        // it OFF (the operational kill switch / parity-test lever).
        assertTrue(new JdbcRuntimeConfig().pushdownEnabled());
    }

    public void testInitializeReadsPushdownEnabled() {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        config.initialize(Settings.builder().put(JdbcRuntimeConfig.PUSHDOWN_ENABLED.getKey(), false).build());
        assertFalse("esql.jdbc.pushdown.enabled=false must seed the runtime config", config.pushdownEnabled());
    }

    public void testSetPushdownEnabledFlipsAtomically() {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        assertTrue(config.pushdownEnabled());
        config.setPushdownEnabled(false);
        assertFalse(config.pushdownEnabled());
        config.setPushdownEnabled(true);
        assertTrue(config.pushdownEnabled());
    }

    public void testSetAllowedSubprotocolsPreservesLoopbackToggle() {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        config.initialize(Settings.builder().put(JdbcRuntimeConfig.ALLOW_LOOPBACK.getKey(), true).build());
        assertTrue("preconditions: loopback allowed", config.guard().allowLoopback());
        config.setAllowedSubprotocols(List.of("jdbc:postgresql://"));
        assertTrue("loopback toggle must survive a subprotocols update", config.guard().allowLoopback());
        assertEquals(1, config.guard().allowedSubprotocols().size());
    }

    public void testSetAllowLoopbackPreservesAllowlist() {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        config.setAllowedSubprotocols(List.of("jdbc:custom://"));
        config.setAllowLoopback(true);
        assertEquals(1, config.guard().allowedSubprotocols().size());
        assertTrue(config.guard().allowLoopback());
    }

    public void testInitializeRejectsNullSettings() {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        expectThrows(IllegalArgumentException.class, () -> config.initialize(null));
    }

    public void testSettingsListContainsAllOwnedSettings() {
        var settings = JdbcRuntimeConfig.settings();
        assertEquals(10, settings.size());
        assertTrue(settings.contains(JdbcRuntimeConfig.ENABLED));
        assertTrue(settings.contains(JdbcRuntimeConfig.PUSHDOWN_ENABLED));
        assertTrue(settings.contains(JdbcRuntimeConfig.ALLOWED_SUBPROTOCOLS));
        assertTrue(settings.contains(JdbcRuntimeConfig.ALLOW_LOOPBACK));
        assertTrue(settings.contains(JdbcRuntimeConfig.POOL_MAX_PER_URL));
        assertTrue(settings.contains(JdbcRuntimeConfig.POOL_CONNECTION_TIMEOUT_MS));
        assertTrue(settings.contains(JdbcRuntimeConfig.POOL_IDLE_TIMEOUT_MS));
        assertTrue(settings.contains(JdbcRuntimeConfig.POOL_MAX_LIFETIME_MS));
        assertTrue(settings.contains(JdbcRuntimeConfig.POOL_KEEPALIVE_MS));
        assertTrue(settings.contains(JdbcRuntimeConfig.POOL_VALIDATION_TIMEOUT_MS));
    }

    public void testPoolDefaults() {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        assertEquals(10, config.poolMaxPerUrl());
        assertEquals(5000L, config.poolConnectionTimeoutMs());
        assertEquals(30000L, config.poolIdleTimeoutMs());
        assertEquals(900000L, config.poolMaxLifetimeMs());
        // keepalive disabled by default (a trivially-valid ordering); validationTimeout == connectionTimeout.
        assertEquals(0L, config.poolKeepaliveMs());
        assertEquals(5000L, config.poolValidationTimeoutMs());
    }

    public void testDefaultsSatisfyTheOrderingInvariant() {
        // With the defaults, keepalive (0=disabled) < idle (30000) < max_lifetime (900000) and validation (5000)
        // <= connection_timeout (5000) all hold, so no clamp/disable happens out of the box.
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        assertTrue(config.poolValidationTimeoutMs() <= config.poolConnectionTimeoutMs());
        assertTrue(config.poolIdleTimeoutMs() < config.poolMaxLifetimeMs());
        assertEquals("keepalive disabled by default", 0L, config.poolKeepaliveMs());
    }

    public void testInitializeReadsPoolSettings() {
        Settings s = Settings.builder()
            .put(JdbcRuntimeConfig.POOL_MAX_PER_URL.getKey(), 4)
            .put(JdbcRuntimeConfig.POOL_CONNECTION_TIMEOUT_MS.getKey(), 750L)
            .put(JdbcRuntimeConfig.POOL_IDLE_TIMEOUT_MS.getKey(), 12000L)
            .put(JdbcRuntimeConfig.POOL_MAX_LIFETIME_MS.getKey(), 60000L)
            .put(JdbcRuntimeConfig.POOL_KEEPALIVE_MS.getKey(), 40000L)
            .put(JdbcRuntimeConfig.POOL_VALIDATION_TIMEOUT_MS.getKey(), 700L)
            .build();
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        config.initialize(s);
        assertEquals(4, config.poolMaxPerUrl());
        assertEquals(750L, config.poolConnectionTimeoutMs());
        assertEquals(12000L, config.poolIdleTimeoutMs());
        assertEquals(60000L, config.poolMaxLifetimeMs());
        assertEquals(40000L, config.poolKeepaliveMs());
        assertEquals(700L, config.poolValidationTimeoutMs());
    }

    public void testPoolSettersUpdateValues() {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        config.setPoolMaxPerUrl(3);
        config.setPoolConnectionTimeoutMs(400L);
        config.setPoolIdleTimeoutMs(11000L);
        config.setPoolMaxLifetimeMs(50000L);
        config.setPoolKeepaliveMs(35000L);
        config.setPoolValidationTimeoutMs(350L);
        assertEquals(3, config.poolMaxPerUrl());
        assertEquals(400L, config.poolConnectionTimeoutMs());
        assertEquals(11000L, config.poolIdleTimeoutMs());
        assertEquals(50000L, config.poolMaxLifetimeMs());
        assertEquals(35000L, config.poolKeepaliveMs());
        assertEquals(350L, config.poolValidationTimeoutMs());
    }
}
