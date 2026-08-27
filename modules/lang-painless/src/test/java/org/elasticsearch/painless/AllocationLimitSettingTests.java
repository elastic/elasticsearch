/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests the {@code max_allocation_bytes} affix setting: parsing and bounds, and that {@link PainlessScriptEngine} reads it
 * once at construction into an immutable per-context map ({@code NodeScope} semantics).
 */
public class AllocationLimitSettingTests extends ESTestCase {

    private static final String TEST_CONTEXT = PainlessTestScript.CONTEXT.name;
    private static final String LIMIT_KEY = "script.painless.max_allocation_bytes.context." + TEST_CONTEXT + ".limit";

    private static ByteSizeValue resolve(String value) {
        Settings settings = Settings.builder().put(LIMIT_KEY, value).build();
        return CompilerSettings.MAX_ALLOCATION_BYTES.getConcreteSettingForNamespace(TEST_CONTEXT).get(settings);
    }

    public void testDefaultIsDisabledSentinel() {
        ByteSizeValue value = CompilerSettings.MAX_ALLOCATION_BYTES.getConcreteSettingForNamespace(TEST_CONTEXT).get(Settings.EMPTY);
        assertEquals(-1L, value.getBytes());
    }

    public void testParsesOffSentinel() {
        assertEquals(-1L, resolve("-1b").getBytes());
        assertEquals(-1L, resolve("-1").getBytes());
    }

    public void testParsesPositiveLimits() {
        assertEquals(1L, resolve("1b").getBytes());
        assertEquals(ByteSizeValue.ofMb(64).getBytes(), resolve("64mb").getBytes());
        assertEquals(ByteSizeValue.ofGb(1).getBytes(), resolve("1gb").getBytes());
    }

    public void testZeroRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("0b"));
        assertTrue(e.getMessage(), e.getMessage().contains("tracking disabled"));
    }

    public void testAboveUpperBoundRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("2gb"));
        assertTrue(e.getMessage(), e.getMessage().contains("1gb"));
    }

    public void testNegativesOtherThanSentinelRejected() {
        // -2b is rejected by ByteSizeValue parsing itself (only -1 is a legal negative byte size).
        expectThrows(IllegalArgumentException.class, () -> resolve("-2b"));
    }

    public void testEngineReadsLimitOnceAtConstruction() {
        Settings settings = Settings.builder().put(LIMIT_KEY, "32mb").build();
        PainlessScriptEngine engine = new PainlessScriptEngine(settings, scriptContexts());

        CompilerSettings contextDefaults = engine.getDefaultCompilerSettings(PainlessTestScript.CONTEXT);
        assertEquals(ByteSizeValue.ofMb(32).getBytes(), contextDefaults.getMaxAllocationBytes());
        assertTrue(contextDefaults.isAllocationTrackingEnabled());
    }

    public void testEngineDefaultsToDisabled() {
        PainlessScriptEngine engine = new PainlessScriptEngine(Settings.EMPTY, scriptContexts());
        CompilerSettings contextDefaults = engine.getDefaultCompilerSettings(PainlessTestScript.CONTEXT);
        assertEquals(-1L, contextDefaults.getMaxAllocationBytes());
        assertFalse(contextDefaults.isAllocationTrackingEnabled());
    }

    public void testSettingIsNodeScopeAndNotDynamic() {
        assertTrue(CompilerSettings.MAX_ALLOCATION_BYTES.hasNodeScope());
        assertFalse(CompilerSettings.MAX_ALLOCATION_BYTES.getProperties().contains(Setting.Property.Dynamic));
        assertFalse(CompilerSettings.MAX_ALLOCATION_BYTES.getProperties().contains(Setting.Property.OperatorDynamic));
    }

    /**
     * Models the rolling-upgrade boundary. A node that registers the setting (a new-version node, like {@link PainlessPlugin})
     * accepts the affix key; a node that does not (an older version) rejects it as an unknown setting, i.e. would fail to
     * start. The setting is {@code NodeScope} local config, so there is no transport/cluster-state BWC surface.
     */
    public void testUnregisteredNodeRejectsTheKey() {
        Settings withLimit = Settings.builder().put(LIMIT_KEY, "64mb").build();
        // New-version node: setting registered -> accepted.
        new SettingsModule(withLimit, CompilerSettings.MAX_ALLOCATION_BYTES);
        // Older node: setting not registered -> rejected at validation (node would refuse to start).
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SettingsModule(withLimit));
        assertThat(e.getMessage(), containsString("unknown setting"));
        assertThat(e.getMessage(), containsString(LIMIT_KEY));
    }

    private static Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PainlessPlugin.baseWhiteList());
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.test"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }
}
