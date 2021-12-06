/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting.AffixSetting;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ConsistentSettingsIT extends ESIntegTestCase {

    static final Setting<SecureString> DUMMY_STRING_CONSISTENT_SETTING = SecureSetting.secureString(
        "dummy.consistent.secure.string.setting",
        null,
        Setting.Property.Consistent
    );
    static final AffixSetting<SecureString> DUMMY_AFFIX_STRING_CONSISTENT_SETTING = Setting.affixKeySetting(
        "dummy.consistent.secure.string.affix.setting.",
        "suffix",
        key -> SecureSetting.secureString(key, null, Setting.Property.Consistent)
    );
    private final AtomicReference<BiFunction<Integer, Settings, Settings>> nodeSettingsOverride = new AtomicReference<>(null);

    public void testAllConsistentOnAllNodesSuccess() throws Exception {
        for (String nodeName : internalCluster().getNodeNames()) {
            Environment environment = internalCluster().getInstance(Environment.class, nodeName);
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeName);
            assertTrue(
                "Empty settings list always consistent.",
                new ConsistentSettingsService(environment.settings(), clusterService, Collections.emptyList()).areAllConsistent()
            );
            assertTrue(
                "Simple consistent secure setting is consistent [" + clusterService.state().metadata().hashesOfConsistentSettings() + "].",
                new ConsistentSettingsService(
                    environment.settings(),
                    clusterService,
                    Collections.singletonList(DUMMY_STRING_CONSISTENT_SETTING)
                ).areAllConsistent()
            );
            assertTrue(
                "Affix consistent secure setting is consistent [" + clusterService.state().metadata().hashesOfConsistentSettings() + "].",
                new ConsistentSettingsService(
                    environment.settings(),
                    clusterService,
                    Collections.singletonList(DUMMY_AFFIX_STRING_CONSISTENT_SETTING)
                ).areAllConsistent()
            );
            assertTrue(
                "All secure settings are consistent [" + clusterService.state().metadata().hashesOfConsistentSettings() + "].",
                new ConsistentSettingsService(
                    environment.settings(),
                    clusterService,
                    List.of(DUMMY_STRING_CONSISTENT_SETTING, DUMMY_AFFIX_STRING_CONSISTENT_SETTING)
                ).areAllConsistent()
            );
        }
    }

    public void testConsistencyFailures() throws Exception {
        nodeSettingsOverride.set((n, s) -> {
            Settings.Builder builder = Settings.builder().put(super.nodeSettings(n, s));
            MockSecureSettings secureSettings = new MockSecureSettings();
            if (randomBoolean()) {
                // different value
                secureSettings.setString("dummy.consistent.secure.string.setting", "DIFFERENT_VALUE");
            } else {
                // missing value
                // secureSettings.setString("dummy.consistent.secure.string.setting", "string_value");
            }
            secureSettings.setString("dummy.consistent.secure.string.affix.setting." + "affix1" + ".suffix", "affix_value_1");
            secureSettings.setString("dummy.consistent.secure.string.affix.setting." + "affix2" + ".suffix", "affix_value_2");
            assert builder.getSecureSettings() == null : "Deal with the settings merge";
            builder.setSecureSettings(secureSettings);
            return builder.build();
        });
        String newNodeName = internalCluster().startNode();
        Environment environment = internalCluster().getInstance(Environment.class, newNodeName);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, newNodeName);
        assertTrue(
            "Empty settings list always consistent.",
            new ConsistentSettingsService(environment.settings(), clusterService, Collections.emptyList()).areAllConsistent()
        );
        assertFalse(
            "Simple consistent secure setting is NOT consistent [" + clusterService.state().metadata().hashesOfConsistentSettings() + "].",
            new ConsistentSettingsService(
                environment.settings(),
                clusterService,
                Collections.singletonList(DUMMY_STRING_CONSISTENT_SETTING)
            ).areAllConsistent()
        );
        assertTrue(
            "Affix consistent secure setting is consistent [" + clusterService.state().metadata().hashesOfConsistentSettings() + "].",
            new ConsistentSettingsService(
                environment.settings(),
                clusterService,
                Collections.singletonList(DUMMY_AFFIX_STRING_CONSISTENT_SETTING)
            ).areAllConsistent()
        );
        assertFalse(
            "All secure settings are NOT consistent [" + clusterService.state().metadata().hashesOfConsistentSettings() + "].",
            new ConsistentSettingsService(
                environment.settings(),
                clusterService,
                List.of(DUMMY_STRING_CONSISTENT_SETTING, DUMMY_AFFIX_STRING_CONSISTENT_SETTING)
            ).areAllConsistent()
        );
        nodeSettingsOverride.set((n, s) -> {
            Settings.Builder builder = Settings.builder().put(super.nodeSettings(n, s));
            MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString("dummy.consistent.secure.string.setting", "string_value");
            if (randomBoolean()) {
                secureSettings.setString("dummy.consistent.secure.string.affix.setting." + "affix1" + ".suffix", "affix_value_1");
                if (randomBoolean()) {
                    secureSettings.setString("dummy.consistent.secure.string.affix.setting." + "affix2" + ".suffix", "DIFFERENT_VALUE");
                } else {
                    // missing value
                    // "dummy.consistent.secure.string.affix.setting.affix2.suffix"
                }
            } else {
                if (randomBoolean()) {
                    secureSettings.setString("dummy.consistent.secure.string.affix.setting." + "affix1" + ".suffix", "DIFFERENT_VALUE_1");
                    secureSettings.setString("dummy.consistent.secure.string.affix.setting." + "affix2" + ".suffix", "DIFFERENT_VALUE_2");
                } else {
                    // missing values
                    // dummy.consistent.secure.string.affix.setting.affix1.suffix
                    // dummy.consistent.secure.string.affix.setting.affix2.suffix
                }
            }
            assert builder.getSecureSettings() == null : "Deal with the settings merge";
            builder.setSecureSettings(secureSettings);
            return builder.build();
        });
        newNodeName = internalCluster().startNode();
        environment = internalCluster().getInstance(Environment.class, newNodeName);
        clusterService = internalCluster().getInstance(ClusterService.class, newNodeName);
        assertTrue(
            "Empty settings list always consistent.",
            new ConsistentSettingsService(environment.settings(), clusterService, Collections.emptyList()).areAllConsistent()
        );
        assertTrue(
            "Simple consistent secure setting is consistent [" + clusterService.state().metadata().hashesOfConsistentSettings() + "].",
            new ConsistentSettingsService(
                environment.settings(),
                clusterService,
                Collections.singletonList(DUMMY_STRING_CONSISTENT_SETTING)
            ).areAllConsistent()
        );
        assertFalse(
            "Affix consistent secure setting is NOT consistent [" + clusterService.state().metadata().hashesOfConsistentSettings() + "].",
            new ConsistentSettingsService(
                environment.settings(),
                clusterService,
                Collections.singletonList(DUMMY_AFFIX_STRING_CONSISTENT_SETTING)
            ).areAllConsistent()
        );
        assertFalse(
            "All secure settings are NOT consistent [" + clusterService.state().metadata().hashesOfConsistentSettings() + "].",
            new ConsistentSettingsService(
                environment.settings(),
                clusterService,
                List.of(DUMMY_STRING_CONSISTENT_SETTING, DUMMY_AFFIX_STRING_CONSISTENT_SETTING)
            ).areAllConsistent()
        );
        nodeSettingsOverride.set(null);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        BiFunction<Integer, Settings, Settings> nodeSettingsOverrideFunction = nodeSettingsOverride.get();
        if (nodeSettingsOverrideFunction != null) {
            final Settings overrideSettings = nodeSettingsOverrideFunction.apply(nodeOrdinal, otherSettings);
            if (overrideSettings != null) {
                return overrideSettings;
            }
        }
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("dummy.consistent.secure.string.setting", "string_value");
        secureSettings.setString("dummy.consistent.secure.string.affix.setting." + "affix1" + ".suffix", "affix_value_1");
        secureSettings.setString("dummy.consistent.secure.string.affix.setting." + "affix2" + ".suffix", "affix_value_2");
        assert builder.getSecureSettings() == null : "Deal with the settings merge";
        builder.setSecureSettings(secureSettings);
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DummyPlugin.class);
    }

    public static final class DummyPlugin extends Plugin {

        public DummyPlugin() {}

        @Override
        public List<Setting<?>> getSettings() {
            List<Setting<?>> settings = new ArrayList<>(super.getSettings());
            settings.add(DUMMY_STRING_CONSISTENT_SETTING);
            settings.add(DUMMY_AFFIX_STRING_CONSISTENT_SETTING);
            return settings;
        }
    }
}
