/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting.AffixSetting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ConsistentSettingsIT extends ESIntegTestCase {

    static final Setting<SecureString> DUMMY_STRING_CONSISTENT_SETTING = SecureSetting
            .secureString("dummy.consistent.secure.string.setting", null, Setting.Property.Consistent);
    static final AffixSetting<SecureString> DUMMY_AFFIX_STRING_CONSISTENT_SETTING = Setting.affixKeySetting(
            "dummy.consistent.secure.string.affix.setting.", "suffix",
            key -> SecureSetting.secureString(key, null, Setting.Property.Consistent));
    private final AtomicReference<Function<Integer, Settings>> nodeSettingsOverride = new AtomicReference<>(null);

    public void testAllConsistentOnAllNodesSuccess() throws Exception {
        for (String nodeName : internalCluster().getNodeNames()) {
            Environment environment = internalCluster().getInstance(Environment.class, nodeName);
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeName);
            assertTrue("Empty settings list always consistent.",
                    new ConsistentSettingsService(environment.settings(), clusterService, Collections.emptyList()).areAllConsistent());
            assertTrue(
                    "Simple consistent secure setting is consistent [" + clusterService.state().metaData().hashesOfConsistentSettings()
                            + "].",
                    new ConsistentSettingsService(environment.settings(), clusterService,
                            Collections.singletonList(DUMMY_STRING_CONSISTENT_SETTING)).areAllConsistent());
            assertTrue(
                    "Affix consistent secure setting is consistent [" + clusterService.state().metaData().hashesOfConsistentSettings()
                            + "].",
                    new ConsistentSettingsService(environment.settings(), clusterService,
                            Collections.singletonList(DUMMY_AFFIX_STRING_CONSISTENT_SETTING)).areAllConsistent());
            assertTrue("All secure settings are consistent [" + clusterService.state().metaData().hashesOfConsistentSettings() + "].",
                    new ConsistentSettingsService(environment.settings(), clusterService,
                            List.of(DUMMY_STRING_CONSISTENT_SETTING, DUMMY_AFFIX_STRING_CONSISTENT_SETTING)).areAllConsistent());
        }
    }

    public void testConsistencyFailures() throws Exception {
        nodeSettingsOverride.set(nodeOrdinal -> {
            Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
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
        assertTrue("Empty settings list always consistent.",
                new ConsistentSettingsService(environment.settings(), clusterService, Collections.emptyList()).areAllConsistent());
        assertFalse(
                "Simple consistent secure setting is NOT consistent [" + clusterService.state().metaData().hashesOfConsistentSettings()
                        + "].",
                new ConsistentSettingsService(environment.settings(), clusterService,
                        Collections.singletonList(DUMMY_STRING_CONSISTENT_SETTING)).areAllConsistent());
        assertTrue(
                "Affix consistent secure setting is consistent [" + clusterService.state().metaData().hashesOfConsistentSettings()
                        + "].",
                new ConsistentSettingsService(environment.settings(), clusterService,
                        Collections.singletonList(DUMMY_AFFIX_STRING_CONSISTENT_SETTING)).areAllConsistent());
        assertFalse("All secure settings are NOT consistent [" + clusterService.state().metaData().hashesOfConsistentSettings() + "].",
                new ConsistentSettingsService(environment.settings(), clusterService,
                        List.of(DUMMY_STRING_CONSISTENT_SETTING, DUMMY_AFFIX_STRING_CONSISTENT_SETTING)).areAllConsistent());
        nodeSettingsOverride.set(nodeOrdinal -> {
            Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
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
        assertTrue("Empty settings list always consistent.",
                new ConsistentSettingsService(environment.settings(), clusterService, Collections.emptyList()).areAllConsistent());
        assertTrue(
                "Simple consistent secure setting is consistent [" + clusterService.state().metaData().hashesOfConsistentSettings()
                        + "].",
                new ConsistentSettingsService(environment.settings(), clusterService,
                        Collections.singletonList(DUMMY_STRING_CONSISTENT_SETTING)).areAllConsistent());
        assertFalse(
                "Affix consistent secure setting is NOT consistent [" + clusterService.state().metaData().hashesOfConsistentSettings()
                        + "].",
                new ConsistentSettingsService(environment.settings(), clusterService,
                        Collections.singletonList(DUMMY_AFFIX_STRING_CONSISTENT_SETTING)).areAllConsistent());
        assertFalse("All secure settings are NOT consistent [" + clusterService.state().metaData().hashesOfConsistentSettings() + "].",
                new ConsistentSettingsService(environment.settings(), clusterService,
                        List.of(DUMMY_STRING_CONSISTENT_SETTING, DUMMY_AFFIX_STRING_CONSISTENT_SETTING)).areAllConsistent());
        nodeSettingsOverride.set(null);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Function<Integer, Settings> nodeSettingsOverrideFunction = nodeSettingsOverride.get();
        if (nodeSettingsOverrideFunction != null) {
            final Settings overrideSettings = nodeSettingsOverrideFunction.apply(nodeOrdinal);
            if (overrideSettings != null) {
                return overrideSettings;
            }
        }
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
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
        Collection<Class<? extends Plugin>> classes = new ArrayList<>(super.nodePlugins());
        classes.add(DummyPlugin.class);
        return classes;
    }

    public static final class DummyPlugin extends Plugin {

        public DummyPlugin() {
        }

        @Override
        public List<Setting<?>> getSettings() {
            List<Setting<?>> settings = new ArrayList<>(super.getSettings());
            settings.add(DUMMY_STRING_CONSISTENT_SETTING);
            settings.add(DUMMY_AFFIX_STRING_CONSISTENT_SETTING);
            return settings;
        }
    }
}
