/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

public class InternalSettingsPreparerTests extends ESTestCase {
    private static final Supplier<String> DEFAULT_NODE_NAME_SHOULDNT_BE_CALLED = () -> {
                throw new AssertionError("shouldn't be called");
            };

    Path homeDir;
    Settings baseEnvSettings;

    @Before
    public void createBaseEnvSettings() {
        homeDir = createTempDir();
        baseEnvSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), homeDir)
            .build();
    }

    @After
    public void clearBaseEnvSettings() {
        homeDir = null;
        baseEnvSettings = null;
    }

    public void testEmptySettings() {
        Settings settings = InternalSettingsPreparer.prepareSettings(Settings.EMPTY);
        assertNull(settings.get("node.name"));
        assertNotNull(settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey())); // a cluster name was set
        int size = settings.names().size();

        String defaultNodeName = randomAlphaOfLength(8);
        Environment env = InternalSettingsPreparer.prepareEnvironment(baseEnvSettings, emptyMap(), null, () -> defaultNodeName);
        settings = env.settings();
        assertEquals(defaultNodeName, settings.get("node.name"));
        assertNotNull(settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey())); // a cluster name was set
        assertEquals(settings.toString(), size + 1 /* path.home is in the base settings */, settings.names().size());
        String home = Environment.PATH_HOME_SETTING.get(baseEnvSettings);
        String configDir = env.configFile().toString();
        assertTrue(configDir, configDir.startsWith(home));
    }

    public void testDefaultClusterName() {
        Settings settings = InternalSettingsPreparer.prepareSettings(Settings.EMPTY);
        assertEquals("elasticsearch", settings.get("cluster.name"));
        settings = InternalSettingsPreparer.prepareSettings(Settings.builder().put("cluster.name", "foobar").build());
        assertEquals("foobar", settings.get("cluster.name"));
    }

    public void testGarbageIsNotSwallowed() throws IOException {
        try {
            InputStream garbage = getClass().getResourceAsStream("/config/garbage/garbage.yml");
            Path home = createTempDir();
            Path config = home.resolve("config");
            Files.createDirectory(config);
            Files.copy(garbage, config.resolve("elasticsearch.yml"));
            InternalSettingsPreparer.prepareEnvironment(Settings.builder().put(baseEnvSettings).build(),
                    emptyMap(), null, () -> "default_node_name");
        } catch (SettingsException e) {
            assertEquals("Failed to load settings from [elasticsearch.yml]", e.getMessage());
        }
    }

    public void testYamlNotAllowed() throws IOException {
        InputStream yaml = getClass().getResourceAsStream("/config/elasticsearch.yml");
        Path config = homeDir.resolve("config");
        Files.createDirectory(config);
        Files.copy(yaml, config.resolve("elasticsearch.yaml"));
        SettingsException e = expectThrows(SettingsException.class, () -> InternalSettingsPreparer.prepareEnvironment(
                Settings.builder().put(baseEnvSettings).build(), emptyMap(), null, DEFAULT_NODE_NAME_SHOULDNT_BE_CALLED));
        assertEquals("elasticsearch.yaml was deprecated in 5.5.0 and must be renamed to elasticsearch.yml", e.getMessage());
    }

    public void testJsonNotAllowed() throws IOException {
        InputStream yaml = getClass().getResourceAsStream("/config/elasticsearch.json");
        Path config = homeDir.resolve("config");
        Files.createDirectory(config);
        Files.copy(yaml, config.resolve("elasticsearch.json"));
        SettingsException e = expectThrows(SettingsException.class, () -> InternalSettingsPreparer.prepareEnvironment(
                Settings.builder().put(baseEnvSettings).build(), emptyMap(), null, DEFAULT_NODE_NAME_SHOULDNT_BE_CALLED));
        assertEquals("elasticsearch.json was deprecated in 5.5.0 and must be converted to elasticsearch.yml", e.getMessage());
    }

    public void testSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "secret");
        Settings input = Settings.builder().put(baseEnvSettings).setSecureSettings(secureSettings).build();
        Environment env = InternalSettingsPreparer.prepareEnvironment(input, emptyMap(),
                null, () -> "default_node_name");
        Setting<SecureString> fakeSetting = SecureSetting.secureString("foo", null);
        assertEquals("secret", fakeSetting.get(env.settings()).toString());
    }

    public void testDefaultPropertiesDoNothing() throws Exception {
        Map<String, String> props = Collections.singletonMap("default.setting", "foo");
        Environment env = InternalSettingsPreparer.prepareEnvironment(baseEnvSettings, props,
                null, () -> "default_node_name");
        assertEquals("foo", env.settings().get("default.setting"));
        assertNull(env.settings().get("setting"));
    }

}
