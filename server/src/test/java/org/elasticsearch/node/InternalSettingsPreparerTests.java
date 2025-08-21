/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import static org.hamcrest.Matchers.equalTo;

public class InternalSettingsPreparerTests extends ESTestCase {
    private static final Supplier<String> DEFAULT_NODE_NAME_SHOULDNT_BE_CALLED = () -> { throw new AssertionError("shouldn't be called"); };

    Path homeDir;
    Settings baseEnvSettings;

    @Before
    public void createBaseEnvSettings() {
        homeDir = createTempDir();
        baseEnvSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), homeDir).build();
    }

    @After
    public void clearBaseEnvSettings() {
        homeDir = null;
        baseEnvSettings = null;
    }

    public void testEmptySettings() {
        String defaultNodeName = randomAlphaOfLength(8);
        Environment env = InternalSettingsPreparer.prepareEnvironment(baseEnvSettings, emptyMap(), null, () -> defaultNodeName);
        Settings settings = env.settings();
        assertEquals(defaultNodeName, settings.get("node.name"));
        assertNotNull(settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey())); // a cluster name was set
        String home = Environment.PATH_HOME_SETTING.get(baseEnvSettings);
        String configDir = env.configDir().toString();
        assertTrue(configDir, configDir.startsWith(home));
        assertEquals("elasticsearch", settings.get("cluster.name"));
    }

    public void testExplicitClusterName() {
        Settings.Builder output = Settings.builder().put(baseEnvSettings);
        InternalSettingsPreparer.prepareEnvironment(output.put("cluster.name", "foobar").build(), Map.of(), null, () -> "nodename");
        assertEquals("foobar", output.build().get("cluster.name"));
    }

    public void testGarbageIsNotSwallowed() throws IOException {
        try {
            InputStream garbage = getClass().getResourceAsStream("/config/garbage/garbage.yml");
            Path home = createTempDir();
            Path config = home.resolve("config");
            Files.createDirectory(config);
            Files.copy(garbage, config.resolve("elasticsearch.yml"));
            InternalSettingsPreparer.prepareEnvironment(
                Settings.builder().put(baseEnvSettings).build(),
                emptyMap(),
                null,
                () -> "default_node_name"
            );
        } catch (SettingsException e) {
            assertEquals("Failed to load settings from [elasticsearch.yml]", e.getMessage());
        }
    }

    public void testReplacePlaceholderFailure() {
        try {
            InternalSettingsPreparer.prepareEnvironment(
                Settings.builder().put(baseEnvSettings).put("cluster.name", "${ES_CLUSTER_NAME}").build(),
                emptyMap(),
                null,
                () -> "default_node_name"
            );
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertEquals("Failed to replace property placeholders from [elasticsearch.yml]", e.getMessage());
        }
    }

    public void testSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "secret");
        Settings input = Settings.builder().put(baseEnvSettings).setSecureSettings(secureSettings).build();
        Environment env = InternalSettingsPreparer.prepareEnvironment(input, emptyMap(), null, () -> "default_node_name");
        Setting<SecureString> fakeSetting = SecureSetting.secureString("foo", null);
        assertEquals("secret", fakeSetting.get(env.settings()).toString());
    }

    public void testDefaultPropertiesDoNothing() throws Exception {
        Map<String, String> props = Collections.singletonMap("default.setting", "foo");
        Environment env = InternalSettingsPreparer.prepareEnvironment(baseEnvSettings, props, null, () -> "default_node_name");
        assertEquals("foo", env.settings().get("default.setting"));
        assertNull(env.settings().get("setting"));
    }

    private Path copyConfig(String resourceName) throws IOException {
        InputStream yaml = getClass().getResourceAsStream(resourceName);
        Path configDir = homeDir.resolve("config");
        Files.createDirectory(configDir);
        Path configFile = configDir.resolve("elasticsearch.yaml");
        Files.copy(yaml, configFile);
        return configFile;
    }

    private Settings loadConfigWithSubstitutions(Path configFile, Map<String, String> env) throws IOException {
        Settings.Builder output = Settings.builder();
        InternalSettingsPreparer.loadConfigWithSubstitutions(output, configFile, env::get);
        return output.build();
    }

    public void testSubstitutionEntireLine() throws Exception {
        Path config = copyConfig("subst-entire-line.yml");
        Settings settings = loadConfigWithSubstitutions(config, Map.of("mysubst", "foo: bar"));
        assertThat(settings.get("foo"), equalTo("bar"));
    }

    public void testSubstitutionFirstLine() throws Exception {
        Path config = copyConfig("subst-first-line.yml");
        Settings settings = loadConfigWithSubstitutions(config, Map.of("mysubst", "v1"));
        assertThat(settings.get("foo"), equalTo("v1"));
        assertThat(settings.get("bar"), equalTo("v2"));
        assertThat(settings.get("baz"), equalTo("v3"));
    }

    public void testSubstitutionLastLine() throws Exception {
        Path config = copyConfig("subst-last-line.yml");
        Settings settings = loadConfigWithSubstitutions(config, Map.of("mysubst", "kazaam"));
        assertThat(settings.get("foo.bar.baz"), equalTo("kazaam"));
    }

    public void testSubstitutionMultiple() throws Exception {
        Path config = copyConfig("subst-multiple.yml");
        Settings settings = loadConfigWithSubstitutions(config, Map.of("s1", "substituted", "s2", "line"));
        assertThat(settings.get("foo"), equalTo("substituted line value"));
    }

    public void testSubstitutionMissingLenient() throws Exception {
        Path config = copyConfig("subst-missing.yml");
        Settings settings = loadConfigWithSubstitutions(config, Map.of());
        assertThat(settings.get("foo"), equalTo("${dne}"));
    }

    public void testSubstitutionBrokenLenient() throws Exception {
        Path config = copyConfig("subst-broken.yml");
        Settings settings = loadConfigWithSubstitutions(config, Map.of("goodsubst", "replaced"));
        assertThat(settings.get("foo"), equalTo("${no closing brace"));
        assertThat(settings.get("bar"), equalTo("replaced"));
    }

    public void testOverridesOverride() throws Exception {
        Settings.Builder output = Settings.builder().put("foo", "bar");
        InternalSettingsPreparer.loadOverrides(output, Map.of("foo", "baz"));
        Settings settings = output.build();
        assertThat(settings.get("foo"), equalTo("baz"));
    }

    public void testOverridesEmpty() throws Exception {
        Settings.Builder output = Settings.builder().put("foo", "bar");
        InternalSettingsPreparer.loadOverrides(output, Map.of());
        Settings settings = output.build();
        assertThat(settings.get("foo"), equalTo("bar"));
    }

    public void testOverridesNew() throws Exception {
        Settings.Builder output = Settings.builder().put("foo", "bar");
        InternalSettingsPreparer.loadOverrides(output, Map.of("baz", "wat"));
        Settings settings = output.build();
        assertThat(settings.get("foo"), equalTo("bar"));
        assertThat(settings.get("baz"), equalTo("wat"));
    }

    public void testOverridesMultiple() throws Exception {
        Settings.Builder output = Settings.builder().put("foo1", "bar").put("foo2", "baz");
        InternalSettingsPreparer.loadOverrides(output, Map.of("foo1", "wat", "foo2", "yas"));
        Settings settings = output.build();
        assertThat(settings.get("foo1"), equalTo("wat"));
        assertThat(settings.get("foo2"), equalTo("yas"));
    }
}
