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

package org.elasticsearch.node.internal;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cluster.ClusterName;
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class InternalSettingsPreparerTests extends ESTestCase {

    Settings baseEnvSettings;

    @Before
    public void createBaseEnvSettings() {
        baseEnvSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();
    }

    @After
    public void clearBaseEnvSettings() {
        baseEnvSettings = null;
    }

    public void testEmptySettings() {
        Settings settings = InternalSettingsPreparer.prepareSettings(Settings.EMPTY);
        assertNotNull(settings.get("node.name")); // a name was set
        assertNotNull(settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey())); // a cluster name was set
        int size = settings.names().size();

        Environment env = InternalSettingsPreparer.prepareEnvironment(baseEnvSettings, null);
        settings = env.settings();
        assertNotNull(settings.get("node.name")); // a name was set
        assertNotNull(settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey())); // a cluster name was set
        assertEquals(settings.toString(), size + 1 /* path.home is in the base settings */, settings.names().size());
        String home = Environment.PATH_HOME_SETTING.get(baseEnvSettings);
        String configDir = env.configFile().toString();
        assertTrue(configDir, configDir.startsWith(home));
    }

    public void testReplacePromptPlaceholders() {
        MockTerminal terminal = new MockTerminal();
        terminal.addTextInput("text");
        terminal.addSecretInput("replaced");

        Settings.Builder builder = Settings.builder()
                .put(baseEnvSettings)
                .put("password.replace", InternalSettingsPreparer.SECRET_PROMPT_VALUE)
                .put("dont.replace", "prompt:secret")
                .put("dont.replace2", "_prompt:secret_")
                .put("dont.replace3", "_prompt:text__")
                .put("dont.replace4", "__prompt:text_")
                .put("dont.replace5", "prompt:secret__")
                .put("replace_me", InternalSettingsPreparer.TEXT_PROMPT_VALUE);
        Settings settings = InternalSettingsPreparer.prepareEnvironment(builder.build(), terminal).settings();

        assertThat(settings.get("password.replace"), equalTo("replaced"));
        assertThat(settings.get("replace_me"), equalTo("text"));

        // verify other values unchanged
        assertThat(settings.get("dont.replace"), equalTo("prompt:secret"));
        assertThat(settings.get("dont.replace2"), equalTo("_prompt:secret_"));
        assertThat(settings.get("dont.replace3"), equalTo("_prompt:text__"));
        assertThat(settings.get("dont.replace4"), equalTo("__prompt:text_"));
        assertThat(settings.get("dont.replace5"), equalTo("prompt:secret__"));
    }

    public void testReplaceSecretPromptPlaceholderWithNullTerminal() {
        Settings.Builder builder = Settings.builder()
                .put(baseEnvSettings)
                .put("replace_me1", InternalSettingsPreparer.SECRET_PROMPT_VALUE);
        try {
            InternalSettingsPreparer.prepareEnvironment(builder.build(), null);
            fail("an exception should have been thrown since no terminal was provided!");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage(), containsString("with value [" + InternalSettingsPreparer.SECRET_PROMPT_VALUE + "]"));
        }
    }

    public void testReplaceTextPromptPlaceholderWithNullTerminal() {
        Settings.Builder builder = Settings.builder()
                .put(baseEnvSettings)
                .put("replace_me1", InternalSettingsPreparer.TEXT_PROMPT_VALUE);
        try {
            InternalSettingsPreparer.prepareEnvironment(builder.build(), null);
            fail("an exception should have been thrown since no terminal was provided!");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage(), containsString("with value [" + InternalSettingsPreparer.TEXT_PROMPT_VALUE + "]"));
        }
    }

    public void testGarbageIsNotSwallowed() throws IOException {
        try {
            InputStream garbage = getClass().getResourceAsStream("/config/garbage/garbage.yml");
            Path home = createTempDir();
            Path config = home.resolve("config");
            Files.createDirectory(config);
            Files.copy(garbage, config.resolve("elasticsearch.yml"));
            InternalSettingsPreparer.prepareEnvironment(Settings.builder()
                .put(baseEnvSettings)
                .build(), null);
        } catch (SettingsException e) {
            assertEquals("Failed to load settings from [elasticsearch.yml]", e.getMessage());
        }
    }

    public void testMultipleSettingsFileNotAllowed() throws IOException {
        InputStream yaml = getClass().getResourceAsStream("/config/elasticsearch.yaml");
        InputStream properties = getClass().getResourceAsStream("/config/elasticsearch.properties");
        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(yaml, config.resolve("elasticsearch.yaml"));
        Files.copy(properties, config.resolve("elasticsearch.properties"));

        try {
            InternalSettingsPreparer.prepareEnvironment(Settings.builder()
                .put(baseEnvSettings)
                .build(), null);
        } catch (SettingsException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("multiple settings files found with suffixes"));
            assertTrue(e.getMessage(), e.getMessage().contains(".yaml"));
            assertTrue(e.getMessage(), e.getMessage().contains(".properties"));
        }
    }
}
