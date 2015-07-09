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

import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.*;

public class InternalSettingsPreparerTests extends ElasticsearchTestCase {

    @Before
    public void setupSystemProperties() {
        System.setProperty("es.node.zone", "foo");
        System.setProperty("name", "sys-prop-name");
    }

    @After
    public void cleanupSystemProperties() {
        System.clearProperty("es.node.zone");
        System.clearProperty("name");
    }

    @Test
    public void testIgnoreSystemProperties() {
        Settings settings = settingsBuilder()
                .put("node.zone", "bar")
                .put("path.home", createTempDir().toString())
                .build();
        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(settings, true);
        // Should use setting from the system property
        assertThat(tuple.v1().get("node.zone"), equalTo("foo"));

        settings = settingsBuilder()
                .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true)
                .put("node.zone", "bar")
                .put("path.home", createTempDir().toString())
                .build();
        tuple = InternalSettingsPreparer.prepareSettings(settings, true);
        // Should use setting from the system property
        assertThat(tuple.v1().get("node.zone"), equalTo("bar"));
    }

    @Test
    public void testAlternateConfigFileSuffixes() {
        // test that we can read config files with .yaml, .json, and .properties suffixes
        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(settingsBuilder()
                .put("config.ignore_system_properties", true)
                .put("path.home", createTempDir().toString())
                .build(), true);

        assertThat(tuple.v1().get("yaml.config.exists"), equalTo("true"));
        assertThat(tuple.v1().get("json.config.exists"), equalTo("true"));
        assertThat(tuple.v1().get("properties.config.exists"), equalTo("true"));
    }

    @Test
    public void testReplacePromptPlaceholders() {
        final List<String> replacedSecretProperties = new ArrayList<>();
        final List<String> replacedTextProperties = new ArrayList<>();
        final Terminal terminal = new CliToolTestCase.MockTerminal() {
            @Override
            public char[] readSecret(String message, Object... args) {
                for (Object arg : args) {
                    replacedSecretProperties.add((String) arg);
                }
                return "replaced".toCharArray();
            }

            @Override
            public String readText(String message, Object... args) {
                for (Object arg : args) {
                    replacedTextProperties.add((String) arg);
                }
                return "text";
            }
        };

        Settings.Builder builder = settingsBuilder()
                .put("password.replace", InternalSettingsPreparer.SECRET_PROMPT_VALUE)
                .put("dont.replace", "prompt:secret")
                .put("dont.replace2", "_prompt:secret_")
                .put("dont.replace3", "_prompt:text__")
                .put("dont.replace4", "__prompt:text_")
                .put("dont.replace5", "prompt:secret__")
                .put("replace_me", InternalSettingsPreparer.TEXT_PROMPT_VALUE);
        Settings settings = builder.build();
        settings = InternalSettingsPreparer.replacePromptPlaceholders(settings, terminal);

        assertThat(replacedSecretProperties.size(), is(1));
        assertThat(replacedTextProperties.size(), is(1));
        assertThat(settings.get("password.replace"), equalTo("replaced"));
        assertThat(settings.get("replace_me"), equalTo("text"));

        // verify other values unchanged
        assertThat(settings.get("dont.replace"), equalTo("prompt:secret"));
        assertThat(settings.get("dont.replace2"), equalTo("_prompt:secret_"));
        assertThat(settings.get("dont.replace3"), equalTo("_prompt:text__"));
        assertThat(settings.get("dont.replace4"), equalTo("__prompt:text_"));
        assertThat(settings.get("dont.replace5"), equalTo("prompt:secret__"));
    }

    @Test
    public void testReplaceSecretPromptPlaceholderWithNullTerminal() {
        Settings.Builder builder = settingsBuilder()
                .put("replace_me1", InternalSettingsPreparer.SECRET_PROMPT_VALUE);
        try {
            InternalSettingsPreparer.replacePromptPlaceholders(builder.build(), null);
            fail("an exception should have been thrown since no terminal was provided!");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage(), containsString("with value [" + InternalSettingsPreparer.SECRET_PROMPT_VALUE + "]"));
        }
    }

    @Test
    public void testReplaceTextPromptPlaceholderWithNullTerminal() {
        Settings.Builder builder = settingsBuilder()
                .put("replace_me1", InternalSettingsPreparer.TEXT_PROMPT_VALUE);
        try {
            InternalSettingsPreparer.replacePromptPlaceholders(builder.build(), null);
            fail("an exception should have been thrown since no terminal was provided!");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage(), containsString("with value [" + InternalSettingsPreparer.TEXT_PROMPT_VALUE + "]"));
        }
    }

    @Test
    public void testNameSettingsPreference() {
        // Test system property overrides node.name
        Settings settings = settingsBuilder()
                .put("node.name", "node-name")
                .put("path.home", createTempDir().toString())
                .build();
        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(settings, true);
        assertThat(tuple.v1().get("name"), equalTo("sys-prop-name"));

        // test name in settings overrides sys prop and node.name
        settings = settingsBuilder()
                .put("name", "name-in-settings")
                .put("node.name", "node-name")
                .put("path.home", createTempDir().toString())
                .build();
        tuple = InternalSettingsPreparer.prepareSettings(settings, true);
        assertThat(tuple.v1().get("name"), equalTo("name-in-settings"));

        // test only node.name in settings
        System.clearProperty("name");
        settings = settingsBuilder()
                .put("node.name", "node-name")
                .put("path.home", createTempDir().toString())
                .build();
        tuple = InternalSettingsPreparer.prepareSettings(settings, true);
        assertThat(tuple.v1().get("name"), equalTo("node-name"));

        // test no name at all results in name being set
        settings = settingsBuilder()
                .put("path.home", createTempDir().toString())
                .build();
        tuple = InternalSettingsPreparer.prepareSettings(settings, true);
        assertThat(tuple.v1().get("name"), not("name-in-settings"));
        assertThat(tuple.v1().get("name"), not("sys-prop-name"));
        assertThat(tuple.v1().get("name"), not("node-name"));
        assertThat(tuple.v1().get("name"), notNullValue());
    }

    @Test
    public void testPromptForNodeNameOnlyPromptsOnce() {
        final AtomicInteger counter = new AtomicInteger();
        final Terminal terminal = new CliToolTestCase.MockTerminal() {
            @Override
            public char[] readSecret(String message, Object... args) {
                fail("readSecret should never be called by this test");
                return null;
            }

            @Override
            public String readText(String message, Object... args) {
                int count = counter.getAndIncrement();
                return "prompted name " + count;
            }
        };

        System.clearProperty("name");
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("node.name", InternalSettingsPreparer.TEXT_PROMPT_VALUE)
                .build();
        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(settings, false, terminal);
        settings = tuple.v1();
        assertThat(counter.intValue(), is(1));
        assertThat(settings.get("name"), is("prompted name 0"));
        assertThat(settings.get("node.name"), is("prompted name 0"));
    }
}
