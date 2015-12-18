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

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@SuppressForbidden(reason = "modifies system properties intentionally")
public class EvilInternalSettingsPreparerTests extends ESTestCase {

    Map<String, String> savedProperties = new HashMap<>();
    Settings baseEnvSettings;

    @Before
    public void saveSettingsSystemProperties() {
        // clear out any properties the settings preparer may look for
        savedProperties.clear();
        for (Object propObj : System.getProperties().keySet()) {
            String property = (String)propObj;
            // NOTE: these prefixes are prefixes of the defaults, so both are handled here
            for (String prefix : InternalSettingsPreparer.PROPERTY_PREFIXES) {
                if (property.startsWith(prefix)) {
                    savedProperties.put(property, System.getProperty(property));
                }
            }
        }
        String name = System.getProperty("name");
        if (name != null) {
            savedProperties.put("name", name);
        }
        for (String property : savedProperties.keySet()) {
            System.clearProperty(property);
        }
    }

    @After
    public void restoreSettingsSystemProperties() {
        for (Map.Entry<String, String> property : savedProperties.entrySet()) {
            System.setProperty(property.getKey(), property.getValue());
        }
    }

    @Before
    public void createBaseEnvSettings() {
        baseEnvSettings = settingsBuilder()
            .put("path.home", createTempDir())
            .build();
    }

    @After
    public void clearBaseEnvSettings() {
        baseEnvSettings = null;
    }

    public void testIgnoreSystemProperties() {
        try {
            System.setProperty("es.node.zone", "foo");
            Settings settings = settingsBuilder()
                .put("node.zone", "bar")
                .put(baseEnvSettings)
                .build();
            Environment env = InternalSettingsPreparer.prepareEnvironment(settings, null);
            // Should use setting from the system property
            assertThat(env.settings().get("node.zone"), equalTo("foo"));

            settings = settingsBuilder()
                .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true)
                .put("node.zone", "bar")
                .put(baseEnvSettings)
                .build();
            env = InternalSettingsPreparer.prepareEnvironment(settings, null);
            // Should use setting from the system property
            assertThat(env.settings().get("node.zone"), equalTo("bar"));
        } finally {
            System.clearProperty("es.node.zone");
        }
    }

    public void testNameSettingsPreference() {
        try {
            System.setProperty("name", "sys-prop-name");
            // Test system property overrides node.name
            Settings settings = settingsBuilder()
                .put("node.name", "node-name")
                .put(baseEnvSettings)
                .build();
            Environment env = InternalSettingsPreparer.prepareEnvironment(settings, null);
            assertThat(env.settings().get("name"), equalTo("sys-prop-name"));

            // test name in settings overrides sys prop and node.name
            settings = settingsBuilder()
                .put("name", "name-in-settings")
                .put("node.name", "node-name")
                .put(baseEnvSettings)
                .build();
            env = InternalSettingsPreparer.prepareEnvironment(settings, null);
            assertThat(env.settings().get("name"), equalTo("name-in-settings"));

            // test only node.name in settings
            System.clearProperty("name");
            settings = settingsBuilder()
                .put("node.name", "node-name")
                .put(baseEnvSettings)
                .build();
            env = InternalSettingsPreparer.prepareEnvironment(settings, null);
            assertThat(env.settings().get("name"), equalTo("node-name"));

            // test no name at all results in name being set
            env = InternalSettingsPreparer.prepareEnvironment(baseEnvSettings, null);
            assertThat(env.settings().get("name"), not("name-in-settings"));
            assertThat(env.settings().get("name"), not("sys-prop-name"));
            assertThat(env.settings().get("name"), not("node-name"));
            assertThat(env.settings().get("name"), notNullValue());
        } finally {
            System.clearProperty("name");
        }
    }
}
