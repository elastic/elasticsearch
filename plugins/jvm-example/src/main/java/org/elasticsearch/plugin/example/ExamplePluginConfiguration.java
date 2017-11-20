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

package org.elasticsearch.plugin.example;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Example configuration.
 */
public class ExamplePluginConfiguration {
    private final Settings customSettings;

    public static final Setting<String> TEST_SETTING =
      new Setting<String>("test", "default_value",
      (value) -> value, Setting.Property.Dynamic);

    public ExamplePluginConfiguration(Environment env) {
        // The directory part of the location matches the artifactId of this plugin
        Path path = env.configFile().resolve("jvm-example/example.yml");
        try {
            customSettings = Settings.builder().loadFromPath(path).build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load settings, giving up", e);
        }

        // asserts for tests
        assert customSettings != null;
        assert TEST_SETTING.get(customSettings) != null;
    }

    public String getTestConfig() {
        return TEST_SETTING.get(customSettings);
    }
}
