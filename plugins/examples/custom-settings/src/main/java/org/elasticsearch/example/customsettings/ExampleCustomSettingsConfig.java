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
package org.elasticsearch.example.customsettings;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

/**
 * {@link ExampleCustomSettingsConfig} contains the custom settings values and their static declarations.
 */
public class ExampleCustomSettingsConfig {

    /**
     * A simple string setting
     */
    static final Setting<String> SIMPLE_SETTING = Setting.simpleString("custom.simple", Property.NodeScope);

    /**
     * A simple boolean setting that can be dynamically updated using the Cluster Settings API and that is {@code "false"} by default
     */
    static final Setting<Boolean> BOOLEAN_SETTING = Setting.boolSetting("custom.bool", false, Property.NodeScope, Property.Dynamic);

    /**
     * A string setting that can be dynamically updated and that is validated by some logic
     */
    static final Setting<String> VALIDATED_SETTING = Setting.simpleString("custom.validated", value -> {
        if (value != null && value.contains("forbidden")) {
            throw new IllegalArgumentException("Setting must not contain [forbidden]");
        }
    }, Property.NodeScope, Property.Dynamic);

    /**
     * A setting that is filtered out when listing all the cluster's settings
     */
    static final Setting<String> FILTERED_SETTING = Setting.simpleString("custom.filtered", Property.NodeScope, Property.Filtered);

    /**
     * A setting which contains a sensitive string. This may be any sensitive string, e.g. a username, a password, an auth token, etc.
     */
    static final Setting<SecureString> SECURED_SETTING = SecureSetting.secureString("custom.secured", null);

    /**
     * A setting that consists of a list of integers
     */
    static final Setting<List<Integer>> LIST_SETTING =
        Setting.listSetting("custom.list", Collections.emptyList(), Integer::valueOf, Property.NodeScope);


    private final String simple;
    private final String validated;
    private final Boolean bool;
    private final List<Integer> list;
    private final String filtered;

    public ExampleCustomSettingsConfig(final Environment environment) {
        // Elasticsearch config directory
        final Path configDir = environment.configFile();

        // Resolve the plugin's custom settings file
        final Path customSettingsYamlFile = configDir.resolve("custom-settings/custom.yml");

        // Load the settings from the plugin's custom settings file
        final Settings customSettings;
        try {
            customSettings = Settings.builder().loadFromPath(customSettingsYamlFile).build();
            assert customSettings != null;
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to load settings", e);
        }

        this.simple = SIMPLE_SETTING.get(customSettings);
        this.bool = BOOLEAN_SETTING.get(customSettings);
        this.validated = VALIDATED_SETTING.get(customSettings);
        this.filtered = FILTERED_SETTING.get(customSettings);
        this.list = LIST_SETTING.get(customSettings);

        // Loads the secured setting from the keystore
        final SecureString secured = SECURED_SETTING.get(environment.settings());
        assert secured != null;
    }

    public String getSimple() {
        return simple;
    }

    public Boolean getBool() {
        return bool;
    }

    public String getValidated() {
        return validated;
    }

    public String getFiltered() {
        return filtered;
    }

    public List<Integer> getList() {
        return list;
    }

}
