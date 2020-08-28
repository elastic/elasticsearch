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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class ExampleCustomSettingsPlugin extends Plugin {

    private final ExampleCustomSettingsConfig config;

    public ExampleCustomSettingsPlugin(final Settings settings, final Path configPath) {
        this.config = new ExampleCustomSettingsConfig(new Environment(settings, configPath));

        // asserts that the setting has been correctly loaded from the custom setting file
        assert "secret".equals(config.getFiltered());
    }

    /**
     * @return the plugin's custom settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(ExampleCustomSettingsConfig.SIMPLE_SETTING,
                             ExampleCustomSettingsConfig.BOOLEAN_SETTING,
                             ExampleCustomSettingsConfig.VALIDATED_SETTING,
                             ExampleCustomSettingsConfig.FILTERED_SETTING,
                             ExampleCustomSettingsConfig.SECURED_SETTING,
                             ExampleCustomSettingsConfig.LIST_SETTING);
    }

    @Override
    public Settings additionalSettings() {
        final Settings.Builder builder = Settings.builder();

        // Exposes SIMPLE_SETTING and LIST_SETTING as a node settings
        builder.put(ExampleCustomSettingsConfig.SIMPLE_SETTING.getKey(), config.getSimple());

        final List<String> values = config.getList().stream().map(integer -> Integer.toString(integer)).collect(toList());
        builder.putList(ExampleCustomSettingsConfig.LIST_SETTING.getKey(), values);

        return builder.build();
    }
}
