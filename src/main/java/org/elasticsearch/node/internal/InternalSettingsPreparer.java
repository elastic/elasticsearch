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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Names;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;

import java.util.Map;

import static org.elasticsearch.common.Strings.cleanPath;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 *
 */
public class InternalSettingsPreparer {

    public static Tuple<Settings, Environment> prepareSettings(Settings pSettings, boolean loadConfigSettings) {
        // ignore this prefixes when getting properties from es. and elasticsearch.
        String[] ignorePrefixes = new String[]{"es.default.", "elasticsearch.default."};
        boolean useSystemProperties = !pSettings.getAsBoolean("config.ignore_system_properties", false);
        // just create enough settings to build the environment
        ImmutableSettings.Builder settingsBuilder = settingsBuilder().put(pSettings);
        if (useSystemProperties) {
            settingsBuilder.putProperties("elasticsearch.default.", System.getProperties())
                    .putProperties("es.default.", System.getProperties())
                    .putProperties("elasticsearch.", System.getProperties(), ignorePrefixes)
                    .putProperties("es.", System.getProperties(), ignorePrefixes);
        }
        settingsBuilder.replacePropertyPlaceholders();

        Environment environment = new Environment(settingsBuilder.build());

        if (loadConfigSettings) {
            boolean loadFromEnv = true;
            if (useSystemProperties) {
                // if its default, then load it, but also load form env
                if (Strings.hasText(System.getProperty("es.default.config"))) {
                    loadFromEnv = true;
                    settingsBuilder.loadFromUrl(environment.resolveConfig(System.getProperty("es.default.config")));
                }
                // if explicit, just load it and don't load from env
                if (Strings.hasText(System.getProperty("es.config"))) {
                    loadFromEnv = false;
                    settingsBuilder.loadFromUrl(environment.resolveConfig(System.getProperty("es.config")));
                }
                if (Strings.hasText(System.getProperty("elasticsearch.config"))) {
                    loadFromEnv = false;
                    settingsBuilder.loadFromUrl(environment.resolveConfig(System.getProperty("elasticsearch.config")));
                }
            }
            if (loadFromEnv) {
                try {
                    settingsBuilder.loadFromUrl(environment.resolveConfig("elasticsearch.yml"));
                } catch (FailedToResolveConfigException e) {
                    // ignore
                } catch (NoClassDefFoundError e) {
                    // ignore, no yaml
                }
                try {
                    settingsBuilder.loadFromUrl(environment.resolveConfig("elasticsearch.json"));
                } catch (FailedToResolveConfigException e) {
                    // ignore
                }
                try {
                    settingsBuilder.loadFromUrl(environment.resolveConfig("elasticsearch.properties"));
                } catch (FailedToResolveConfigException e) {
                    // ignore
                }
            }
        }

        settingsBuilder.put(pSettings);
        if (useSystemProperties) {
            settingsBuilder.putProperties("elasticsearch.", System.getProperties(), ignorePrefixes)
                    .putProperties("es.", System.getProperties(), ignorePrefixes);
        }
        settingsBuilder.replacePropertyPlaceholders();

        // allow to force set properties based on configuration of the settings provided
        for (Map.Entry<String, String> entry : pSettings.getAsMap().entrySet()) {
            String setting = entry.getKey();
            if (setting.startsWith("force.")) {
                settingsBuilder.remove(setting);
                settingsBuilder.put(setting.substring(".force".length()), entry.getValue());
            }
        }
        settingsBuilder.replacePropertyPlaceholders();

        // generate the name
        if (settingsBuilder.get("name") == null) {
            String name = System.getProperty("name");
            if (name == null || name.isEmpty()) {
                name = settingsBuilder.get("node.name");
                if (name == null || name.isEmpty()) {
                    name = Names.randomNodeName(environment.resolveConfig("names.txt"));
                }
            }

            if (name != null) {
                settingsBuilder.put("name", name);
            }
        }

        // put the cluster name
        if (settingsBuilder.get(ClusterName.SETTING) == null) {
            settingsBuilder.put(ClusterName.SETTING, ClusterName.DEFAULT.value());
        }

        Settings v1 = settingsBuilder.build();
        environment = new Environment(v1);

        // put back the env settings
        settingsBuilder = settingsBuilder().put(v1);
        // we put back the path.logs so we can use it in the logging configuration file
        settingsBuilder.put("path.logs", cleanPath(environment.logsFile().toAbsolutePath().toString()));

        v1 = settingsBuilder.build();

        return new Tuple<>(v1, environment);
    }
}
