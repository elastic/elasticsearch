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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Names;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.Strings.cleanPath;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;

/**
 *
 */
public class InternalSettingsPreparer {

    static final List<String> ALLOWED_SUFFIXES = ImmutableList.of(".yml", ".yaml", ".json", ".properties");

    public static final String SECRET_PROMPT_VALUE = "${prompt.secret}";
    public static final String TEXT_PROMPT_VALUE = "${prompt.text}";
    public static final String IGNORE_SYSTEM_PROPERTIES_SETTING = "config.ignore_system_properties";

    /**
     * Prepares the settings by gathering all elasticsearch system properties, optionally loading the configuration settings,
     * and then replacing all property placeholders. This method will not work with settings that have <code>${prompt.text}</code>
     * or <code>${prompt.secret}</code> as their value unless they have been resolved previously.
     * @param pSettings The initial settings to use
     * @param loadConfigSettings flag to indicate whether to load settings from the configuration directory/file
     * @return the {@link Settings} and {@link Environment} as a {@link Tuple}
     */
    public static Tuple<Settings, Environment> prepareSettings(Settings pSettings, boolean loadConfigSettings) {
        return prepareSettings(pSettings, loadConfigSettings, null);
    }

    /**
     * Prepares the settings by gathering all elasticsearch system properties, optionally loading the configuration settings,
     * and then replacing all property placeholders. If a {@link Terminal} is provided and configuration settings are loaded,
     * settings with a value of <code>${prompt.text}</code> or <code>${prompt.secret}</code> will result in a prompt for
     * the setting to the user.
     * @param pSettings The initial settings to use
     * @param loadConfigSettings flag to indicate whether to load settings from the configuration directory/file
     * @param terminal the Terminal to use for input/output
     * @return the {@link Settings} and {@link Environment} as a {@link Tuple}
     */
    public static Tuple<Settings, Environment> prepareSettings(Settings pSettings, boolean loadConfigSettings, Terminal terminal) {
        // ignore this prefixes when getting properties from es. and elasticsearch.
        String[] ignorePrefixes = new String[]{"es.default.", "elasticsearch.default."};
        boolean useSystemProperties = !pSettings.getAsBoolean(IGNORE_SYSTEM_PROPERTIES_SETTING, false);
        // just create enough settings to build the environment
        Settings.Builder settingsBuilder = settingsBuilder().put(pSettings);
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
                for (String allowedSuffix : ALLOWED_SUFFIXES) {
                    try {
                        settingsBuilder.loadFromUrl(environment.resolveConfig("elasticsearch" + allowedSuffix));
                    } catch (FailedToResolveConfigException e) {
                        // ignore
                    }
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
                settingsBuilder.put(setting.substring("force.".length()), entry.getValue());
            }
        }
        settingsBuilder.replacePropertyPlaceholders();

        // check if name is set in settings, if not look for system property and set it
        if (settingsBuilder.get("name") == null) {
            String name = System.getProperty("name");
            if (name != null) {
                settingsBuilder.put("name", name);
            }
        }

        // put the cluster name
        if (settingsBuilder.get(ClusterName.SETTING) == null) {
            settingsBuilder.put(ClusterName.SETTING, ClusterName.DEFAULT.value());
        }

        String v = settingsBuilder.get(Settings.SETTINGS_REQUIRE_UNITS);
        if (v != null) {
            Settings.setSettingsRequireUnits(Booleans.parseBoolean(v, true));
        }

        Settings settings = replacePromptPlaceholders(settingsBuilder.build(), terminal);
        // all settings placeholders have been resolved. resolve the value for the name setting by checking for name,
        // then looking for node.name, and finally generate one if needed
        if (settings.get("name") == null) {
            final String name = settings.get("node.name");
            if (name == null || name.isEmpty()) {
                settings = settingsBuilder().put(settings)
                        .put("name", Names.randomNodeName(environment.resolveConfig("names.txt")))
                        .build();
            } else {
                settings = settingsBuilder().put(settings)
                        .put("name", name)
                        .build();
            }
        }

        environment = new Environment(settings);

        // put back the env settings
        settingsBuilder = settingsBuilder().put(settings);
        // we put back the path.logs so we can use it in the logging configuration file
        settingsBuilder.put("path.logs", cleanPath(environment.logsFile().toAbsolutePath().toString()));

        settings = settingsBuilder.build();

        return new Tuple<>(settings, environment);
    }

    static Settings replacePromptPlaceholders(Settings settings, Terminal terminal) {
        UnmodifiableIterator<Map.Entry<String, String>> iter = settings.getAsMap().entrySet().iterator();
        Settings.Builder builder = Settings.builder();

        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            String value = entry.getValue();
            String key = entry.getKey();
            switch (value) {
                case SECRET_PROMPT_VALUE:
                    String secretValue = promptForValue(key, terminal, true);
                    if (Strings.hasLength(secretValue)) {
                        builder.put(key, secretValue);
                    }
                    break;
                case TEXT_PROMPT_VALUE:
                    String textValue = promptForValue(key, terminal, false);
                    if (Strings.hasLength(textValue)) {
                        builder.put(key, textValue);
                    }
                    break;
                default:
                    builder.put(key, value);
                    break;
            }
        }

        return builder.build();
    }

    static String promptForValue(String key, Terminal terminal, boolean secret) {
        if (terminal == null) {
            throw new UnsupportedOperationException("found property [" + key + "] with value [" + (secret ? SECRET_PROMPT_VALUE : TEXT_PROMPT_VALUE) +"]. prompting for property values is only supported when running elasticsearch in the foreground");
        }

        if (secret) {
            return new String(terminal.readSecret("Enter value for [%s]: ", key));
        }
        return terminal.readText("Enter value for [%s]: ", key);
    }
}
