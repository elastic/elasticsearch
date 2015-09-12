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

import java.nio.charset.StandardCharsets;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.common.Strings.cleanPath;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;

/**
 *
 */
public class InternalSettingsPreparer {

    private static final String[] ALLOWED_SUFFIXES = {".yml", ".yaml", ".json", ".properties"};
    static final String[] PROPERTY_PREFIXES = {"es.", "elasticsearch."};
    static final String[] PROPERTY_DEFAULTS_PREFIXES = {"es.default.", "elasticsearch.default."};

    public static final String SECRET_PROMPT_VALUE = "${prompt.secret}";
    public static final String TEXT_PROMPT_VALUE = "${prompt.text}";
    public static final String IGNORE_SYSTEM_PROPERTIES_SETTING = "config.ignore_system_properties";

    /**
     * Prepares the settings by gathering all elasticsearch system properties and setting defaults.
     */
    public static Settings prepareSettings(Settings input) {
        Settings.Builder output = settingsBuilder();
        initializeSettings(output, input, true);
        finalizeSettings(output, null, null);
        return output.build();
    }

    /**
     * Prepares the settings by gathering all elasticsearch system properties, optionally loading the configuration settings,
     * and then replacing all property placeholders. If a {@link Terminal} is provided and configuration settings are loaded,
     * settings with a value of <code>${prompt.text}</code> or <code>${prompt.secret}</code> will result in a prompt for
     * the setting to the user.
     * @param input The initial settings to use
     * @param terminal the Terminal to use for input/output
     * @return the {@link Settings} and {@link Environment} as a {@link Tuple}
     */
    public static Environment prepareEnvironment(Settings input, Terminal terminal) {
        // just create enough settings to build the environment, to get the config dir
        Settings.Builder output = settingsBuilder();
        initializeSettings(output, input, true);
        Environment environment = new Environment(output.build());

        // TODO: can we simplify all of this and have a single filename, which is looked up in the config dir?
        boolean loadFromEnv = true;
        if (useSystemProperties(input)) {
            // if its default, then load it, but also load form env
            if (Strings.hasText(System.getProperty("es.default.config"))) {
                // TODO: we don't allow multiple config files, but having loadFromEnv true here allows just that
                loadFromEnv = true;
                output.loadFromPath(environment.configFile().resolve(System.getProperty("es.default.config")));
            }
            // TODO: these should be elseifs so that multiple files cannot be loaded
            // if explicit, just load it and don't load from env
            if (Strings.hasText(System.getProperty("es.config"))) {
                loadFromEnv = false;
                output.loadFromPath(environment.configFile().resolve(System.getProperty("es.config")));
            }
            if (Strings.hasText(System.getProperty("elasticsearch.config"))) {
                loadFromEnv = false;
                output.loadFromPath(environment.configFile().resolve(System.getProperty("elasticsearch.config")));
            }
        }
        if (loadFromEnv) {
            boolean settingsFileFound = false;
            Set<String> foundSuffixes = new HashSet<>();
            for (String allowedSuffix : ALLOWED_SUFFIXES) {
                Path path = environment.configFile().resolve("elasticsearch" + allowedSuffix);
                if (Files.exists(path)) {
                    if (!settingsFileFound) {
                        output.loadFromPath(path);
                    }
                    settingsFileFound = true;
                    foundSuffixes.add(allowedSuffix);
                }
            }
            if (foundSuffixes.size() > 1) {
                throw new SettingsException("multiple settings files found with suffixes: " + Strings.collectionToDelimitedString(foundSuffixes, ","));
            }
        }

        // re-initialize settings now that the config file has been loaded
        // TODO: only re-initialize if a config file was actually loaded
        initializeSettings(output, input, false);
        finalizeSettings(output, terminal, environment.configFile());

        environment = new Environment(output.build());

        // we put back the path.logs so we can use it in the logging configuration file
        output.put("path.logs", cleanPath(environment.logsFile().toAbsolutePath().toString()));

        return new Environment(output.build());
    }

    private static boolean useSystemProperties(Settings input) {
        return !input.getAsBoolean(IGNORE_SYSTEM_PROPERTIES_SETTING, false);
    }

    /**
     * Initializes the builder with the given input settings, and loads system properties settings if allowed.
     * If loadDefaults is true, system property default settings are loaded.
     */
    private static void initializeSettings(Settings.Builder output, Settings input, boolean loadDefaults) {
        output.put(input);
        if (useSystemProperties(input)) {
            if (loadDefaults) {
                for (String prefix : PROPERTY_DEFAULTS_PREFIXES) {
                    output.putProperties(prefix, System.getProperties());
                }
            }
            for (String prefix : PROPERTY_PREFIXES) {
                output.putProperties(prefix, System.getProperties(), PROPERTY_DEFAULTS_PREFIXES);
            }
        }
        output.replacePropertyPlaceholders();
    }

    /**
     * Finish preparing settings by replacing forced settings, prompts, and any defaults that need to be added.
     * The provided terminal is used to prompt for settings needing to be replaced.
     * The provided configDir is optional and will be used to lookup names.txt if the node name is not set, if provided.
     */
    private static void finalizeSettings(Settings.Builder output, Terminal terminal, Path configDir) {
        // allow to force set properties based on configuration of the settings provided
        List<String> forcedSettings = new ArrayList<>();
        for (String setting : output.internalMap().keySet()) {
            if (setting.startsWith("force.")) {
                forcedSettings.add(setting);
            }
        }
        for (String forcedSetting : forcedSettings) {
            String value = output.remove(forcedSetting);
            output.put(forcedSetting.substring("force.".length()), value);
        }
        output.replacePropertyPlaceholders();

        // check if name is set in settings, if not look for system property and set it
        if (output.get("name") == null) {
            String name = System.getProperty("name");
            if (name != null) {
                output.put("name", name);
            }
        }

        // put the cluster name
        if (output.get(ClusterName.SETTING) == null) {
            output.put(ClusterName.SETTING, ClusterName.DEFAULT.value());
        }

        String v = output.get(Settings.SETTINGS_REQUIRE_UNITS);
        if (v != null) {
            Settings.setSettingsRequireUnits(Booleans.parseBoolean(v, true));
        }

        replacePromptPlaceholders(output, terminal);
        // all settings placeholders have been resolved. resolve the value for the name setting by checking for name,
        // then looking for node.name, and finally generate one if needed
        if (output.get("name") == null) {
            String name = output.get("node.name");
            if (name == null || name.isEmpty()) {
                name = randomNodeName(configDir);
            }
            output.put("name", name);
        }
    }

    private static String randomNodeName(Path configDir) {
        InputStream input;
        if (configDir != null && Files.exists(configDir.resolve("names.txt"))) {
            Path namesPath = configDir.resolve("names.txt");
            try {
                input = Files.newInputStream(namesPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load custom names.txt from " + namesPath, e);
            }
        } else {
            input = InternalSettingsPreparer.class.getResourceAsStream("/config/names.txt");
        }

        try {
            List<String> names = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                String name = reader.readLine();
                while (name != null) {
                    names.add(name);
                    name = reader.readLine();
                }
            }
            int index = ThreadLocalRandom.current().nextInt(names.size());
            return names.get(index);
        } catch (IOException e) {
            throw new RuntimeException("Could not read node names list", e);
        }
    }

    private static void replacePromptPlaceholders(Settings.Builder settings, Terminal terminal) {
        List<String> secretToPrompt = new ArrayList<>();
        List<String> textToPrompt = new ArrayList<>();
        for (Map.Entry<String, String> entry : settings.internalMap().entrySet()) {
            switch (entry.getValue()) {
                case SECRET_PROMPT_VALUE:
                    secretToPrompt.add(entry.getKey());
                    break;
                case TEXT_PROMPT_VALUE:
                    textToPrompt.add(entry.getKey());
                    break;
            }
        }
        for (String setting : secretToPrompt) {
            String secretValue = promptForValue(setting, terminal, true);
            if (Strings.hasLength(secretValue)) {
                settings.put(setting, secretValue);
            } else {
                // TODO: why do we remove settings if prompt returns empty??
                settings.remove(setting);
            }
        }
        for (String setting : textToPrompt) {
            String textValue = promptForValue(setting, terminal, false);
            if (Strings.hasLength(textValue)) {
                settings.put(setting, textValue);
            } else {
                // TODO: why do we remove settings if prompt returns empty??
                settings.remove(setting);
            }
        }
    }

    private static String promptForValue(String key, Terminal terminal, boolean secret) {
        if (terminal == null) {
            throw new UnsupportedOperationException("found property [" + key + "] with value [" + (secret ? SECRET_PROMPT_VALUE : TEXT_PROMPT_VALUE) +"]. prompting for property values is only supported when running elasticsearch in the foreground");
        }

        if (secret) {
            return new String(terminal.readSecret("Enter value for [%s]: ", key));
        }
        return terminal.readText("Enter value for [%s]: ", key);
    }
}
