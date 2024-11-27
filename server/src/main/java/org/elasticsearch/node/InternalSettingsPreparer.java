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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class InternalSettingsPreparer {

    /**
     * Prepares the settings by gathering all elasticsearch system properties, optionally loading the configuration settings.
     *
     * @param input      the custom settings to use; these are not overwritten by settings in the configuration file
     * @param properties map of properties key/value pairs (usually from the command-line)
     * @param configPath path to config directory; (use null to indicate the default)
     * @param defaultNodeName supplier for the default node.name if the setting isn't defined
     * @return the {@link Environment}
     */
    public static Environment prepareEnvironment(
        Settings input,
        Map<String, String> properties,
        Path configPath,
        Supplier<String> defaultNodeName
    ) {
        Path configDir = findConfigDir(configPath, input, properties);
        Path configFile = configDir.resolve("elasticsearch.yml");

        Settings.Builder output = Settings.builder(); // start with a fresh output

        loadConfigWithSubstitutions(output, configFile, System::getenv);
        loadOverrides(output, properties);
        output.put(input);
        replaceForcedSettings(output);
        try {
            output.replacePropertyPlaceholders();
        } catch (Exception e) {
            throw new SettingsException("Failed to replace property placeholders from [" + configFile.getFileName() + "]", e);
        }
        ensureSpecialSettingsExist(output, defaultNodeName);

        return new Environment(output.build(), configDir);
    }

    static Path findConfigDir(Path configPath, Settings input, Map<String, String> properties) {
        if (configPath != null) {
            return configPath;
        }

        String esHome = properties.get(Environment.PATH_HOME_SETTING.getKey());
        if (esHome == null) {
            // TODO: this fallback is only needed for tests, in production input is always Settings.EMPTY
            esHome = Environment.PATH_HOME_SETTING.get(input);
            if (esHome == null) {
                throw new IllegalStateException(Environment.PATH_HOME_SETTING.getKey() + " is not configured");
            }
        }

        return resolveConfigDir(esHome);
    }

    @SuppressForbidden(reason = "reading initial config")
    private static Path resolveConfigDir(String esHome) {
        return PathUtils.get(esHome).resolve("config");
    }

    static void loadConfigWithSubstitutions(Settings.Builder output, Path configFile, Function<String, String> substitutions) {

        if (Files.exists(configFile) == false) {
            return;
        }

        try {
            long existingSize = Files.size(configFile);
            StringBuilder builder = new StringBuilder((int) existingSize);
            try (BufferedReader reader = Files.newBufferedReader(configFile, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    int dollarNdx;
                    int nextNdx = 0;
                    while ((dollarNdx = line.indexOf("${", nextNdx)) != -1) {
                        int closeNdx = line.indexOf('}', dollarNdx + 2);
                        if (closeNdx == -1) {
                            // No close substitution was found. Break to leniently copy the rest of the line as is.
                            break;
                        }
                        // copy up to the dollar
                        if (dollarNdx > nextNdx) {
                            builder.append(line, nextNdx, dollarNdx);
                        }
                        nextNdx = closeNdx + 1;

                        String substKey = line.substring(dollarNdx + 2, closeNdx);
                        String substValue = substitutions.apply(substKey);
                        if (substValue != null) {
                            builder.append(substValue);
                        } else {
                            // the substitution name doesn't exist, defer to setting based substitution after yaml parsing
                            builder.append(line, dollarNdx, nextNdx);
                        }
                    }
                    if (nextNdx < line.length()) {
                        builder.append(line, nextNdx, line.length());
                    }
                    builder.append(System.lineSeparator());
                }
            }
            var is = new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8));
            output.loadFromStream(configFile.getFileName().toString(), is, false);
        } catch (IOException e) {
            throw new SettingsException("Failed to load settings from " + configFile.toString(), e);
        }
    }

    static void loadOverrides(Settings.Builder output, Map<String, String> overrides) {
        StringBuilder builder = new StringBuilder();
        for (var entry : overrides.entrySet()) {
            builder.append(entry.getKey());
            builder.append(": ");
            builder.append(entry.getValue());
            builder.append(System.lineSeparator());
        }
        var is = new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8));
        // fake the resource name so it loads yaml
        try {
            output.loadFromStream("overrides.yml", is, false);
        } catch (IOException e) {
            throw new SettingsException("Malformed setting override value", e);
        }
    }

    private static void replaceForcedSettings(Settings.Builder output) {
        List<String> forcedSettings = new ArrayList<>();
        for (String setting : output.keys()) {
            if (setting.startsWith("force.")) {
                forcedSettings.add(setting);
            }
        }
        for (String forcedSetting : forcedSettings) {
            String value = output.remove(forcedSetting);
            output.put(forcedSetting.substring("force.".length()), value);
        }
    }

    private static void ensureSpecialSettingsExist(Settings.Builder output, Supplier<String> defaultNodeName) {
        // put the cluster and node name if they aren't set
        if (output.get(ClusterName.CLUSTER_NAME_SETTING.getKey()) == null) {
            output.put(ClusterName.CLUSTER_NAME_SETTING.getKey(), ClusterName.DEFAULT.value());
        }
        if (output.get(Node.NODE_NAME_SETTING.getKey()) == null) {
            output.put(Node.NODE_NAME_SETTING.getKey(), defaultNodeName.get());
        }
    }
}
