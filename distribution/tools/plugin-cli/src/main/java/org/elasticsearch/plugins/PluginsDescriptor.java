/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PluginsDescriptor {
    private List<String> pluginIds = List.of();

    public List<String> getPluginIds() {
        return pluginIds;
    }

    public void setPluginIds(List<String> pluginIds) {
        this.pluginIds = pluginIds;
    }

    public void validate(Path descriptorPath) {
        if (this.getPluginIds().stream().anyMatch(each -> each == null || each.trim().isEmpty())) {
            throw new RuntimeException("Cannot have null or empty plugin IDs in: " + descriptorPath);
        }

        final Map<String, Long> counts = this.pluginIds.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));

        final List<String> duplicatePluginNames = counts.entrySet()
            .stream()
            .filter(entry -> entry.getValue() > 1)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        if (duplicatePluginNames.isEmpty() == false) {
            throw new RuntimeException("Duplicate plugin names " + duplicatePluginNames + " found in: " + descriptorPath);
        }
    }

    public static PluginsDescriptor parsePluginsDescriptor(Environment env) throws IOException, UserException {
        final Path descriptorPath = env.configFile().resolve("elasticsearch-plugins.yml");
        if (Files.exists(descriptorPath) == false) {
            throw new UserException(1, "Plugin descriptor file missing: " + descriptorPath);
        }
        Yaml yaml = new Yaml(new SafeConstructor());
        Map<String, Object> root;
        try {
            root = yaml.load(Files.readString(descriptorPath));
        } catch (YAMLException | ClassCastException ex) {
            throw new UserException(2, "Cannot parse plugin descriptor file [" + descriptorPath + "]: " + ex.getMessage());
        }

        final PluginsDescriptor pluginsDescriptor = new PluginsDescriptor();

        for (Map.Entry<String, Object> entry : root.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if ("plugins".equals(key)) {
                if (value instanceof List) {
                    pluginsDescriptor.setPluginIds(asStringList(value));
                } else {
                    throw new UserException(2, "Expected a list of strings for [" + key + "] in plugin descriptor file: " + descriptorPath);
                }
            } else {
                throw new UserException(2, "Unknown key [" + key + "] in plugin descriptor file: " + descriptorPath);
            }
        }

        pluginsDescriptor.validate(descriptorPath);

        return pluginsDescriptor;
    }

    @SuppressWarnings("unchecked")
    private static List<String> asStringList(Object input) {
        return ((List<Object>) input).stream().map(String::valueOf).collect(Collectors.toList());
    }
}
