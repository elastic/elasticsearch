/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * This class models the contents of the {@code elasticsearch-plugins.yml} file. This file specifies all the plugins
 * that ought to be installed in an Elasticsearch instance, and where to find them if they are not an official
 * Elasticsearch plugin.
 */
public class PluginsConfig {
    private final List<PluginDescriptor> plugins;
    private final String proxy;

    @JsonCreator
    public PluginsConfig(@JsonProperty("plugins") List<PluginDescriptor> plugins, @JsonProperty("proxy") String proxy) {
        this.plugins = plugins == null ? List.of() : plugins;
        this.proxy = proxy;
    }

    /**
     * Validate this instance. For example:
     * <ul>
     *     <li>All {@link PluginDescriptor}s must have IDs</li>
     *     <li>Any proxy must be well-formed.</li>
     *     <li>Unofficial plugins must have URLs</li>
     * </ul>
     *
     * @param configPath the path to the file used to create this instance. Used to construct error messages.
     * @throws UserException if validation problems are found
     */
    public void validate(Path configPath) throws UserException {
        if (this.plugins.stream().anyMatch(each -> each == null || each.getId() == null || each.getId().isBlank())) {
            throw new RuntimeException("Cannot have null or empty plugin IDs in: " + configPath);
        }

        final Set<String> uniquePluginIds = new HashSet<>();
        for (final PluginDescriptor plugin : plugins) {
            if (uniquePluginIds.add(plugin.getId()) == false) {
                throw new UserException(ExitCodes.USAGE, "Duplicate plugin ID [" + plugin.getId() + "] found in: " + configPath);
            }
        }

        for (PluginDescriptor plugin : this.plugins) {
            if (InstallPluginAction.OFFICIAL_PLUGINS.contains(plugin.getId()) == false && plugin.getLocation() == null) {
                throw new UserException(
                    ExitCodes.CONFIG,
                    "Must specify location for non-official plugin [" + plugin.getId() + "] in " + configPath
                );
            }
        }

        if (this.proxy != null) {
            final String[] parts = this.proxy.split(":");
            if (parts.length != 2) {
                throw new UserException(ExitCodes.CONFIG, "Malformed [proxy], expected [host:port] in: " + configPath);
            }

            if (ProxyUtils.validateProxy(parts[0], parts[1]) == false) {
                throw new UserException(ExitCodes.CONFIG, "Malformed [proxy], expected [host:port] in: " + configPath);
            }
        }

        for (PluginDescriptor p : plugins) {
            if (p.getLocation() != null) {
                try {
                    new URL(p.getLocation());
                } catch (MalformedURLException e) {
                    throw new UserException(ExitCodes.CONFIG, "Malformed URL for plugin [" + p.getId() + "]");
                }
            }
        }
    }

    /**
     * Constructs a {@link PluginsConfig} instance from the specified YAML file, and validates the contents.
     * @param env the environment to use in order to locate the config file.
     * @return a validated config
     * @throws UserException if there is a problem finding, parsing or validating the file
     */
    public static PluginsConfig parseConfig(Environment env) throws UserException {
        final Path configPath = env.configFile().resolve("elasticsearch-plugins.yml");
        if (Files.exists(configPath) == false) {
            throw new UserException(ExitCodes.CONFIG, "Plugins config file missing: " + configPath);
        }

        final YAMLFactory yamlFactory = new YAMLFactory();
        final ObjectMapper mapper = new ObjectMapper(yamlFactory);

        PluginsConfig pluginsConfig;
        try {
            byte[] configBytes = Files.readAllBytes(configPath);
            pluginsConfig = mapper.readValue(configBytes, PluginsConfig.class);
        } catch (IOException e) {
            throw new UserException(ExitCodes.CONFIG, "Cannot parse plugins config file [" + configPath + "]: " + e.getMessage());
        }

        pluginsConfig.validate(configPath);

        return pluginsConfig;
    }

    public List<PluginDescriptor> getPlugins() {
        return plugins;
    }

    public String getProxy() {
        return proxy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PluginsConfig that = (PluginsConfig) o;
        return plugins.equals(that.plugins) && Objects.equals(proxy, that.proxy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plugins, proxy);
    }
}
