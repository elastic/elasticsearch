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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.plugins.ProxyUtils.validateProxy;

/**
 * This class models the contents of the {@code elasticsearch-plugins.yml} file. This file specifies all the plugins
 * that ought to be installed in an Elasticsearch instance, and where to find them if they are not an official
 * Elasticsearch plugin.
 */
public class PluginsManifest {
    private final List<PluginDescriptor> plugins;
    private final String proxy;

    @JsonCreator
    public PluginsManifest(@JsonProperty("plugins") List<PluginDescriptor> plugins, @JsonProperty("proxy") String proxy) {
        this.plugins = plugins == null ? List.of() : plugins;
        this.proxy = proxy;
    }

    /**
     * Validate this instance. For example:
     * <ul>
     *     <li>All {@link PluginDescriptor}s must have IDs</li>
     *     <li>Proxies must be well-formed.</li>
     *     <li>Unofficial plugins must have URLs</li>
     * </ul>
     *
     * @param manifestPath the path to the file used to create this instance. Used to construct error messages.
     * @throws UserException if validation problems are found
     */
    public void validate(Path manifestPath) throws UserException {
        if (this.plugins.stream().anyMatch(each -> each == null || each.getId() == null || each.getId().isBlank())) {
            throw new RuntimeException("Cannot have null or empty plugin IDs in: " + manifestPath);
        }

        final Map<String, Long> counts = this.plugins.stream()
            .map(PluginDescriptor::getId)
            .collect(Collectors.groupingBy(e -> e, Collectors.counting()));

        final List<String> duplicatePluginNames = counts.entrySet()
            .stream()
            .filter(entry -> entry.getValue() > 1)
            .map(Map.Entry::getKey)
            .sorted()
            .collect(Collectors.toList());

        if (duplicatePluginNames.isEmpty() == false) {
            throw new RuntimeException("Duplicate plugin names " + duplicatePluginNames + " found in: " + manifestPath);
        }

        for (PluginDescriptor plugin : this.plugins) {
            if (InstallPluginAction.OFFICIAL_PLUGINS.contains(plugin.getId()) == false && plugin.getUrl() == null) {
                throw new UserException(ExitCodes.CONFIG, "Must specify URL for non-official plugin [" + plugin.getId() + "]");
            }
        }

        if (this.proxy != null) {
            validateProxy(this.proxy, null, manifestPath);
        }

        for (PluginDescriptor p : plugins) {
            if (p.getUrl() != null) {
                try {
                    new URL(p.getUrl());
                } catch (MalformedURLException e) {
                    throw new UserException(ExitCodes.CONFIG, "Malformed URL for plugin [" + p.getId() + "]");
                }
            }

            String proxy = p.getProxy();
            if (proxy != null) {
                validateProxy(proxy, p.getId(), manifestPath);
            }
        }
    }

    /**
     * Constructs a {@link PluginsManifest} instance from the specified YAML file, and validates the contents.
     * @param env the environment to use in order to locate the config file.
     * @return a validated manifest
     * @throws UserException if problems are found finding, parsing or validating the file
     */
    public static PluginsManifest parseManifest(Environment env) throws UserException {
        final Path manifestPath = env.configFile().resolve("elasticsearch-plugins.yml");
        if (Files.exists(manifestPath) == false) {
            throw new UserException(ExitCodes.CONFIG, "Plugin manifest file missing: " + manifestPath);
        }

        final YAMLFactory yamlFactory = new YAMLFactory();
        final ObjectMapper mapper = new ObjectMapper(yamlFactory);

        PluginsManifest pluginsManifest;
        try {
            byte[] manifestBytes = Files.readAllBytes(manifestPath);
            pluginsManifest = mapper.readValue(manifestBytes, PluginsManifest.class);
        } catch (IOException e) {
            throw new UserException(ExitCodes.CONFIG, "Cannot parse plugin manifest file [" + manifestPath + "]: " + e.getMessage());
        }

        pluginsManifest.validate(manifestPath);

        return pluginsManifest;
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
        PluginsManifest that = (PluginsManifest) o;
        return plugins.equals(that.plugins) && Objects.equals(proxy, that.proxy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plugins, proxy);
    }
}
