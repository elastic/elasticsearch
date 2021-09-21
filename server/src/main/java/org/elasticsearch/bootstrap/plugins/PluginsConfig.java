/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap.plugins;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);

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
     *     <li>Unofficial plugins must have locations</li>
     * </ul>
     *
     * @param officialPlugins the plugins that can be installed by name only
     * @throws PluginSyncException if validation problems are found
     */
    public void validate(Set<String> officialPlugins) throws PluginSyncException {
        if (this.plugins.stream().anyMatch(each -> each == null || each.getId() == null || each.getId().isBlank())) {
            throw new RuntimeException("Cannot have null or empty IDs in [elasticsearch-plugins.yml]");
        }

        final Set<String> uniquePluginIds = new HashSet<>();
        for (final PluginDescriptor plugin : plugins) {
            if (uniquePluginIds.add(plugin.getId()) == false) {
                throw new PluginSyncException("Duplicate plugin ID [" + plugin.getId() + "] found in [elasticsearch-plugins.yml]");
            }
        }

        for (PluginDescriptor plugin : this.plugins) {
            if (officialPlugins.contains(plugin.getId()) == false && plugin.getLocation() == null) {
                throw new PluginSyncException(
                    "Must specify location for non-official plugin [" + plugin.getId() + "] in [elasticsearch-plugins.yml]"
                );
            }
        }

        if (this.proxy != null) {
            final String[] parts = this.proxy.split(":");
            if (parts.length != 2) {
                throw new PluginSyncException("Malformed [proxy], expected [host:port] in [elasticsearch-plugins.yml]");
            }

            if (ProxyUtils.validateProxy(parts[0], parts[1]) == false) {
                throw new PluginSyncException("Malformed [proxy], expected [host:port] in [elasticsearch-plugins.yml]");
            }
        }

        for (PluginDescriptor p : plugins) {
            if (p.getLocation() != null) {
                if (p.getLocation().isBlank()) {
                    throw new PluginSyncException("Empty location for plugin [" + p.getId() + "]");
                }

                try {
                    // This also accepts Maven coordinates
                    new URI(p.getLocation());
                } catch (URISyntaxException e) {
                    throw new PluginSyncException("Malformed location for plugin [" + p.getId() + "]");
                }
            }
        }
    }

    /**
     * Constructs a {@link PluginsConfig} instance from the config YAML file
     * @param configPath the config file to load
     * @return a validated config
     * @throws PluginSyncException if there is a problem finding or parsing the file
     */
    public static PluginsConfig parseConfig(Path configPath) throws PluginSyncException {
        PluginsConfig pluginsConfig;
        try {
            byte[] configBytes = Files.readAllBytes(configPath);
            pluginsConfig = MAPPER.readValue(configBytes, PluginsConfig.class);
        } catch (IOException e) {
            throw new PluginSyncException("Cannot parse plugins config file [" + configPath + "]: " + e.getMessage(), e);
        }

        return pluginsConfig;
    }

    static void writeConfig(PluginsConfig config, Path destination) throws IOException {
        MAPPER.writeValue(Files.newOutputStream(destination), config);
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

    @Override
    public String toString() {
        return "PluginsConfig{plugins=" + plugins + ", proxy='" + proxy + "'}";
    }
}
