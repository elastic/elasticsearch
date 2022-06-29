/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.io.OutputStream;
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
    private List<InstallablePlugin> plugins;
    private String proxy;

    public PluginsConfig() {
        plugins = List.of();
        proxy = null;
    }

    public void setPlugins(List<InstallablePlugin> plugins) {
        this.plugins = plugins == null ? List.of() : plugins;
    }

    public void setProxy(String proxy) {
        this.proxy = proxy;
    }

    /**
     * Validate this instance. For example:
     * <ul>
     *     <li>All {@link InstallablePlugin}s must have IDs</li>
     *     <li>Any proxy must be well-formed.</li>
     *     <li>Unofficial plugins must have locations</li>
     * </ul>
     *
     * @param officialPlugins the plugins that can be installed by name only
     * @param migratedPlugins plugins that were once official but have since become modules. These
     *                        plugin IDs can still be specified, but do nothing.
     * @throws PluginSyncException if validation problems are found
     */
    public void validate(Set<String> officialPlugins, Set<String> migratedPlugins) throws PluginSyncException {
        if (this.plugins.stream().anyMatch(each -> each == null || Strings.isNullOrBlank(each.getId()))) {
            throw new RuntimeException("Cannot have null or empty IDs in [elasticsearch-plugins.yml]");
        }

        final Set<String> uniquePluginIds = new HashSet<>();
        for (final InstallablePlugin plugin : plugins) {
            if (uniquePluginIds.add(plugin.getId()) == false) {
                throw new PluginSyncException("Duplicate plugin ID [" + plugin.getId() + "] found in [elasticsearch-plugins.yml]");
            }
        }

        for (InstallablePlugin plugin : this.plugins) {
            if (officialPlugins.contains(plugin.getId()) == false
                && migratedPlugins.contains(plugin.getId()) == false
                && plugin.getLocation() == null) {
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

        for (InstallablePlugin p : plugins) {
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

    public List<InstallablePlugin> getPlugins() {
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

    /**
     * Constructs a {@link PluginsConfig} instance from the config YAML file
     *
     * @param configPath the config file to load
     * @param xContent the XContent type to expect when reading the file
     * @return a validated config
     */
    static PluginsConfig parseConfig(Path configPath, XContent xContent) throws IOException {
        // Normally a parser is declared and built statically in the class, but we'll only
        // use this when starting up Elasticsearch, so there's no point keeping one around.

        final ObjectParser<InstallablePlugin, Void> descriptorParser = new ObjectParser<>("descriptor parser", InstallablePlugin::new);
        descriptorParser.declareString(InstallablePlugin::setId, new ParseField("id"));
        descriptorParser.declareStringOrNull(InstallablePlugin::setLocation, new ParseField("location"));

        final ObjectParser<PluginsConfig, Void> parser = new ObjectParser<>("plugins parser", PluginsConfig::new);
        parser.declareStringOrNull(PluginsConfig::setProxy, new ParseField("proxy"));
        parser.declareObjectArrayOrNull(PluginsConfig::setPlugins, descriptorParser, new ParseField("plugins"));

        final XContentParser yamlXContentParser = xContent.createParser(
            XContentParserConfiguration.EMPTY,
            Files.newInputStream(configPath)
        );

        return parser.parse(yamlXContentParser, null);
    }

    /**
     * Write a config file to disk
     * @param xContent the format to use when writing the config
     * @param config the config to write
     * @param configPath the path to write to
     * @throws IOException if anything breaks
     */
    static void writeConfig(XContent xContent, PluginsConfig config, Path configPath) throws IOException {
        final OutputStream outputStream = Files.newOutputStream(configPath);
        final XContentBuilder builder = new XContentBuilder(xContent, outputStream);

        builder.startObject();
        builder.startArray("plugins");
        for (InstallablePlugin p : config.getPlugins()) {
            builder.startObject();
            {
                builder.field("id", p.getId());
                builder.field("location", p.getLocation());
            }
            builder.endObject();
        }
        builder.endArray();
        builder.field("proxy", config.getProxy());
        builder.endObject();

        builder.close();
        outputStream.close();
    }
}
