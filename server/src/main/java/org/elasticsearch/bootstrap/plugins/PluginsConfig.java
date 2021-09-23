/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap.plugins;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;

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
    private List<PluginDescriptor> plugins;
    private String proxy;

    public PluginsConfig() {
        plugins = null;
        proxy = null;
    }

    public void setPlugins(List<PluginDescriptor> plugins) {
        this.plugins = plugins;
    }

    public void setProxy(String proxy) {
        this.proxy = proxy;
    }

    public PluginsConfig(List<PluginDescriptor> plugins, String proxy) {
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

    /**
     * Constructs a {@link PluginsConfig} instance from the config YAML file
     *
     * @param configPath the config file to load
     * @return a validated config
     */
    static PluginsConfig parseConfig(Path configPath) throws IOException {
        // Normally a parser is declared and built statically in the class, but we'll only
        // use this when starting up Elasticsearch, so there's no point keeping one around.

        final ObjectParser<PluginDescriptor, Void> descriptorParser = new ObjectParser<>("descriptor parser", PluginDescriptor::new);
        descriptorParser.declareString(PluginDescriptor::setId, new ParseField("id"));
        descriptorParser.declareStringOrNull(PluginDescriptor::setLocation, new ParseField("location"));

        final ObjectParser<PluginsConfig, Void> parser = new ObjectParser<>("plugins parser", PluginsConfig::new);
        parser.declareStringOrNull(PluginsConfig::setProxy, new ParseField("proxy"));
        parser.declareObjectArrayOrNull(PluginsConfig::setPlugins, descriptorParser, new ParseField("plugins"));

        final XContentParser yamlXContentParser = YamlXContent.yamlXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Files.newInputStream(configPath)
        );

        return parser.parse(yamlXContentParser, null);
    }

    static void writeConfig(PluginsConfig config, Path configPath) throws IOException {
        final XContentBuilder builder = YamlXContent.contentBuilder();

        builder.startObject();
        builder.startArray("plugins");
        for (PluginDescriptor p : config.getPlugins()) {
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

        final BytesReference bytes = BytesReference.bytes(builder);

        Files.write(configPath, bytes.array());
    }
}
