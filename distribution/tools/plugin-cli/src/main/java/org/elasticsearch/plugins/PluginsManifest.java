/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.plugins.ProxyUtils.buildProxy;
import static org.elasticsearch.plugins.ProxyUtils.validateProxy;

public class PluginsManifest {
    private List<PluginDescriptor> plugins = List.of();
    private String proxy = null;

    public void validate(Path manifestPath) throws UserException {
        if (this.plugins == null) {
            this.plugins = List.of();
        }

        if (this.getPlugins().stream().anyMatch(each -> each == null || each.getId() == null || each.getId().isBlank())) {
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

        if (this.proxy != null) {
            validateProxy(this.proxy, null, manifestPath);
        }

        for (PluginDescriptor p : this.getPlugins()) {
            String proxy = p.getProxy();
            if (proxy != null) {
                validateProxy(proxy, p.getId(), manifestPath);
            }
        }

    }

    public static PluginsManifest parseManifest(Environment env) throws UserException, IOException {
        final Path manifestPath = env.configFile().resolve("elasticsearch-plugins.yml");
        if (Files.exists(manifestPath) == false) {
            throw new UserException(1, "Plugin manifest file missing: " + manifestPath);
        }

        final YAMLFactory yamlFactory = new YAMLFactory();
        final ObjectMapper mapper = new ObjectMapper(yamlFactory);

        PluginsManifest pluginsManifest;
        try {
            pluginsManifest = mapper.readValue(manifestPath.toFile(), PluginsManifest.class);
        } catch (IOException e) {
            throw new UserException(2, "Cannot parse plugin manifest file [" + manifestPath + "]: " + e.getMessage());
        }

        pluginsManifest.validate(manifestPath);

        return pluginsManifest;
    }

    public List<PluginDescriptor> getPlugins() {
        return plugins;
    }

    public void setPlugins(List<PluginDescriptor> plugins) {
        this.plugins = plugins;
    }

    public String getProxy() {
        return proxy;
    }

    public void setProxy(String proxy) {
        this.proxy = proxy;
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
        return plugins.equals(that.plugins)
            && Objects.equals(proxy, that.proxy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plugins, proxy);
    }
}
