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
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PluginsManifest {
    private List<PluginDescriptor> pluginDescriptors = List.of();
    private boolean purge = false;
    private boolean batch = false;
    private String proxy = null;

    public void validate(Path manifestPath) throws UserException {
        if (this.getPluginDescriptors().stream().anyMatch(each -> each == null || each.getId().isBlank())) {
            throw new RuntimeException("Cannot have null or empty plugin IDs in: " + manifestPath);
        }

        final Map<String, Long> counts = this.pluginDescriptors.stream()
            .map(PluginDescriptor::getId)
            .collect(Collectors.groupingBy(e -> e, Collectors.counting()));

        final List<String> duplicatePluginNames = counts.entrySet()
            .stream()
            .filter(entry -> entry.getValue() > 1)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        if (duplicatePluginNames.isEmpty() == false) {
            throw new RuntimeException("Duplicate plugin names " + duplicatePluginNames + " found in: " + manifestPath);
        }

        if (this.proxy != null) {
            validateProxy(this.proxy, null, manifestPath);
        }

        for (PluginDescriptor p : this.getPluginDescriptors()) {
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

    public List<PluginDescriptor> getPluginDescriptors() {
        return pluginDescriptors;
    }

    public void setPluginDescriptors(List<PluginDescriptor> pluginDescriptors) {
        this.pluginDescriptors = pluginDescriptors;
    }

    public boolean isPurge() {
        return purge;
    }

    public void setPurge(boolean purge) {
        this.purge = purge;
    }

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
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
        return purge == that.purge
            && batch == that.batch
            && pluginDescriptors.equals(that.pluginDescriptors)
            && Objects.equals(proxy, that.proxy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pluginDescriptors, purge, batch, proxy);
    }

    private void validateProxy(String proxy, String pluginId, Path manifestPath) throws UserException {
        String pluginDescription = pluginId == null ? "" : "for plugin [" + pluginId + "] ";
        try {
            URI uri = new URI(proxy);
            if (uri.getHost().isBlank()) {
                throw new UserException(ExitCodes.CONFIG, "Malformed host " + pluginDescription + "in [proxy] value in: " + manifestPath);
            }
            if (uri.getPort() == -1) {
                throw new UserException(ExitCodes.CONFIG, "Malformed or missing port " + pluginDescription + "in [proxy] value in: " + manifestPath);
            }
        } catch (URISyntaxException e) {
            throw new UserException(ExitCodes.CONFIG, "Malformed [proxy] value " + pluginDescription + "in: " + manifestPath);
        }
    }
}
