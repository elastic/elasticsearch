/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A "bundle" is a group of jars that will be loaded in their own classloader
 */
public class PluginBundle {
    public final PluginDescriptor plugin;
    private final Path dir;
    public final Set<URL> urls;
    public final Set<URL> spiUrls;
    public final Set<URL> allUrls;

    PluginBundle(PluginDescriptor plugin, Path dir) throws IOException {
        this.plugin = Objects.requireNonNull(plugin);
        this.dir = dir;

        Path spiDir = dir.resolve("spi");
        // plugin has defined an explicit api for extension
        this.spiUrls = Files.exists(spiDir) ? gatherUrls(spiDir) : null;
        this.urls = gatherUrls(dir);
        Set<URL> allUrls = new LinkedHashSet<>(urls);
        if (spiUrls != null) {
            allUrls.addAll(spiUrls);
        }
        this.allUrls = allUrls;
    }

    public Path getDir() {
        return dir;
    }

    public PluginDescriptor pluginDescriptor() {
        return this.plugin;
    }

    static Set<URL> gatherUrls(Path dir) throws IOException {
        Set<URL> urls = new LinkedHashSet<>();
        // gather urls for jar files
        try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(dir, "*.jar")) {
            for (Path jar : jarStream) {
                // normalize with toRealPath to get symlinks out of our hair
                URL url = jar.toRealPath().toUri().toURL();
                if (urls.add(url) == false) {
                    throw new IllegalStateException("duplicate codebase: " + url);
                }
            }
        }
        return urls;
    }

    boolean hasSPI() {
        return spiUrls != null;
    }

    Set<URL> getExtensionUrls() {
        if (spiUrls != null) {
            return spiUrls;
        }
        return urls;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PluginBundle bundle = (PluginBundle) o;
        return Objects.equals(plugin, bundle.plugin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plugin);
    }

}
