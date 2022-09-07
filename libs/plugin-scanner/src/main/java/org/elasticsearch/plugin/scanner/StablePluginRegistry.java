/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import org.elasticsearch.plugin.scanner.spi.StablePluginRegistryLocator;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public interface StablePluginRegistry {

    StablePluginRegistry INSTANCE = getInstance();

    private static StablePluginRegistry getInstance() {
        try (Stream<Path> pathStream = ClassUtil.ofModulePath()) {
            return StablePluginRegistryLocator.STABLE_PLUGIN_REGISTRY_PROVIDER.getInstance(pathStream);
        } catch (IOException e) {
            // e.printStackTrace();
            return StablePluginRegistryLocator.STABLE_PLUGIN_REGISTRY_PROVIDER.getInstance(Stream.empty());
        }
    }

    void scanBundleForStablePlugins(Set<URL> pluginsUrls, ClassLoader pluginClassLoader);

    Map<String, NameToPluginInfo> getNamedComponents();
}
