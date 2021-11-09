/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap.plugins;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.SyncPluginsProvider;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

public class PluginsManager {

    public static final String SYNC_PLUGINS_ACTION = "org.elasticsearch.plugins.cli.SyncPluginsAction";

    public static boolean configExists(Environment env) {
        return Files.exists(env.configFile().resolve("elasticsearch-plugins.yml"));
    }

    public static void syncPlugins(Environment env) throws Exception {
        ClassLoader classLoader = buildClassLoader(env);

        @SuppressWarnings("unchecked")
        final Class<SyncPluginsProvider> installClass = (Class<SyncPluginsProvider>) classLoader.loadClass(SYNC_PLUGINS_ACTION);

        final SyncPluginsProvider provider = installClass.getConstructor(Terminal.class, Environment.class)
            .newInstance(LoggerTerminal.getLogger(SYNC_PLUGINS_ACTION), env);

        provider.execute();
    }

    private static ClassLoader buildClassLoader(Environment env) {
        final Path pluginLibDir = env.libFile().resolve("tools").resolve("plugin-cli");

        try {
            final URL[] urls = Files.list(pluginLibDir)
                .filter(each -> each.getFileName().toString().endsWith(".jar"))
                .map(PluginsManager::pathToURL)
                .toArray(URL[]::new);

            return URLClassLoader.newInstance(urls, PluginsManager.class.getClassLoader());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list jars in [" + pluginLibDir + "]: " + e.getMessage(), e);
        }
    }

    private static URL pathToURL(Path path) {
        try {
            return path.toUri().toURL();
        } catch (MalformedURLException e) {
            // Shouldn't happen, but have to handle the exception
            throw new RuntimeException("Failed to convert path [" + path + "] to URL", e);
        }
    }
}
