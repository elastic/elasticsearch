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
import org.elasticsearch.plugins.PluginsSynchronizer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This class is responsible for adding, updating or removing plugins so that the list of installed plugins
 * matches those in the {@code elasticsearch-plugins.yml} config file. It does this by loading a class
 * dynamically from the {@code plugin-cli} jar and executing it.
 */
public class PluginsManager {

    public static final String SYNC_PLUGINS_ACTION = "org.elasticsearch.plugins.cli.SyncPluginsAction";

    public static boolean configExists(Environment env) {
        return Files.exists(env.configFile().resolve("elasticsearch-plugins.yml"));
    }

    /**
     * Synchronizes the currently-installed plugins.
     * @param env the environment to use
     * @throws Exception if anything goes wrong
     */
    public static void syncPlugins(Environment env) throws Exception {
        ClassLoader classLoader = buildClassLoader(env);

        @SuppressWarnings("unchecked")
        final Class<PluginsSynchronizer> synchronizerClass = (Class<PluginsSynchronizer>) classLoader.loadClass(SYNC_PLUGINS_ACTION);

        final PluginsSynchronizer provider = synchronizerClass.getConstructor(Terminal.class, Environment.class)
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
