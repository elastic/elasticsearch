/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap.plugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.InstallPluginProvider;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginLogger;
import org.elasticsearch.plugins.RemovePluginProvider;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class PluginsActionWrapper {
    private final Logger logger = LogManager.getLogger(this.getClass());

    private final InstallPluginProvider pluginInstaller;
    private final RemovePluginProvider pluginRemover;

    public PluginsActionWrapper(Environment env, Proxy proxy) throws Exception {
        ClassLoader classLoader = buildClassLoader(env);

        @SuppressWarnings("unchecked")
        final Class<InstallPluginProvider> installClass = (Class<InstallPluginProvider>) classLoader.loadClass(
            "org.elasticsearch.plugins.cli.InstallPluginAction"
        );
        @SuppressWarnings("unchecked")
        final Class<RemovePluginProvider> removeClass = (Class<RemovePluginProvider>) classLoader.loadClass(
            "org.elasticsearch.plugins.cli.RemovePluginAction"
        );

        this.pluginInstaller = installClass.getDeclaredConstructor(PluginLogger.class, Environment.class, boolean.class)
            .newInstance(Log4jPluginLogger.getLogger("org.elasticsearch.plugins.cli.InstallPluginAction"), env, true);

        if (proxy != null) {
            this.pluginInstaller.setProxy(proxy);
        }

        this.pluginRemover = removeClass.getDeclaredConstructor(PluginLogger.class, Environment.class, boolean.class)
            .newInstance(Log4jPluginLogger.getLogger("org.elasticsearch.plugins.cli.RemovePluginAction"), env, true);
    }

    public void removePlugins(List<PluginDescriptor> plugins) throws Exception {
        if (plugins.isEmpty()) {
            return;
        }

        this.pluginRemover.setPurge(true);
        this.pluginRemover.execute(plugins);
    }

    public void installPlugins(List<PluginDescriptor> plugins) throws Exception {
        if (plugins.isEmpty()) {
            return;
        }
        this.pluginInstaller.execute(plugins);
    }

    public void upgradePlugins(List<PluginDescriptor> plugins) throws Exception {
        if (plugins.isEmpty()) {
            return;
        }
        this.pluginRemover.setPurge(false);
        this.pluginInstaller.execute(plugins);
    }

    private ClassLoader buildClassLoader(Environment env) throws PluginSyncException {
        final Path pluginLibDir = env.libFile()
            .resolve("tools")
            .resolve("plugin-cli");

        try {
            final URL[] urls = Files.list(pluginLibDir)
                .filter(each -> each.getFileName().toString().endsWith(".jar"))
                .map(each -> {
                    try {
                        return each.toUri().toURL();
                    } catch (MalformedURLException e) {
                        // Shouldn't happen, but have to handle the exception
                        throw new RuntimeException("Failed to convert path [" + each + "] to URL", e);
                    }
                })
                .toArray(URL[]::new);

            return URLClassLoader.newInstance(urls, PluginsManager.class.getClassLoader());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list jars in [" + pluginLibDir + "]: " + e.getMessage(), e);
        }
    }

}
