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
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.InstallPluginProvider;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.RemovePluginProblem;
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
            "org.elasticsearch.plugins.cli.action.InstallPluginAction"
        );
        @SuppressWarnings("unchecked")
        final Class<RemovePluginProvider> removeClass = (Class<RemovePluginProvider>) classLoader.loadClass(
            "org.elasticsearch.plugins.cli.action.RemovePluginAction"
        );

        this.pluginInstaller = installClass.getDeclaredConstructor(Terminal.class, Environment.class, boolean.class)
            .newInstance(LoggerTerminal.getLogger("org.elasticsearch.plugins.cli.action.InstallPluginAction"), env, true);

        if (proxy != null) {
            this.pluginInstaller.setProxy(proxy);
        }

        this.pluginRemover = removeClass.getDeclaredConstructor(Terminal.class, Environment.class, boolean.class)
            .newInstance(LoggerTerminal.getLogger("org.elasticsearch.plugins.cli.action.RemovePluginAction"), env, true);
    }

    public void removePlugins(List<PluginDescriptor> plugins) throws Exception {
        if (plugins.isEmpty()) {
            return;
        }

        final Tuple<RemovePluginProblem, String> problem = this.pluginRemover.checkRemovePlugins(plugins);
        if (problem != null) {
            logger.error("Cannot proceed with plugin removal: {}", problem.v2());
            throw new PluginSyncException(problem.v2());
        }

        this.pluginRemover.setPurge(true);
        this.pluginRemover.removePlugins(plugins);
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

        final Tuple<RemovePluginProblem, String> problem = this.pluginRemover.checkRemovePlugins(plugins);
        if (problem != null) {
            logger.error("Cannot proceed with plugin removal: {}", problem.v2());
            throw new PluginSyncException(problem.v2());
        }

        this.pluginRemover.setPurge(false);
        this.pluginInstaller.execute(plugins);
    }

    private ClassLoader buildClassLoader(Environment env) {
        final Path pluginLibDir = env.libFile().resolve("tools").resolve("plugin-cli");

        try {
            final URL[] urls = Files.list(pluginLibDir)
                .filter(each -> each.getFileName().toString().endsWith(".jar"))
                .map(this::pathToURL)
                .toArray(URL[]::new);

            return URLClassLoader.newInstance(urls, PluginsManager.class.getClassLoader());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list jars in [" + pluginLibDir + "]: " + e.getMessage(), e);
        }
    }

    private URL pathToURL(Path path) {
        try {
            return path.toUri().toURL();
        } catch (MalformedURLException e) {
            // Shouldn't happen, but have to handle the exception
            throw new RuntimeException("Failed to convert path [" + path + "] to URL", e);
        }
    }

}
