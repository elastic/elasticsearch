/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.ResourceWatcherService.Frequency;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Ensures that the files backing an {@link SSLConfiguration} are monitored for changes and the underlying key/trust material is reloaded
 * and the {@link SSLContext} has existing sessions invalidated to force the use of the new key/trust material
 */
public class SSLConfigurationReloader {

    private static final Logger logger = LogManager.getLogger(SSLConfigurationReloader.class);

    private final ConcurrentHashMap<Path, ChangeListener> pathToChangeListenerMap = new ConcurrentHashMap<>();
    private final Environment environment;
    private final ResourceWatcherService resourceWatcherService;
    private final SSLService sslService;

    public SSLConfigurationReloader(Environment env, SSLService sslService, ResourceWatcherService resourceWatcher) {
        this.environment = env;
        this.resourceWatcherService = resourceWatcher;
        this.sslService = sslService;
        startWatching(sslService.getLoadedSSLConfigurations());
    }

    /**
     * Collects all of the directories that need to be monitored for the provided {@link SSLConfiguration} instances and ensures that
     * they are being watched for changes
     */
    private void startWatching(Collection<SSLConfiguration> sslConfigurations) {
        for (SSLConfiguration sslConfiguration : sslConfigurations) {
            for (Path directory : directoriesToMonitor(sslConfiguration.filesToMonitor(environment))) {
                pathToChangeListenerMap.compute(directory, (path, listener) -> {
                    if (listener != null) {
                        listener.addSSLConfiguration(sslConfiguration);
                        return listener;
                    }

                    ChangeListener changeListener = new ChangeListener();
                    changeListener.addSSLConfiguration(sslConfiguration);
                    FileWatcher fileWatcher = new FileWatcher(path);
                    fileWatcher.addListener(changeListener);
                    try {
                        resourceWatcherService.add(fileWatcher, Frequency.HIGH);
                        return changeListener;
                    } catch (IOException e) {
                        logger.error("failed to start watching directory [{}] for ssl configuration [{}]", path, sslConfiguration);
                    }
                    return null;
                });
            }
        }
    }

    /**
     * Reloads the ssl context associated with this configuration. It is visible so that tests can override as needed
     */
    void reloadSSLContext(SSLConfiguration configuration) {
        logger.debug("reloading ssl configuration [{}]", configuration);
        sslService.sslContextHolder(configuration).reload();
    }

    /**
     * Returns a unique set of directories that need to be monitored based on the provided file paths
     */
    private static Set<Path> directoriesToMonitor(List<Path> filePaths) {
        Set<Path> paths = new HashSet<>();
        for (Path path : filePaths) {
            paths.add(path.getParent());
        }
        return paths;
    }

    private class ChangeListener implements FileChangesListener {

        private final CopyOnWriteArraySet<SSLConfiguration> sslConfigurations = new CopyOnWriteArraySet<>();

        /**
         * Adds the given ssl configuration to those that have files within the directory watched by this change listener
         */
        private void addSSLConfiguration(SSLConfiguration sslConfiguration) {
            sslConfigurations.add(sslConfiguration);
        }

        @Override
        public void onFileCreated(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileChanged(Path file) {
            boolean reloaded = false;
            for (SSLConfiguration sslConfiguration : sslConfigurations) {
                if (sslConfiguration.filesToMonitor(environment).contains(file)) {
                    reloadSSLContext(sslConfiguration);
                    reloaded = true;
                }
            }

            if (reloaded) {
                logger.info("reloaded [{}] and updated ssl contexts using this file", file);
            }
        }
    }
}
