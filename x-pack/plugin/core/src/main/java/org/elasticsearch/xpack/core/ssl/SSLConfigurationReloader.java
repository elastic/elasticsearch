/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.ResourceWatcherService.Frequency;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * Ensures that the files backing an {@link SSLConfiguration} are monitored for changes and the underlying key/trust material is reloaded
 * and the {@link SSLContext} has existing sessions invalidated to force the use of the new key/trust material
 */
public final class SSLConfigurationReloader {

    private static final Logger logger = LogManager.getLogger(SSLConfigurationReloader.class);

    private final CompletableFuture<SSLService> sslServiceFuture = new CompletableFuture<>();

    public SSLConfigurationReloader(Environment environment,
                                    ResourceWatcherService resourceWatcherService,
                                    Collection<SSLConfiguration> sslConfigurations) {
        startWatching(environment, reloadConsumer(sslServiceFuture), resourceWatcherService, sslConfigurations);
    }

    // for testing
    SSLConfigurationReloader(Environment environment,
                             Consumer<SSLConfiguration> reloadConsumer,
                             ResourceWatcherService resourceWatcherService,
                             Collection<SSLConfiguration> sslConfigurations) {
        startWatching(environment, reloadConsumer, resourceWatcherService, sslConfigurations);
    }

    public void setSSLService(SSLService sslService) {
        final boolean completed = sslServiceFuture.complete(sslService);
        if (completed == false) {
            throw new IllegalStateException("ssl service future was already completed!");
        }
    }

    private static Consumer<SSLConfiguration> reloadConsumer(CompletableFuture<SSLService> future) {
        return sslConfiguration -> {
            try {
                final SSLService sslService = future.get();
                logger.debug("reloading ssl configuration [{}]", sslConfiguration);
                sslService.reloadSSLContext(sslConfiguration);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                throw new ElasticsearchException("failed to obtain ssl service", e);
            }
        };
    }

    /**
     * Collects all of the directories that need to be monitored for the provided {@link SSLConfiguration} instances and ensures that
     * they are being watched for changes
     */
    private static void startWatching(Environment environment, Consumer<SSLConfiguration> reloadConsumer,
                                      ResourceWatcherService resourceWatcherService, Collection<SSLConfiguration> sslConfigurations) {
        Map<Path, List<SSLConfiguration>> pathToConfigurationsMap = new HashMap<>();
        for (SSLConfiguration sslConfiguration : sslConfigurations) {
            for (Path directory : directoriesToMonitor(sslConfiguration.filesToMonitor(environment))) {
                pathToConfigurationsMap.compute(directory, (path, list) -> {
                    if (list == null) {
                        list = new ArrayList<>();
                    }
                    list.add(sslConfiguration);
                    return list;
                });
            }
        }

        for (Entry<Path, List<SSLConfiguration>> entry : pathToConfigurationsMap.entrySet()) {
            ChangeListener changeListener = new ChangeListener(environment, List.copyOf(entry.getValue()), reloadConsumer);
            FileWatcher fileWatcher = new FileWatcher(entry.getKey());
            fileWatcher.addListener(changeListener);
            try {
                resourceWatcherService.add(fileWatcher, Frequency.HIGH);
            } catch (IOException e) {
                logger.error("failed to start watching directory [{}] for ssl configurations [{}]", entry.getKey(), sslConfigurations);
            }
        }
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

    private static class ChangeListener implements FileChangesListener {

        private final Environment environment;
        private final List<SSLConfiguration> sslConfigurations;
        private final Consumer<SSLConfiguration> reloadConsumer;

        private ChangeListener(Environment environment, List<SSLConfiguration> sslConfigurations,
                               Consumer<SSLConfiguration> reloadConsumer) {
            this.environment = environment;
            this.sslConfigurations = sslConfigurations;
            this.reloadConsumer = reloadConsumer;
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
                    reloadConsumer.accept(sslConfiguration);
                    reloaded = true;
                }
            }

            if (reloaded) {
                logger.info("reloaded [{}] and updated ssl contexts using this file", file);
            }
        }
    }
}
