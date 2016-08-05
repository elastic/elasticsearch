/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ssl;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.ResourceWatcherService.Frequency;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSessionContext;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Ensures that the files backing an {@link SSLConfiguration} are monitored for changes and the underlying key/trust material is reloaded
 * and the {@link SSLContext} has existing sessions invalidated to force the use of the new key/trust material
 */
public class SSLConfigurationReloader extends AbstractComponent implements AbstractSSLService.Listener {

    private final ConcurrentHashMap<Path, ChangeListener> pathToChangeListenerMap = new ConcurrentHashMap<>();
    private final Environment environment;
    private final ResourceWatcherService resourceWatcherService;
    private final ServerSSLService serverSSLService;
    private final ClientSSLService clientSSLService;

    public SSLConfigurationReloader(Settings settings, Environment env, ServerSSLService serverSSLService,
                                    ClientSSLService clientSSLService, ResourceWatcherService resourceWatcher) {
        super(settings);
        this.environment = env;
        this.resourceWatcherService = resourceWatcher;
        this.serverSSLService = serverSSLService;
        this.clientSSLService = clientSSLService;
        serverSSLService.setListener(this);
        clientSSLService.setListener(this);
    }

    @Override
    public void onSSLContextLoaded(SSLConfiguration sslConfiguration) {
        startWatching(Collections.singleton(sslConfiguration));
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
     * Invalidates all of the sessions in the provided {@link SSLContext}
     */
    private static void invalidateAllSessions(SSLContext context) {
        if (context != null) {
            invalidateSessions(context.getClientSessionContext());
            invalidateSessions(context.getServerSessionContext());
        }
    }

    /**
     * Invalidates the sessions in the provided {@link SSLSessionContext}
     */
    private static void invalidateSessions(SSLSessionContext sslSessionContext) {
        Enumeration<byte[]> sessionIds = sslSessionContext.getIds();
        while (sessionIds.hasMoreElements()) {
            byte[] sessionId = sessionIds.nextElement();
            sslSessionContext.getSession(sessionId).invalidate();
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
            for (SSLConfiguration sslConfiguration : sslConfigurations) {
                if (sslConfiguration.filesToMonitor(environment).contains(file)) {
                    sslConfiguration.reload(file, environment);
                    invalidateAllSessions(serverSSLService.getSSLContext(sslConfiguration));
                    invalidateAllSessions(clientSSLService.getSSLContext(sslConfiguration));
                }
            }
        }
    }
}
