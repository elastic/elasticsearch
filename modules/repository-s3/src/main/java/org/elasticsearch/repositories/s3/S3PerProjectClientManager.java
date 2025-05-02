/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class S3PerProjectClientManager implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(S3PerProjectClientManager.class);

    private final Settings settings;
    private final Function<S3ClientSettings, AmazonS3Reference> clientBuilder;
    private final Executor executor;
    // A map of projectId to clients holder. Adding to and removing from the map happen only in the cluster state listener thread.
    private final Map<ProjectId, ClientsHolder> projectClientsHolders;
    // Listener for tracking ongoing async closing of obsolete clients. Updated only in the cluster state listener thread.
    private volatile SubscribableListener<Void> clientsCloseListener = null;

    S3PerProjectClientManager(Settings settings, Function<S3ClientSettings, AmazonS3Reference> clientBuilder, Executor executor) {
        this.settings = settings;
        this.clientBuilder = clientBuilder;
        this.executor = executor;
        this.projectClientsHolders = new ConcurrentHashMap<>();
    }

    // visible for tests
    Map<ProjectId, ClientsHolder> getProjectClientsHolders() {
        return Map.copyOf(projectClientsHolders);
    }

    public void clusterChanged(ClusterChangedEvent event) {
        final Map<ProjectId, ProjectMetadata> currentProjects = event.state().metadata().projects();

        final var updatedPerProjectClients = new HashMap<ProjectId, ClientsHolder>();
        for (var project : currentProjects.values()) {
            final ProjectSecrets projectSecrets = project.custom(ProjectSecrets.TYPE);
            if (projectSecrets == null) {
                // This can only happen when a node restarts, it will be processed again when file settings are loaded
                continue;
            }
            final Settings currentSettings = Settings.builder()
                // merge with static settings such as max retries etc, exclude secure settings
                // TODO: We may need to update this if per-project settings decide to support hierarchical overrides
                .put(settings, false) // do not fallback to cluster scoped secrets
                .setSecureSettings(projectSecrets.getSettings())
                .build();
            final Map<String, S3ClientSettings> clientSettings = S3ClientSettings.load(currentSettings)
                .entrySet()
                .stream()
                // Skip project clients that have no credentials configured. This should not happen in serverless since all clients should
                // have credentials configured. But it is safer to skip them.
                .filter(entry -> entry.getValue().credentials != null)
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            // TODO: clientSettings should not be empty, i.e. there should be at least one client configured
            // Maybe log a warning if it is empty and continue. The project will not have usable client but that is probably ok.

            // TODO: Building and comparing the whole S3ClientSettings may be inefficient, we could just compare the relevant secrets
            if (newOrUpdated(project.id(), clientSettings)) {
                updatedPerProjectClients.put(project.id(), new ClientsHolder(clientSettings));
            }
        }

        final List<ClientsHolder> clientsHoldersToClose = new ArrayList<>();
        // Updated projects
        for (var projectId : updatedPerProjectClients.keySet()) {
            final var old = projectClientsHolders.put(projectId, updatedPerProjectClients.get(projectId));
            if (old != null) {
                clientsHoldersToClose.add(old);
            }
        }
        // removed projects
        for (var projectId : projectClientsHolders.keySet()) {
            if (currentProjects.containsKey(projectId) == false) {
                final var removed = projectClientsHolders.remove(projectId);
                assert removed != null;
                clientsHoldersToClose.add(removed);
            }
        }
        // Close stale clients asynchronously without blocking the cluster state thread
        if (clientsHoldersToClose.isEmpty() == false) {
            final var currentClientsCloseListener = new SubscribableListener<Void>();
            final var previousClientsCloseListener = clientsCloseListener;
            clientsCloseListener = currentClientsCloseListener;
            if (previousClientsCloseListener != null && previousClientsCloseListener.isDone() == false) {
                previousClientsCloseListener.addListener(
                    ActionListener.running(() -> closeClientsAsync(clientsHoldersToClose, currentClientsCloseListener))
                );
            } else {
                closeClientsAsync(clientsHoldersToClose, currentClientsCloseListener);
            }
        }
    }

    private void closeClientsAsync(List<ClientsHolder> clientsHoldersToClose, ActionListener<Void> listener) {
        executor.execute(() -> {
            IOUtils.closeWhileHandlingException(clientsHoldersToClose);
            listener.onResponse(null);
        });
    }

    public AmazonS3Reference client(ProjectId projectId, RepositoryMetadata repositoryMetadata) {
        final var clientsHolder = projectClientsHolders.get(projectId);
        if (clientsHolder == null) {
            throw new IllegalArgumentException("project [" + projectId + "] does not exist");
        }
        final String clientName = S3Repository.CLIENT_NAME.get(repositoryMetadata.settings());
        return clientsHolder.client(clientName);
    }

    /**
     * Similar to S3Service#releaseCachedClients but only clears the cache for the given project.
     * All clients for the project are closed and will be recreated on next access, also similar to S3Service#releaseCachedClients
     */
    public void releaseProjectClients(ProjectId projectId) {
        final var old = projectClientsHolders.get(projectId);
        if (old != null) {
            old.clearCache();
        }
    }

    /**
     * Shutdown the manager by closing all clients holders. This is called when the node is shutting down.
     * It attempts to wait (1 min) for any async client closing to complete.
     */
    public void close() {
        IOUtils.closeWhileHandlingException(projectClientsHolders.values());
        final var currentClientsCloseListener = clientsCloseListener;
        if (currentClientsCloseListener != null && currentClientsCloseListener.isDone() == false) {
            // Wait for async clients closing to be completed
            final CountDownLatch latch = new CountDownLatch(1);
            currentClientsCloseListener.addListener(ActionListener.running(latch::countDown));
            try {
                if (latch.await(1, TimeUnit.MINUTES) == false) {
                    logger.warn("async closing of s3 clients timed out");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // visible for tests
    @Nullable
    SubscribableListener<Void> getClientsCloseListener() {
        return clientsCloseListener;
    }

    private boolean newOrUpdated(ProjectId projectId, Map<String, S3ClientSettings> currentClientSettings) {
        final var old = projectClientsHolders.get(projectId);
        if (old == null) {
            return true;
        }
        return currentClientSettings.equals(old.clientSettings()) == false;
    }

    /**
     * Holder class of s3 clients for a single project. It is instantiated in the cluster state thread with client
     * settings. The clients are created and cached lazily when the {@link #client(String)} method is called.
     * Cached clients are closed and cleared out when the {@link #clearCache()} method is called. Subsequent calls to
     * {@link #client(String)} will recreate them. The call to {@link #close()} method clears the cache as well but
     * also flags the holder to be closed so that no new clients can be created.
     */
    final class ClientsHolder implements Closeable {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Map<String, S3ClientSettings> clientSettings;
        // Client name -> client reference
        private volatile Map<String, AmazonS3Reference> clientsCache = Collections.emptyMap();

        ClientsHolder(Map<String, S3ClientSettings> clientSettings) {
            this.clientSettings = clientSettings;
        }

        Map<String, S3ClientSettings> clientSettings() {
            return clientSettings;
        }

        AmazonS3Reference client(String clientName) {
            final var clientReference = clientsCache.get(clientName);
            // It is ok to retrieve an existing client when the cache is being cleared or the holder is closing.
            // As long as there are paired incRef/decRef calls, the client will be closed when the last reference is released
            // by either the caller of this method or the clearCache() method.
            if (clientReference != null && clientReference.tryIncRef()) {
                return clientReference;
            }
            final var settings = clientSettings.get(clientName);
            if (settings == null) {
                throw new IllegalArgumentException("client [" + clientName + "] does not exist");
            }
            synchronized (this) {
                final var existing = clientsCache.get(clientName);
                if (existing != null && existing.tryIncRef()) {
                    return existing;
                }
                if (closed.get()) {
                    // Not adding a new client once the manager is closed since there won't be anything to close it
                    throw new IllegalStateException("client manager is closed");
                }
                // The close() method maybe called after we checked it, it is ok since we are already inside the synchronized block.
                // The clearCache() will clear the newly added client.
                final var newClientReference = clientBuilder.apply(settings);
                clientsCache = Maps.copyMapWithAddedEntry(clientsCache, clientName, newClientReference);
                return newClientReference;
            }
        }

        /**
         * Clear the cache by closing and clear out all clients. Subsequent {@link #client(String)} calls will recreate
         * the clients and populate the cache again.
         */
        synchronized void clearCache() {
            IOUtils.closeWhileHandlingException(clientsCache.values());
            clientsCache = Collections.emptyMap();
        }

        /**
         * Similar to {@link #clearCache()} but also flag the holder to be closed so that no new client can be created.
         */
        public void close() {
            if (closed.compareAndSet(false, true)) {
                clearCache();
            }
        }

        // visible for tests
        boolean isClosed() {
            return closed.get();
        }
    }
}
