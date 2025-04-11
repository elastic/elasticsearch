/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.s3.AmazonS3;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class S3PerProjectClientManager implements ClusterStateListener {

    // Original settings at node startup time
    private final Settings settings;
    private final Function<S3ClientSettings, AmazonS3> clientBuilder;

    // A map of projectId to clients holder. Adding to and removing from the map happen only with the cluster state listener thread.
    private final Map<ProjectId, ClientsHolder> perProjectClientsCache;

    public S3PerProjectClientManager(Settings settings, Function<S3ClientSettings, AmazonS3> clientBuilder) {
        this.settings = settings;
        this.clientBuilder = clientBuilder;
        this.perProjectClientsCache = new ConcurrentHashMap<>();
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
                .put(settings, false)
                .setSecureSettings(projectSecrets.getSettings())
                .build();
            final Map<String, S3ClientSettings> clientSettings = S3ClientSettings.load(currentSettings);

            // TODO: clientSettings should not be empty, i.e. there should be at least one client configured
            // Maybe log a warning if it is empty and continue. The project will not have usable client but that is probably ok.

            // TODO: Building and comparing the whole S3ClientSettings may be insufficient, we could just compare the relevant secrets
            if (newOrUpdated(project.id(), clientSettings)) {
                updatedPerProjectClients.put(project.id(), new ClientsHolder(clientSettings));
            }
        }

        // Updated projects
        for (var projectId : updatedPerProjectClients.keySet()) {
            final var old = perProjectClientsCache.put(projectId, updatedPerProjectClients.get(projectId));
            if (old != null) {
                old.close();
            }
        }

        // removed projects
        for (var projectId : perProjectClientsCache.keySet()) {
            if (currentProjects.containsKey(projectId) == false) {
                final var removed = perProjectClientsCache.remove(projectId);
                assert removed != null;
                removed.close();
            }
        }
    }

    public AmazonS3Reference client(ProjectId projectId, RepositoryMetadata repositoryMetadata) {
        final var clientsHolder = perProjectClientsCache.get(projectId);
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
    public void clearCacheForProject(ProjectId projectId) {
        final var old = perProjectClientsCache.get(projectId);
        if (old != null) {
            old.clearCache();
        }
        // TODO: do we need this?
        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }

    /**
     * Shutdown the manager by closing all clients holders. This is called when the node is shutting down.
     */
    public void close() {
        IOUtils.closeWhileHandlingException(perProjectClientsCache.values());
    }

    private boolean newOrUpdated(ProjectId projectId, Map<String, S3ClientSettings> currentClientSettings) {
        final var old = perProjectClientsCache.get(projectId);
        if (old == null) {
            return true;
        }
        final var oldClientSettings = old.clientSettings();
        return currentClientSettings.equals(oldClientSettings) == false;
    }

    private final class ClientsHolder implements Closeable {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Map<String, S3ClientSettings> clientSettings;
        private volatile Map<String, AmazonS3Reference> clientsCache = Collections.emptyMap();

        ClientsHolder(Map<String, S3ClientSettings> clientSettings) {
            this.clientSettings = clientSettings;
        }

        public Map<String, S3ClientSettings> clientSettings() {
            return clientSettings;
        }

        public AmazonS3Reference client(String clientName) {
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
                    // Not adding new client once the manager is closed since there won't be anything to close it
                    throw new IllegalStateException("client manager is closed");
                }
                // The close() method maybe called after we checked it, it is ok since we are already inside the synchronized block.
                // The clearCache() will clear the newly added client.
                final var newClientReference = new AmazonS3Reference(clientBuilder.apply(settings));
                newClientReference.mustIncRef();
                clientsCache = Maps.copyMapWithAddedEntry(clientsCache, clientName, newClientReference);
                return newClientReference;
            }
        }

        /**
         * Clear the cache by closing and clear out all clients. New {@link #client(String)} call will recreate
         * the clients and populate the cache again.
         */
        public synchronized void clearCache() {
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
    }
}
