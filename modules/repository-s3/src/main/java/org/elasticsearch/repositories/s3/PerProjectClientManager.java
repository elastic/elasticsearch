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
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PerProjectClientManager implements ClusterStateListener {

    // Original settings at node startup time
    private final Settings settings;
    private final Function<S3ClientSettings, AmazonS3> clientBuilder;

    // A map of per-project clients, where the key is the project ID and the value is a map of client name to client
    private final Map<ProjectId, Map<String, ClientHolder>> perProjectClientsCache;

    public PerProjectClientManager(Settings settings, Function<S3ClientSettings, AmazonS3> clientBuilder) {
        this.settings = settings;
        this.clientBuilder = clientBuilder;
        this.perProjectClientsCache = new ConcurrentHashMap<>();
    }

    public void clusterChanged(ClusterChangedEvent event) {
        final Map<ProjectId, ProjectMetadata> currentProjects = event.state().metadata().projects();

        final var updatedPerProjectClients = new HashMap<ProjectId, Map<String, ClientHolder>>();
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
                updatedPerProjectClients.put(
                    project.id(),
                    clientSettings.entrySet()
                        .stream()
                        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> new ClientHolder(entry.getValue())))
                );
            }
        }
        // Updated projects
        for (var projectId : updatedPerProjectClients.keySet()) {
            final Map<String, ClientHolder> old = perProjectClientsCache.put(projectId, updatedPerProjectClients.get(projectId));
            if (old != null) {
                IOUtils.closeWhileHandlingException(old.values());
            }
        }

        // removed projects
        for (var projectId : perProjectClientsCache.keySet()) {
            if (currentProjects.containsKey(projectId) == false) {
                final Map<String, ClientHolder> removed = perProjectClientsCache.remove(projectId);
                assert removed != null;
                IOUtils.closeWhileHandlingException(removed.values());
            }
        }
    }

    public AmazonS3Reference client(ProjectId projectId, RepositoryMetadata repositoryMetadata) {
        if (perProjectClientsCache.containsKey(projectId) == false) {
            throw new IllegalArgumentException("project [" + projectId + "] does not exist");
        }
        final String clientName = S3Repository.CLIENT_NAME.get(repositoryMetadata.settings());
        final Map<String, ClientHolder> clientHolders = perProjectClientsCache.get(projectId);

        if (clientHolders.containsKey(clientName) == false) {
            throw new IllegalArgumentException("client [" + clientName + "] does not exist");
        }

        return clientHolders.get(clientName).client();
    }

    /**
     * Similar to S3Service#releaseCachedClients but only clears the cache for the given project.
     * All clients for the project are closed and will be recreated on next access. Also, similar to S3Service#releaseCachedClients
     */
    public void clearCacheForProject(ProjectId projectId) {
        final Map<String, ClientHolder> old = perProjectClientsCache.get(projectId);
        assert old != null : projectId;
        IOUtils.closeWhileHandlingException(old.values());
        perProjectClientsCache.put(
            projectId,
            old.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> new ClientHolder(entry.getValue().clientSettings())))
        );
        // TODO: do we need this?
        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }

    public void close() {
        for (var clientHolders : perProjectClientsCache.values()) {
            IOUtils.closeWhileHandlingException(clientHolders.values());
        }
        perProjectClientsCache.clear();
    }

    private boolean newOrUpdated(ProjectId projectId, Map<String, S3ClientSettings> currentClientSettings) {
        if (perProjectClientsCache.containsKey(projectId) == false) {
            return true;
        }
        final var previousClientSettings = perProjectClientsCache.get(projectId)
            .entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().clientSettings()));
        return currentClientSettings.equals(previousClientSettings) == false;
    }

    private final class ClientHolder implements Closeable {
        private final S3ClientSettings clientSettings;
        private final CachedSupplier<AmazonS3Reference> client;

        ClientHolder(S3ClientSettings clientSettings) {
            this.clientSettings = clientSettings;
            this.client = CachedSupplier.wrap(() -> new AmazonS3Reference(clientBuilder.apply(clientSettings)));
        }

        public S3ClientSettings clientSettings() {
            return clientSettings;
        }

        public AmazonS3Reference client() {
            return client.get();
        }

        public void close() {
            client.get().decRef();
        }
    }
}
