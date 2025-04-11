/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class GcsPerProjectClientManager implements ClusterStateListener {

    private final Settings settings;
    private final BiFunction<GoogleCloudStorageClientSettings, GcsRepositoryStatsCollector, MeteredStorage> clientBuilder;
    private final Map<ProjectId, ClientHolder> perProjectClientsCache;

    public GcsPerProjectClientManager(
        Settings settings,
        BiFunction<GoogleCloudStorageClientSettings, GcsRepositoryStatsCollector, MeteredStorage> clientBuilder
    ) {
        this.settings = settings;
        this.clientBuilder = clientBuilder;
        this.perProjectClientsCache = new ConcurrentHashMap<>();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final Map<ProjectId, ProjectMetadata> currentProjects = event.state().metadata().projects();

        final var updatedPerProjectClients = new HashMap<ProjectId, ClientHolder>();
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
            final Map<String, GoogleCloudStorageClientSettings> clientSettings = GoogleCloudStorageClientSettings.load(currentSettings);

            // TODO: clientSettings should not be empty, i.e. there should be at least one client configured
            // Maybe log a warning if it is empty and continue. The project will not have usable client but that is probably ok.

            // TODO: Building and comparing the whole GoogleCloudStorageClientSettings may be insufficient, we could just compare the
            // relevant secrets
            if (newOrUpdated(project.id(), clientSettings)) {
                updatedPerProjectClients.put(project.id(), new ClientHolder(clientSettings));
            }
        }
        // Updated projects
        perProjectClientsCache.putAll(updatedPerProjectClients);

        // removed projects
        for (var projectId : perProjectClientsCache.keySet()) {
            if (currentProjects.containsKey(projectId) == false) {
                perProjectClientsCache.remove(projectId);
            }
        }
    }

    public MeteredStorage client(
        ProjectId projectId,
        String clientName,
        String repositoryName,
        GcsRepositoryStatsCollector statsCollector
    ) {
        final var clientHolder = perProjectClientsCache.get(projectId);
        if (clientHolder == null) {
            throw new IllegalArgumentException("No project found for [" + projectId + "]");
        }
        return clientHolder.client(clientName, repositoryName, statsCollector);
    }

    public void closeRepositoryClients(ProjectId projectId, String repositoryName) {
        final var clientHolder = perProjectClientsCache.get(projectId);
        if (clientHolder != null) {
            clientHolder.closeRepositoryClients(repositoryName);
        }
    }

    private boolean newOrUpdated(ProjectId projectId, Map<String, GoogleCloudStorageClientSettings> currentClientSettings) {
        if (perProjectClientsCache.containsKey(projectId) == false) {
            return true;
        }
        final var previousClientSettings = perProjectClientsCache.get(projectId).getClientSettings();
        return currentClientSettings.equals(previousClientSettings) == false;
    }

    private final class ClientHolder {
        // clientName -> client settings
        private final Map<String, GoogleCloudStorageClientSettings> clientSettings;
        // repositoryName -> client
        private final Map<String, MeteredStorage> clientCache = new ConcurrentHashMap<>();

        ClientHolder(Map<String, GoogleCloudStorageClientSettings> clientSettings) {
            this.clientSettings = clientSettings;
        }

        public Map<String, GoogleCloudStorageClientSettings> getClientSettings() {
            return clientSettings;
        }

        MeteredStorage client(String clientName, String repositoryName, GcsRepositoryStatsCollector statsCollector) {
            return clientCache.computeIfAbsent(repositoryName, ignored -> {
                final var settings = clientSettings.get(clientName);
                if (settings == null) {
                    throw new IllegalArgumentException("No client settings found for [" + clientName + "]");
                }
                return clientBuilder.apply(settings, statsCollector);
            });
        }

        void closeRepositoryClients(String repositoryName) {
            clientCache.remove(repositoryName);
        }
    }
}
