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
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.core.Strings.format;

public class GoogleCloudStorageClientsManager implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageClientsManager.class);
    private static final String GCS_SETTING_PREFIX = "gcs.";

    private final Settings nodeGcsSettings;
    private final CheckedBiFunction<
        GoogleCloudStorageClientSettings,
        GcsRepositoryStatsCollector,
        MeteredStorage,
        IOException> clientBuilder;
    // A map of projectId to clients holder. Adding to and removing from the map happen only in the applier thread.
    private final Map<ProjectId, ClientsHolder> perProjectClientsHolders;
    private final ClusterClientsHolder clusterClientsHolder;

    public GoogleCloudStorageClientsManager(
        Settings nodeSettings,
        CheckedBiFunction<GoogleCloudStorageClientSettings, GcsRepositoryStatsCollector, MeteredStorage, IOException> clientBuilder,
        boolean supportsMultipleProjects
    ) {
        this.nodeGcsSettings = Settings.builder()
            .put(nodeSettings.getByPrefix(GCS_SETTING_PREFIX), false) // not rely on any cluster scoped secrets
            .normalizePrefix(GCS_SETTING_PREFIX)
            .build();
        this.clientBuilder = clientBuilder;
        if (supportsMultipleProjects) {
            this.perProjectClientsHolders = ConcurrentCollections.newConcurrentMap();
        } else {
            this.perProjectClientsHolders = null;
        }
        this.clusterClientsHolder = new ClusterClientsHolder();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        assert perProjectClientsHolders != null;
        final Map<ProjectId, ProjectMetadata> currentProjects = event.state().metadata().projects();

        final var updatedPerProjectClients = new HashMap<ProjectId, ClientsHolder>();
        for (var project : currentProjects.values()) {
            // Skip the default project, it is tracked separately with clusterClientsHolder and
            // updated differently with the ReloadablePlugin interface
            if (ProjectId.DEFAULT.equals(project.id())) {
                continue;
            }
            final ProjectSecrets projectSecrets = project.custom(ProjectSecrets.TYPE);
            // Project secrets can be null when node restarts. It may not have any GCS credentials if GCS is not in use.
            if (projectSecrets == null || projectSecrets.getSettingNames().stream().noneMatch(key -> key.startsWith(GCS_SETTING_PREFIX))) {
                // Most likely there won't be any existing client, but attempt to remove it anyway just in case
                perProjectClientsHolders.remove(project.id());
                continue;
            }

            final Settings currentSettings = Settings.builder()
                // merge with static settings such as max retries etc
                // TODO: https://elasticco.atlassian.net/browse/ES-11716 Consider change this to use per-project settings
                .put(nodeGcsSettings)
                .setSecureSettings(projectSecrets.getSettings())
                .build();

            final var allClientSettings = GoogleCloudStorageClientSettings.load(currentSettings);
            assert allClientSettings.isEmpty() == false;
            // Skip project clients that have no credentials configured. This should not happen in serverless.
            // But it is safer to skip them and is also a more consistent behaviour with the cases when
            // project secrets are not present.
            final var clientSettings = allClientSettings.entrySet()
                .stream()
                .filter(entry -> entry.getValue().getCredential() != null)
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            if (allClientSettings.size() != clientSettings.size()) {
                logger.warn(
                    "Project [{}] has [{}] GCS client settings, but [{}] is usable due to missing credentials for clients {}",
                    project.id(),
                    allClientSettings.size(),
                    clientSettings.size(),
                    Sets.difference(allClientSettings.keySet(), clientSettings.keySet())
                );
            }

            // TODO: If performance is an issue, we may consider comparing just the relevant project secrets for new or updated clients
            // and avoid building the clientSettings
            if (newOrUpdated(project.id(), clientSettings)) {
                updatedPerProjectClients.put(project.id(), new PerProjectClientsHolder(clientSettings));
            }
        }

        // Updated projects
        for (var projectId : updatedPerProjectClients.keySet()) {
            assert ProjectId.DEFAULT.equals(projectId) == false;
            perProjectClientsHolders.put(projectId, updatedPerProjectClients.get(projectId));
        }
        // Removed projects
        for (var projectId : perProjectClientsHolders.keySet()) {
            if (currentProjects.containsKey(projectId) == false) {
                assert ProjectId.DEFAULT.equals(projectId) == false;
                perProjectClientsHolders.remove(projectId);
            }
        }
    }

    void refreshAndClearCacheForClusterClients(Map<String, GoogleCloudStorageClientSettings> clientsSettings) {
        clusterClientsHolder.refreshAndClearCache(clientsSettings);
    }

    MeteredStorage client(ProjectId projectId, String clientName, String repositoryName, GcsRepositoryStatsCollector statsCollector)
        throws IOException {
        if (projectId == null || ProjectId.DEFAULT.equals(projectId)) {
            return clusterClientsHolder.client(clientName, repositoryName, statsCollector);
        } else {
            return getClientsHolderSafe(projectId).client(clientName, repositoryName, statsCollector);
        }
    }

    void closeRepositoryClients(ProjectId projectId, String repositoryName) {
        if (projectId == null || ProjectId.DEFAULT.equals(projectId)) {
            clusterClientsHolder.closeRepositoryClients(repositoryName);
        } else {
            final var old = perProjectClientsHolders.get(projectId);
            if (old != null) {
                old.closeRepositoryClients(repositoryName);
            }
        }
    }

    // package private for tests
    ClusterClientsHolder getClusterClientsHolder() {
        return clusterClientsHolder;
    }

    // package private for tests
    Map<ProjectId, ClientsHolder> getPerProjectClientsHolders() {
        return perProjectClientsHolders == null ? null : Map.copyOf(perProjectClientsHolders);
    }

    private boolean newOrUpdated(ProjectId projectId, Map<String, GoogleCloudStorageClientSettings> currentClientSettings) {
        final var old = perProjectClientsHolders.get(projectId);
        if (old == null) {
            return true;
        }
        return currentClientSettings.equals(old.allClientSettings()) == false;
    }

    private ClientsHolder getClientsHolderSafe(ProjectId projectId) {
        assert ProjectId.DEFAULT.equals(projectId) == false;
        final var clientsHolder = perProjectClientsHolders.get(projectId);
        if (clientsHolder == null) {
            throw new IllegalArgumentException("No GCS client is configured for project [" + projectId + "]");
        }
        return clientsHolder;
    }

    abstract class ClientsHolder {

        /**
         * Dictionary of client instances. Client instances are built lazily from the
         * latest settings. Clients are cached by a composite repositoryName key.
         */
        protected volatile Map<String, MeteredStorage> clientCache = emptyMap();

        /**
         * Get the current client settings for all clients in this holder.
         */
        protected abstract Map<String, GoogleCloudStorageClientSettings> allClientSettings();

        /**
         * Attempts to retrieve a client from the cache. If the client does not exist it
         * will be created from the latest settings and will populate the cache. The
         * returned instance should not be cached by the calling code. Instead, for each
         * use, the (possibly updated) instance should be requested by calling this
         * method.
         *
         * @param clientName name of the client settings used to create the client
         * @param repositoryName name of the repository that would use the client
         * @return a cached client storage instance that can be used to manage objects
         *         (blobs)
         */
        MeteredStorage client(final String clientName, final String repositoryName, final GcsRepositoryStatsCollector statsCollector)
            throws IOException {
            {
                final MeteredStorage storage = clientCache.get(repositoryName);
                if (storage != null) {
                    return storage;
                }
            }
            synchronized (this) {
                final MeteredStorage existing = clientCache.get(repositoryName);

                if (existing != null) {
                    return existing;
                }

                final GoogleCloudStorageClientSettings settings = allClientSettings().get(clientName);

                if (settings == null) {
                    throw new IllegalArgumentException(
                        "Unknown client name [" + clientName + "]. Existing client configs: " + allClientSettings().keySet()
                    );
                }

                logger.debug(() -> format("creating GCS client with client_name [%s], endpoint [%s]", clientName, settings.getHost()));
                final MeteredStorage storage = clientBuilder.apply(settings, statsCollector);
                clientCache = Maps.copyMapWithAddedEntry(clientCache, repositoryName, storage);
                return storage;
            }
        }

        synchronized void closeRepositoryClients(String repositoryName) {
            clientCache = clientCache.entrySet()
                .stream()
                .filter(entry -> entry.getKey().equals(repositoryName) == false)
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        // package private for tests
        final boolean hasCachedClientForRepository(String repositoryName) {
            return clientCache.containsKey(repositoryName);
        }
    }

    final class ClusterClientsHolder extends ClientsHolder {

        private volatile Map<String, GoogleCloudStorageClientSettings> clientSettings = emptyMap();

        @Override
        protected Map<String, GoogleCloudStorageClientSettings> allClientSettings() {
            return clientSettings;
        }

        /**
         * Refreshes the client settings and clears the client cache. Subsequent calls to
         * {@code GoogleCloudStorageService#client} will return new clients constructed
         * using the parameter settings.
         *
         * @param clientsSettings the new settings used for building clients for subsequent requests
         */
        synchronized void refreshAndClearCache(Map<String, GoogleCloudStorageClientSettings> clientsSettings) {
            this.clientCache = emptyMap();
            this.clientSettings = Maps.ofEntries(clientsSettings.entrySet());
        }
    }

    final class PerProjectClientsHolder extends ClientsHolder {

        private final Map<String, GoogleCloudStorageClientSettings> clientSettings;

        PerProjectClientsHolder(Map<String, GoogleCloudStorageClientSettings> clientSettings) {
            this.clientSettings = clientSettings;
        }

        @Override
        protected Map<String, GoogleCloudStorageClientSettings> allClientSettings() {
            return clientSettings;
        }
    }
}
