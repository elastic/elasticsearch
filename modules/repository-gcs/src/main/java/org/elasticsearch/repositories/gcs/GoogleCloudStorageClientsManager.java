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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<ProjectId, ClientsHolder> clientHolders;

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
            // If multiple projects are supported, we need to track per-project clients
            this.clientHolders = new ConcurrentHashMap<>(Map.of(ProjectId.DEFAULT, new ClientsHolder()));
        } else {
            // If only a single project is supported, we use a single holder for the default project
            this.clientHolders = Map.of(ProjectId.DEFAULT, new ClientsHolder());
        }
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        final Map<ProjectId, ProjectMetadata> currentProjects = event.state().metadata().projects();

        final var updatedPerProjectClients = new HashMap<ProjectId, ClientsHolder>();
        for (var project : currentProjects.values()) {
            // Skip the default project, it is tracked separately with clusterClientsHolder and
            // updated differently with the ReloadablePlugin interface
            if (ProjectId.DEFAULT.equals(project.id())) {
                continue;
            }
            final ProjectSecrets projectSecrets = project.custom(ProjectSecrets.TYPE);
            // Project secrets can be null when node restarts. It may not have any s3 credentials if s3 is not in use.
            if (projectSecrets == null || projectSecrets.getSettingNames().stream().noneMatch(key -> key.startsWith(GCS_SETTING_PREFIX))) {
                // Most likely there won't be any existing client, but attempt to remove it anyway just in case
                clientHolders.remove(project.id());
                continue;
            }

            final Settings currentSettings = Settings.builder()
                // merge with static settings such as max retries etc
                // TODO: https://elasticco.atlassian.net/browse/ES-11716 Consider change this to use per-project settings
                .put(nodeGcsSettings)
                .setSecureSettings(projectSecrets.getSettings())
                .build();
            final Map<String, GoogleCloudStorageClientSettings> clientSettings = GoogleCloudStorageClientSettings.load(currentSettings)
                .entrySet()
                .stream()
                // Skip project clients that have no credentials configured. This should not happen in serverless.
                // But it is safer to skip them and is also a more consistent behaviour with the cases when
                // project secrets are not present.
                .filter(entry -> entry.getValue().getCredential() != null)
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            if (clientSettings.isEmpty()) {
                // clientSettings should not be empty, i.e. there should be at least one client configured.
                // But if it does somehow happen, log a warning and continue. The project will not have usable client but that is ok.
                logger.warn("Skipping project [{}] with no client settings", project.id());
                continue;
            }

            // TODO: If performance is an issue, we may consider comparing just the relevant project secrets for new or updated clients
            // and avoid building the clientSettings
            if (newOrUpdated(project.id(), clientSettings)) {
                updatedPerProjectClients.put(project.id(), new ClientsHolder());
            }
        }

        // Updated projects
        for (var projectId : updatedPerProjectClients.keySet()) {
            assert ProjectId.DEFAULT.equals(projectId) == false;
            clientHolders.put(projectId, updatedPerProjectClients.get(projectId));
        }
        // Removed projects
        for (var projectId : clientHolders.keySet()) {
            if (currentProjects.containsKey(projectId) == false) {
                assert ProjectId.DEFAULT.equals(projectId) == false;
                clientHolders.remove(projectId);
            }
        }
    }

    @Deprecated(forRemoval = true)
    MeteredStorage client(String clientName, String repositoryName, GcsRepositoryStatsCollector statsCollector) throws IOException {
        return client(ProjectId.DEFAULT, clientName, repositoryName, statsCollector);
    }

    @Deprecated(forRemoval = true)
    void refreshAndClearCache(Map<String, GoogleCloudStorageClientSettings> clientsSettings) {
        refreshAndClearCache(ProjectId.DEFAULT, clientsSettings);
    }

    @Deprecated(forRemoval = true)
    void closeRepositoryClients(String repositoryName) {
        closeRepositoryClients(ProjectId.DEFAULT, repositoryName);
    }

    MeteredStorage client(ProjectId projectId, String clientName, String repositoryName, GcsRepositoryStatsCollector statsCollector)
        throws IOException {
        return getClientsHolderSafe(projectId).client(clientName, repositoryName, statsCollector);
    }

    void refreshAndClearCache(ProjectId projectId, Map<String, GoogleCloudStorageClientSettings> clientsSettings) {
        getClientsHolderSafe(projectId).refreshAndClearCache(clientsSettings);
    }

    void closeRepositoryClients(ProjectId projectId, String repositoryName) {
        getClientsHolderSafe(projectId).closeRepositoryClients(repositoryName);
    }

    private boolean newOrUpdated(ProjectId projectId, Map<String, GoogleCloudStorageClientSettings> currentClientSettings) {
        final var old = clientHolders.get(projectId);
        if (old == null) {
            return true;
        }
        return currentClientSettings.equals(old.clientSettings) == false;
    }

    private ClientsHolder getClientsHolderSafe(ProjectId projectId) {
        final var clientsHolder = clientHolders.get(projectId);
        if (clientsHolder == null) {
            assert ProjectId.DEFAULT.equals(projectId) == false;
            throw new IllegalArgumentException("No GCS client is configured for project [" + projectId + "]");
        }
        return clientsHolder;
    }

    class ClientsHolder {

        private volatile Map<String, GoogleCloudStorageClientSettings> clientSettings = emptyMap();
        /**
         * Dictionary of client instances. Client instances are built lazily from the
         * latest settings. Clients are cached by a composite repositoryName key.
         */
        private volatile Map<String, MeteredStorage> clientCache = emptyMap();

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

                final GoogleCloudStorageClientSettings settings = clientSettings.get(clientName);

                if (settings == null) {
                    throw new IllegalArgumentException(
                        "Unknown client name ["
                            + clientName
                            + "]. Existing client configs: "
                            + Strings.collectionToDelimitedString(clientSettings.keySet(), ",")
                    );
                }

                logger.debug(() -> format("creating GCS client with client_name [%s], endpoint [%s]", clientName, settings.getHost()));
                final MeteredStorage storage = clientBuilder.apply(settings, statsCollector);
                clientCache = Maps.copyMapWithAddedEntry(clientCache, repositoryName, storage);
                return storage;
            }
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

        synchronized void closeRepositoryClients(String repositoryName) {
            clientCache = clientCache.entrySet()
                .stream()
                .filter(entry -> entry.getKey().equals(repositoryName) == false)
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }
}
