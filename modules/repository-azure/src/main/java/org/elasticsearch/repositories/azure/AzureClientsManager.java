/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

class AzureClientsManager implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(AzureClientsManager.class);
    private static final String AZURE_SETTING_PREFIX = "azure.";

    private final Settings nodeAzureSettings;
    private final AzureClientBuilder azureClientBuilder;

    volatile Map<String, AzureStorageSettings> clusterStorageSettings = emptyMap();
    private final Map<ProjectId, Map<String, AzureStorageSettings>> perProjectStorageSettings;

    AzureClientsManager(Settings nodeSettings, AzureClientBuilder azureClientBuilder, boolean supportsMultipleProjects) {
        this.nodeAzureSettings = Settings.builder()
            .put(nodeSettings.getByPrefix(AZURE_SETTING_PREFIX), false) // not rely on any cluster scoped secrets
            .normalizePrefix(AZURE_SETTING_PREFIX)
            .build();
        this.azureClientBuilder = azureClientBuilder;
        if (supportsMultipleProjects) {
            this.perProjectStorageSettings = ConcurrentCollections.newConcurrentMap();
        } else {
            this.perProjectStorageSettings = null;
        }
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        assert perProjectStorageSettings != null;
        final Map<ProjectId, ProjectMetadata> currentProjects = event.state().metadata().projects();

        for (var project : currentProjects.values()) {
            // Skip the default project, it is tracked separately with clusterStorageSettings and
            // updated differently with the ReloadablePlugin interface
            if (ProjectId.DEFAULT.equals(project.id())) {
                continue;
            }
            final ProjectSecrets projectSecrets = project.custom(ProjectSecrets.TYPE);
            // Project secrets can be null when node restarts. It may not have any azure credentials if azure is not in use.
            if (projectSecrets == null
                || projectSecrets.getSettingNames().stream().noneMatch(key -> key.startsWith(AZURE_SETTING_PREFIX))) {
                // Most likely there won't be any existing client settings, but attempt to remove it anyway just in case
                perProjectStorageSettings.remove(project.id());
                continue;
            }

            final Settings currentSettings = Settings.builder()
                // merge with static settings such as max retries etc
                // TODO: https://elasticco.atlassian.net/browse/ES-11716 Consider change this to use per-project settings
                .put(nodeAzureSettings)
                .setSecureSettings(projectSecrets.getSettings())
                .build();
            final Map<String, AzureStorageSettings> clientSettings = AzureStorageSettings.load(currentSettings)
                .entrySet()
                .stream()
                // Skip project client settings that have no credentials configured. This should not happen in serverless.
                // But it is safer to skip them and is also a more consistent behaviour with the cases when
                // project secrets are not present.
                .filter(entry -> entry.getValue().hasCredentials())
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
                perProjectStorageSettings.put(project.id(), clientSettings);
            }
        }

        // Removed projects
        for (var projectId : perProjectStorageSettings.keySet()) {
            if (currentProjects.containsKey(projectId) == false) {
                assert ProjectId.DEFAULT.equals(projectId) == false;
                perProjectStorageSettings.remove(projectId);
            }
        }
    }

    @Deprecated(forRemoval = true)
    public AzureBlobServiceClient client(
        String clientName,
        LocationMode locationMode,
        OperationPurpose purpose,
        AzureClientProvider.RequestMetricsHandler requestMetricsHandler
    ) {
        final AzureStorageSettings azureStorageSettings = getClientSettings(clientName);
        return azureClientBuilder.buildClient(azureStorageSettings, locationMode, purpose, requestMetricsHandler);
    }

    public AzureBlobServiceClient client(
        ProjectId projectId,
        String clientName,
        LocationMode locationMode,
        OperationPurpose purpose,
        AzureClientProvider.RequestMetricsHandler requestMetricsHandler
    ) {
        final AzureStorageSettings azureStorageSettings;
        if (projectId == null || ProjectId.DEFAULT.equals(projectId)) {
            azureStorageSettings = getClientSettings(clientName);
        } else {
            final Map<String, AzureStorageSettings> storageSettings = perProjectStorageSettings.get(projectId);
            if (storageSettings == null) {
                throw new SettingsException("Unable to find client settings for project [" + projectId + "]");
            }
            azureStorageSettings = storageSettings.get(clientName);
            if (azureStorageSettings == null) {
                throw new SettingsException("Unable to find client with name [" + clientName + "] for project [" + projectId + "]");
            }
        }
        return azureClientBuilder.buildClient(azureStorageSettings, locationMode, purpose, requestMetricsHandler);
    }

    private AzureStorageSettings getClientSettings(String clientName) {
        final AzureStorageSettings azureStorageSettings = this.clusterStorageSettings.get(clientName);
        if (azureStorageSettings == null) {
            throw new SettingsException("Unable to find client with name [" + clientName + "]");
        }
        return azureStorageSettings;
    }

    private boolean newOrUpdated(ProjectId projectId, Map<String, AzureStorageSettings> currentClientSettings) {
        final var old = perProjectStorageSettings.get(projectId);
        if (old == null) {
            return true;
        }
        return currentClientSettings.equals(old) == false;
    }

    @FunctionalInterface
    interface AzureClientBuilder {
        AzureBlobServiceClient buildClient(
            AzureStorageSettings azureStorageSettings,
            LocationMode locationMode,
            OperationPurpose purpose,
            AzureClientProvider.RequestMetricsHandler requestMetricsHandler
        );
    }
}
