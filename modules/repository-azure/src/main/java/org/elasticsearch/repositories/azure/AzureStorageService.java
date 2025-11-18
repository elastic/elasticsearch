/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import com.azure.core.http.ProxyOptions;
import com.azure.core.util.logging.ClientLogger;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AzureStorageService {
    private static final Logger logger = LogManager.getLogger(AzureStorageService.class);

    public static final ByteSizeValue MIN_CHUNK_SIZE = ByteSizeValue.ofBytes(1);

    /**
     * The maximum size of a BlockBlob block.
     * See https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
     */
    public static final ByteSizeValue MAX_BLOCK_SIZE = ByteSizeValue.of(100, ByteSizeUnit.MB);

    /**
     * The maximum number of blocks.
     * See https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
     */
    public static final long MAX_BLOCK_NUMBER = 50000;

    /**
     * Default block size for multi-block uploads. The Azure repository will use the Put block and Put block list APIs to split the
     * stream into several part, each of block_size length, and will upload each part in its own request.
     */
    private static final ByteSizeValue DEFAULT_BLOCK_SIZE = ByteSizeValue.ofBytes(
        Math.max(
            ByteSizeUnit.MB.toBytes(5), // minimum value
            Math.min(MAX_BLOCK_SIZE.getBytes(), JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)
        )
    );

    /**
     * The maximum size of a Block Blob.
     * See https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
     */
    public static final long MAX_BLOB_SIZE = MAX_BLOCK_NUMBER * DEFAULT_BLOCK_SIZE.getBytes();

    /**
     * Maximum allowed blob size in Azure blob store.
     */
    public static final ByteSizeValue MAX_CHUNK_SIZE = ByteSizeValue.ofBytes(MAX_BLOB_SIZE);

    private static final long DEFAULT_UPLOAD_BLOCK_SIZE = DEFAULT_BLOCK_SIZE.getBytes();
    private final int multipartUploadMaxConcurrency;

    private final AzureClientProvider azureClientProvider;
    private final ClientLogger clientLogger = new ClientLogger(AzureStorageService.class);
    private final AzureStorageClientsManager clientsManager;
    private final boolean stateless;

    public AzureStorageService(
        Settings settings,
        AzureClientProvider azureClientProvider,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        this.clientsManager = new AzureStorageClientsManager(settings, projectResolver.supportsMultipleProjects());
        this.azureClientProvider = azureClientProvider;
        this.stateless = DiscoveryNode.isStateless(settings);
        this.multipartUploadMaxConcurrency = azureClientProvider.getMultipartUploadMaxConcurrency();
        if (projectResolver.supportsMultipleProjects()) {
            clusterService.addHighPriorityApplier(this.clientsManager);
        }
    }

    public AzureBlobServiceClient client(
        @Nullable ProjectId projectId,
        String clientName,
        LocationMode locationMode,
        OperationPurpose purpose
    ) {
        return client(projectId, clientName, locationMode, purpose, null);
    }

    public AzureBlobServiceClient client(
        @Nullable ProjectId projectId,
        String clientName,
        LocationMode locationMode,
        OperationPurpose purpose,
        AzureClientProvider.RequestMetricsHandler requestMetricsHandler
    ) {
        return clientsManager.client(projectId, clientName, locationMode, purpose, requestMetricsHandler);
    }

    private AzureBlobServiceClient buildClient(
        LocationMode locationMode,
        OperationPurpose purpose,
        AzureClientProvider.RequestMetricsHandler requestMetricsHandler,
        AzureStorageSettings azureStorageSettings
    ) {
        RequestRetryOptions retryOptions = getRetryOptions(locationMode, azureStorageSettings);
        ProxyOptions proxyOptions = getProxyOptions(azureStorageSettings);
        return azureClientProvider.createClient(
            azureStorageSettings,
            locationMode,
            retryOptions,
            proxyOptions,
            requestMetricsHandler,
            purpose
        );
    }

    private static ProxyOptions getProxyOptions(AzureStorageSettings settings) {
        Proxy proxy = settings.getProxy();
        if (proxy == null) {
            return null;
        }

        return switch (proxy.type()) {
            case HTTP -> new ProxyOptions(ProxyOptions.Type.HTTP, (InetSocketAddress) proxy.address());
            case SOCKS -> new ProxyOptions(ProxyOptions.Type.SOCKS5, (InetSocketAddress) proxy.address());
            default -> null;
        };
    }

    // non-static, package private for testing
    long getUploadBlockSize() {
        return DEFAULT_UPLOAD_BLOCK_SIZE;
    }

    int getMaxReadRetries(@Nullable ProjectId projectId, String clientName) {
        AzureStorageSettings azureStorageSettings = clientsManager.getClientSettings(projectId, clientName);
        return azureStorageSettings.getMaxRetries();
    }

    boolean isStateless() {
        return stateless;
    }

    // non-static, package private for testing
    RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
        AzureStorageSettings.StorageEndpoint endpoint = azureStorageSettings.getStorageEndpoint();
        String primaryUri = endpoint.primaryURI();
        String secondaryUri = endpoint.secondaryURI();

        if (locationMode == LocationMode.PRIMARY_THEN_SECONDARY && secondaryUri == null) {
            throw new IllegalArgumentException("Unable to use " + locationMode + " location mode without a secondary location URI");
        }

        final String secondaryHost = switch (locationMode) {
            case PRIMARY_ONLY, SECONDARY_ONLY -> null;
            case PRIMARY_THEN_SECONDARY -> secondaryUri;
            case SECONDARY_THEN_PRIMARY -> primaryUri;
        };

        // The request retry policy uses seconds as the default time unit, since
        // it's possible to configure a timeout < 1s we should ceil that value
        // as RequestRetryOptions expects a value >= 1.
        // See https://github.com/Azure/azure-sdk-for-java/issues/17590 for a proposal
        // to fix this issue.
        TimeValue configuredTimeout = azureStorageSettings.getTimeout();
        int timeout = configuredTimeout.duration() == -1 ? Integer.MAX_VALUE : Math.max(1, Math.toIntExact(configuredTimeout.getSeconds()));
        return new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL,
            azureStorageSettings.getMaxRetries(),
            timeout,
            null,
            null,
            secondaryHost
        );
    }

    /**
     * Updates settings for building cluster level clients. Any client cache is cleared. Future
     * client requests will use the new refreshed settings.
     *
     * @param clientsSettings the settings for new clients
     */
    public void refreshClusterClientSettings(Map<String, AzureStorageSettings> clientsSettings) {
        clientsManager.refreshClusterClientSettings(clientsSettings);
    }

    /**
     * For Azure repositories, we report the different kinds of credentials in use in the telemetry.
     */
    public Set<String> getExtraUsageFeatures(@Nullable ProjectId projectId, String clientName) {
        try {
            return clientsManager.getClientSettings(projectId, clientName).credentialsUsageFeatures();
        } catch (Exception e) {
            return Set.of();
        }
    }

    public int getMultipartUploadMaxConcurrency() {
        return multipartUploadMaxConcurrency;
    }

    // Package private for testing
    Map<String, AzureStorageSettings> getStorageSettings() {
        return clientsManager.clusterStorageSettings;
    }

    // Package private for testing
    AzureStorageClientsManager getClientsManager() {
        return clientsManager;
    }

    class AzureStorageClientsManager implements ClusterStateApplier {
        private static final String AZURE_SETTING_PREFIX = "azure.";

        private final Settings nodeAzureSettings;
        private volatile Map<String, AzureStorageSettings> clusterStorageSettings;
        private final Map<ProjectId, Map<String, AzureStorageSettings>> perProjectStorageSettings;

        AzureStorageClientsManager(Settings nodeSettings, boolean supportsMultipleProjects) {
            // eagerly load client settings so that secure settings are read
            final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(nodeSettings);
            refreshClusterClientSettings(clientsSettings);

            this.nodeAzureSettings = Settings.builder()
                .put(nodeSettings.getByPrefix(AZURE_SETTING_PREFIX), false) // not rely on any cluster scoped secrets
                .normalizePrefix(AZURE_SETTING_PREFIX)
                .build();
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
                final var allClientSettings = AzureStorageSettings.load(currentSettings);
                // Skip project client settings that have no credentials configured. This should not happen in serverless.
                // But it is safer to skip them and is also a more consistent behaviour with the cases when
                // project secrets are not present.
                final Map<String, AzureStorageSettings> clientSettingsWithCredentials = allClientSettings.entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().hasCredentials())
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

                // TODO: If performance is an issue, we may consider comparing just the relevant project secrets for new or updated clients
                // and avoid building the clientSettings
                if (newOrUpdated(project.id(), clientSettingsWithCredentials)) {
                    if (allClientSettings.size() != clientSettingsWithCredentials.size()) {
                        logger.warn(
                            "Project [{}] has [{}] azure client settings, but [{}] is usable due to missing credentials for clients {}",
                            project.id(),
                            allClientSettings.size(),
                            clientSettingsWithCredentials.size(),
                            Sets.difference(allClientSettings.keySet(), clientSettingsWithCredentials.keySet())
                        );
                    }
                    perProjectStorageSettings.put(project.id(), clientSettingsWithCredentials);
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

        public AzureBlobServiceClient client(
            @Nullable ProjectId projectId,
            String clientName,
            LocationMode locationMode,
            OperationPurpose purpose,
            AzureClientProvider.RequestMetricsHandler requestMetricsHandler
        ) {
            final var azureStorageSettings = getClientSettings(projectId, clientName); // ensure the client exists
            return buildClient(locationMode, purpose, requestMetricsHandler, azureStorageSettings);
        }

        public AzureStorageSettings getClientSettings(@Nullable ProjectId projectId, String clientName) {
            final var allClientSettings = getAllClientSettings(projectId);
            final var azureStorageSettings = allClientSettings.get(clientName);
            if (azureStorageSettings == null) {
                throw new SettingsException(
                    "Unable to find client with name ["
                        + clientName
                        + "]"
                        + (isDefaultProjectIdOrNull(projectId) ? "" : " for project [" + projectId + "]")
                );
            }
            return azureStorageSettings;
        }

        Map<String, AzureStorageSettings> getAllClientSettings(@Nullable ProjectId projectId) {
            if (projectId == null || ProjectId.DEFAULT.equals(projectId)) {
                return clusterStorageSettings;
            }
            final var projectClientSettings = perProjectStorageSettings.get(projectId);
            if (projectClientSettings == null) {
                throw new SettingsException("Unable to find any client for project [" + projectId + "]");
            }
            return projectClientSettings;
        }

        Map<ProjectId, Map<String, AzureStorageSettings>> getPerProjectStorageSettings() {
            return perProjectStorageSettings == null ? null : Map.copyOf(perProjectStorageSettings);
        }

        private void refreshClusterClientSettings(Map<String, AzureStorageSettings> clientsSettings) {
            this.clusterStorageSettings = Map.copyOf(clientsSettings);
            // clients are built lazily by {@link client(String, LocationMode)}
        }

        private boolean newOrUpdated(ProjectId projectId, Map<String, AzureStorageSettings> currentClientSettings) {
            final var old = perProjectStorageSettings.get(projectId);
            if (old == null) {
                return true;
            }
            return currentClientSettings.equals(old) == false;
        }

        private static boolean isDefaultProjectIdOrNull(@Nullable ProjectId projectId) {
            return projectId == null || ProjectId.DEFAULT.equals(projectId);
        }
    }
}
