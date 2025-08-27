/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.SecurityUtils;
import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageRetryStrategy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.core.Strings.format;

public class GoogleCloudStorageService {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageService.class);
    private final GoogleCloudStorageClientsManager clientsManager;

    private final boolean isServerless;

    public GoogleCloudStorageService(ClusterService clusterService, ProjectResolver projectResolver) {
        final Settings nodeSettings = clusterService.getSettings();
        this.isServerless = DiscoveryNode.isStateless(nodeSettings);
        this.clientsManager = new GoogleCloudStorageClientsManager(nodeSettings, projectResolver.supportsMultipleProjects());
        if (projectResolver.supportsMultipleProjects()) {
            clusterService.addHighPriorityApplier(this.clientsManager);
        }
    }

    public boolean isServerless() {
        return isServerless;
    }

    /**
     * Refreshes the client settings and clears the client cache. Subsequent calls to
     * {@code GoogleCloudStorageService#client} will return new clients constructed
     * using the parameter settings. Note this method is for clients in non-MP setup
     * or cluster clients in MP setup. Project clients in MP setup are refreshed differently
     * via cluster applier, see {@link GoogleCloudStorageClientsManager#applyClusterState}.
     *
     * @param clientsSettings the new settings used for building clients for subsequent requests
     */
    public void refreshAndClearCache(Map<String, GoogleCloudStorageClientSettings> clientsSettings) {
        clientsManager.refreshAndClearCacheForClusterClients(clientsSettings);
    }

    /**
     * Attempts to retrieve a client from the cache. If the client does not exist it
     * will be created from the latest settings and will populate the cache. The
     * returned instance should not be cached by the calling code. Instead, for each
     * use, the (possibly updated) instance should be requested by calling this
     * method.
     *
     * @param projectId project for this repository. It is null if the repository is at cluster level in MP setup.
     * @param clientName name of the client settings used to create the client
     * @param repositoryName name of the repository that would use the client
     * @return a cached client storage instance that can be used to manage objects
     *         (blobs)
     */
    public MeteredStorage client(
        @Nullable final ProjectId projectId,
        final String clientName,
        final String repositoryName,
        final GcsRepositoryStatsCollector statsCollector
    ) throws IOException {
        return clientsManager.client(projectId, clientName, repositoryName, statsCollector);
    }

    /**
     * Close the repository client associated with the given project and repository.
     *
     * @param projectId project for the repository. It is null if the repository is at cluster level in MP setup.
     * @param repositoryName name of the repository for which the client is closed
     */
    void closeRepositoryClients(@Nullable final ProjectId projectId, String repositoryName) {
        clientsManager.closeRepositoryClients(projectId, repositoryName);
    }

    // package-private for tests
    GoogleCloudStorageClientsManager getClientsManager() {
        return clientsManager;
    }

    /**
     * Creates a client that can be used to manage Google Cloud Storage objects. The client is thread-safe.
     *
     * @param gcsClientSettings client settings to use, including secure settings
     * @return a new client storage instance that can be used to manage objects
     *         (blobs)
     */
    private MeteredStorage createClient(GoogleCloudStorageClientSettings gcsClientSettings, GcsRepositoryStatsCollector statsCollector)
        throws IOException {

        final NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
        // requires java.lang.RuntimePermission "setFactory"
        // Pin the TLS trust certificates.
        // We manually load the key store from jks instead of using GoogleUtils.getCertificateTrustStore() because that uses a .p12
        // store format not compatible with FIPS mode.
        final HttpTransport httpTransport;
        try {
            final KeyStore certTrustStore = SecurityUtils.getJavaKeyStore();
            try (InputStream keyStoreStream = GoogleUtils.class.getResourceAsStream("google.jks")) {
                SecurityUtils.loadKeyStore(certTrustStore, keyStoreStream, "notasecret");
            }
            builder.trustCertificates(certTrustStore);
            Proxy proxy = gcsClientSettings.getProxy();
            if (proxy != null) {
                builder.setProxy(proxy);
                notifyProxyIsSet(proxy);
            }
            httpTransport = builder.build();
        } catch (RuntimeException | IOException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        final HttpTransportOptions httpTransportOptions = new HttpTransportOptions(
            HttpTransportOptions.newBuilder()
                .setConnectTimeout(toTimeout(gcsClientSettings.getConnectTimeout()))
                .setReadTimeout(toTimeout(gcsClientSettings.getReadTimeout()))
                .setHttpTransportFactory(() -> httpTransport)
        ) {

            @Override
            public HttpRequestInitializer getHttpRequestInitializer(ServiceOptions<?, ?> serviceOptions) {
                HttpRequestInitializer requestInitializer = super.getHttpRequestInitializer(serviceOptions);

                return (httpRequest) -> {
                    if (requestInitializer != null) requestInitializer.initialize(httpRequest);
                    httpRequest.setResponseInterceptor(GcsRepositoryStatsCollector.METERING_INTERCEPTOR);
                };
            }
        };

        final StorageOptions storageOptions = createStorageOptions(gcsClientSettings, httpTransportOptions);
        return new MeteredStorage(storageOptions.getService(), statsCollector);
    }

    StorageOptions createStorageOptions(
        final GoogleCloudStorageClientSettings gcsClientSettings,
        final HttpTransportOptions httpTransportOptions
    ) {
        final StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder()
            .setStorageRetryStrategy(getRetryStrategy())
            .setTransportOptions(httpTransportOptions)
            .setHeaderProvider(() -> {
                return Strings.hasLength(gcsClientSettings.getApplicationName())
                    ? Map.of("user-agent", gcsClientSettings.getApplicationName())
                    : Map.of();
            });
        if (Strings.hasLength(gcsClientSettings.getHost())) {
            storageOptionsBuilder.setHost(gcsClientSettings.getHost());
        }
        if (Strings.hasLength(gcsClientSettings.getProjectId())) {
            storageOptionsBuilder.setProjectId(gcsClientSettings.getProjectId());
        } else {
            String defaultProjectId = null;
            try {
                defaultProjectId = ServiceOptions.getDefaultProjectId();
                if (defaultProjectId != null) {
                    storageOptionsBuilder.setProjectId(defaultProjectId);
                }
            } catch (Exception e) {
                logger.warn("failed to load default project id", e);
            }
            if (defaultProjectId == null) {
                try {
                    // fallback to manually load project ID here as the above ServiceOptions method has the metadata endpoint hardcoded,
                    // which makes it impossible to test
                    final String projectId = getDefaultProjectId(gcsClientSettings.getProxy());
                    if (projectId != null) {
                        storageOptionsBuilder.setProjectId(projectId);
                    }
                } catch (Exception e) {
                    logger.warn("failed to load default project id fallback", e);
                }
            }
        }
        if (gcsClientSettings.getCredential() == null) {
            try {
                storageOptionsBuilder.setCredentials(GoogleCredentials.getApplicationDefault());
            } catch (Exception e) {
                logger.warn("failed to load Application Default Credentials", e);
            }
        } else {
            ServiceAccountCredentials serviceAccountCredentials = gcsClientSettings.getCredential();
            // override token server URI
            final URI tokenServerUri = gcsClientSettings.getTokenUri();
            if (Strings.hasLength(tokenServerUri.toString())) {
                // Rebuild the service account credentials in order to use a custom Token url.
                // This is mostly used for testing purpose.
                serviceAccountCredentials = serviceAccountCredentials.toBuilder().setTokenServerUri(tokenServerUri).build();
            }
            storageOptionsBuilder.setCredentials(serviceAccountCredentials);
        }
        return storageOptionsBuilder.build();
    }

    protected StorageRetryStrategy getRetryStrategy() {
        return ShouldRetryDecorator.decorate(
            StorageRetryStrategy.getLegacyStorageRetryStrategy(),
            (Throwable prevThrowable, Object prevResponse, ResultRetryAlgorithm<Object> delegate) -> {
                // Retry in the event of an unknown host exception
                if (ExceptionsHelper.unwrap(prevThrowable, UnknownHostException.class) != null) {
                    return true;
                }
                // Also retry on `SocketException`s
                if (ExceptionsHelper.unwrap(prevThrowable, SocketException.class) != null) {
                    return true;
                }
                return delegate.shouldRetry(prevThrowable, prevResponse);
            }
        );
    }

    /**
     * This method imitates what MetadataConfig.getProjectId() does, but does not have the endpoint hardcoded.
     */
    @SuppressForbidden(reason = "ok to open connection here")
    static String getDefaultProjectId(@Nullable Proxy proxy) throws IOException {
        String metaHost = System.getenv("GCE_METADATA_HOST");
        if (metaHost == null) {
            metaHost = "metadata.google.internal";
        }
        URL url = new URL("http://" + metaHost + "/computeMetadata/v1/project/project-id");
        HttpURLConnection connection = (HttpURLConnection) (proxy != null ? url.openConnection(proxy) : url.openConnection());
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestProperty("Metadata-Flavor", "Google");
        try (InputStream input = connection.getInputStream()) {
            if (connection.getResponseCode() == 200) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, UTF_8))) {
                    return reader.readLine();
                }
            }
        }
        return null;
    }

    /**
     * Converts timeout values from the settings to a timeout value for the Google
     * Cloud SDK
     **/
    static Integer toTimeout(final TimeValue timeout) {
        // Null or zero in settings means the default timeout
        if (timeout == null || TimeValue.ZERO.equals(timeout)) {
            // negative value means using the default value
            return -1;
        }
        // -1 means infinite timeout
        if (TimeValue.MINUS_ONE.equals(timeout)) {
            // 0 is the infinite timeout expected by Google Cloud SDK
            return 0;
        }
        return Math.toIntExact(timeout.getMillis());
    }

    // used for unit testing
    void notifyProxyIsSet(Proxy proxy) {}

    class GoogleCloudStorageClientsManager implements ClusterStateApplier {

        private static final String GCS_SETTING_PREFIX = "gcs.";

        private final Settings nodeGcsSettings;
        // A map of projectId to clients holder. Adding to and removing from the map happen only in the applier thread.
        private final Map<ProjectId, PerProjectClientsHolder> perProjectClientsHolders;
        private final ClusterClientsHolder clusterClientsHolder;

        GoogleCloudStorageClientsManager(Settings nodeSettings, boolean supportsMultipleProjects) {
            this.nodeGcsSettings = Settings.builder()
                .put(nodeSettings.getByPrefix(GCS_SETTING_PREFIX), false) // not rely on any cluster scoped secrets
                .normalizePrefix(GCS_SETTING_PREFIX)
                .build();
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

            final var updatedPerProjectClients = new HashMap<ProjectId, PerProjectClientsHolder>();
            for (var project : currentProjects.values()) {
                // Skip the default project, it is tracked separately with clusterClientsHolder and
                // updated differently with the ReloadablePlugin interface
                if (ProjectId.DEFAULT.equals(project.id())) {
                    continue;
                }
                final ProjectSecrets projectSecrets = project.custom(ProjectSecrets.TYPE);
                // Project secrets can be null when node restarts. It may not have any GCS credentials if GCS is not in use.
                if (projectSecrets == null
                    || projectSecrets.getSettingNames().stream().noneMatch(key -> key.startsWith(GCS_SETTING_PREFIX))) {
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
                final var clientSettingsWithCredentials = allClientSettings.entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().getCredential() != null)
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

                // TODO: If performance is an issue, we may consider comparing just the relevant project secrets for new or updated clients
                // and avoid building the clientSettings
                if (newOrUpdated(project.id(), clientSettingsWithCredentials)) {
                    if (allClientSettings.size() != clientSettingsWithCredentials.size()) {
                        logger.warn(
                            "Project [{}] has [{}] GCS client settings, but [{}] is usable due to missing credentials for clients {}",
                            project.id(),
                            allClientSettings.size(),
                            clientSettingsWithCredentials.size(),
                            Sets.difference(allClientSettings.keySet(), clientSettingsWithCredentials.keySet())
                        );
                    }
                    updatedPerProjectClients.put(project.id(), new PerProjectClientsHolder(clientSettingsWithCredentials));
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
                final MeteredStorage storage = createClient(settings, statsCollector);
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
