/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * The S3ClientsManager is responsible for managing Amazon S3 clients associated with either the cluster or projects.
 * To use a single data structure for all clients, the cluster clients are stored against the default project-id.
 * Note that the cluster level clients are created and refreshed based on the ReloadablePlugin interface, while
 * the project level clients are created and refreshed by cluster state updates. All clients are released when
 * the manager itself is closed.
 */
public class S3ClientsManager implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(S3ClientsManager.class);
    private static final String S3_SETTING_PREFIX = "s3.";

    private final Settings nodeS3Settings;
    private final Function<S3ClientSettings, AmazonS3Reference> clientBuilder;
    private final Executor executor;
    private final AtomicBoolean managerClosed = new AtomicBoolean(false);
    // A map of projectId to clients holder. Adding to and removing from the map happen only in the applier thread.
    private final Map<ProjectId, PerProjectClientsHolder> perProjectClientsHolders;
    private final ClusterClientsHolder clusterClientsHolder;

    S3ClientsManager(
        Settings nodeSettings,
        Function<S3ClientSettings, AmazonS3Reference> clientBuilder,
        Executor executor,
        boolean supportsMultipleProjects
    ) {
        this.nodeS3Settings = Settings.builder()
            .put(nodeSettings.getByPrefix(S3_SETTING_PREFIX), false) // not rely on any cluster scoped secrets
            .normalizePrefix(S3_SETTING_PREFIX)
            .build();
        this.clientBuilder = clientBuilder;
        this.executor = executor;
        this.clusterClientsHolder = new ClusterClientsHolder();
        this.perProjectClientsHolders = supportsMultipleProjects ? new ConcurrentHashMap<>() : null;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        assert perProjectClientsHolders != null : "expect per-project clients holders to be non-null";
        final Map<ProjectId, ProjectMetadata> currentProjects = event.state().metadata().projects();

        final var updatedPerProjectClients = new HashMap<ProjectId, PerProjectClientsHolder>();
        final List<PerProjectClientsHolder> clientsHoldersToClose = new ArrayList<>();
        for (var project : currentProjects.values()) {
            // Skip the default project, it is tracked separately with clusterClientsHolder and
            // updated differently with the ReloadablePlugin interface
            if (ProjectId.DEFAULT.equals(project.id())) {
                continue;
            }
            final ProjectSecrets projectSecrets = project.custom(ProjectSecrets.TYPE);
            // Project secrets can be null when node restarts. It may not have any s3 credentials if s3 is not in use.
            if (projectSecrets == null || projectSecrets.getSettingNames().stream().noneMatch(key -> key.startsWith("s3."))) {
                // Most likely there won't be any existing client, but attempt to remove it anyway just in case
                final var removed = perProjectClientsHolders.remove(project.id());
                if (removed != null) {
                    clientsHoldersToClose.add(removed);
                }
                continue;
            }

            final Settings currentSettings = Settings.builder()
                // merge with static settings such as max retries etc
                // TODO: https://elasticco.atlassian.net/browse/ES-11716 Consider change this to use per-project settings
                .put(nodeS3Settings)
                .setSecureSettings(projectSecrets.getSettings())
                .build();
            final Map<String, S3ClientSettings> clientSettings = S3ClientSettings.load(currentSettings)
                .entrySet()
                .stream()
                // Skip project clients that have no credentials configured. This should not happen in serverless.
                // But it is safer to skip them and is also a more consistent behaviour with the cases when
                // project secrets are not present.
                .filter(entry -> entry.getValue().credentials != null)
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
                updatedPerProjectClients.put(project.id(), new PerProjectClientsHolder(project.id(), clientSettings));
            }
        }

        // Updated projects
        for (var projectId : updatedPerProjectClients.keySet()) {
            assert ProjectId.DEFAULT.equals(projectId) == false;
            final var old = perProjectClientsHolders.put(projectId, updatedPerProjectClients.get(projectId));
            if (old != null) {
                clientsHoldersToClose.add(old);
            }
        }
        // Removed projects
        for (var projectId : perProjectClientsHolders.keySet()) {
            assert ProjectId.DEFAULT.equals(projectId) == false;
            if (currentProjects.containsKey(projectId) == false) {
                final var removed = perProjectClientsHolders.remove(projectId);
                clientsHoldersToClose.add(removed);
            }
        }
        // Close stale clients asynchronously without blocking the applier thread
        if (clientsHoldersToClose.isEmpty() == false) {
            closePerProjectClientsAsync(clientsHoldersToClose);
        }
    }

    private void closePerProjectClientsAsync(List<PerProjectClientsHolder> clientsHoldersToClose) {
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                IOUtils.closeWhileHandlingException(clientsHoldersToClose);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to close s3 clients", e);
            }
        });
    }

    // visible for tests
    ClusterClientsHolder getClusterClientsHolder() {
        return clusterClientsHolder;
    }

    // visible for tests
    Map<ProjectId, PerProjectClientsHolder> getPerProjectClientsHolders() {
        return perProjectClientsHolders == null ? null : Map.copyOf(perProjectClientsHolders);
    }

    // visible for tests
    boolean isManagerClosed() {
        return managerClosed.get();
    }

    void refreshAndClearCacheForClusterClients(Map<String, S3ClientSettings> clientsSettings) {
        clusterClientsHolder.refreshAndClearCache(clientsSettings);
    }

    S3ClientSettings settingsForClient(ProjectId projectId, RepositoryMetadata repositoryMetadata) {
        return getClientsHolderSafe(projectId).singleClientSettings(repositoryMetadata);
    }

    AmazonS3Reference client(ProjectId projectId, RepositoryMetadata repositoryMetadata) {
        return getClientsHolderSafe(projectId).client(repositoryMetadata);
    }

    /**
     * Clears the cache for the given project (default project-id is for the cluster level clients).
     * All clients for the project are closed and will be recreated on next access.
     */
    void releaseCachedClients(ProjectId projectId) {
        final var old = getClientsHolder(projectId);
        if (old != null) {
            old.clearCache();
        }
    }

    private ClientsHolder<?> getClientsHolderSafe(ProjectId projectId) {
        final var clientsHolder = getClientsHolder(projectId);
        if (clientsHolder == null) {
            throw new IllegalArgumentException("no s3 client is configured for project [" + projectId + "]");
        }
        return clientsHolder;
    }

    @Nullable
    private ClientsHolder<?> getClientsHolder(ProjectId projectId) {
        if (ProjectId.DEFAULT.equals(Objects.requireNonNull(projectId))) {
            return clusterClientsHolder;
        } else {
            assert perProjectClientsHolders != null : "expect per-project clients holders to be non-null";
            return perProjectClientsHolders.get(projectId);
        }
    }

    /**
     * Shutdown the manager by closing all clients holders. This is called when the node is shutting down.
     */
    void close() {
        if (managerClosed.compareAndSet(false, true)) {
            // Close all clients holders, they will close their cached clients.
            // It's OK if a new clients holder is added concurrently or after this point because
            // no new client will be created once the manager is closed, i.e. nothing to release.
            if (perProjectClientsHolders != null) {
                IOUtils.closeWhileHandlingException(perProjectClientsHolders.values());
            }
            IOUtils.closeWhileHandlingException(clusterClientsHolder);
        } else {
            assert false : "attempting to close s3 clients manager multiple times";
        }
    }

    private boolean newOrUpdated(ProjectId projectId, Map<String, S3ClientSettings> currentClientSettings) {
        final var old = perProjectClientsHolders.get(projectId);
        if (old == null) {
            return true;
        }
        return currentClientSettings.equals(old.allClientSettings()) == false;
    }

    /**
     * The base class of s3 clients holder for a project or the cluster. The clients are created and cached lazily when
     * {@link #client(RepositoryMetadata)} method is called. Cached clients are closed and cleared out when
     * {@link #clearCache()} method is called. Subsequent calls to {@link #client(RepositoryMetadata)} will
     * recreate them. The call to {@link #close()} method clears the cache as well but also flags the holder to be
     * closed so that no new clients can be created.
     * Subclasses must define the type of the client key for the cache.
     */
    abstract class ClientsHolder<K> implements Closeable {
        protected volatile Map<K, AmazonS3Reference> clientsCache = Collections.emptyMap();
        private final AtomicBoolean closed = new AtomicBoolean(false);

        /**
         * Determine the client key for retrieving the cached client.
         * @param repositoryMetadata The repository metadata
         * @return Key to a cached client.
         */
        abstract K clientKey(RepositoryMetadata repositoryMetadata);

        /**
         * Get the client settings for a given client key.
         */
        abstract S3ClientSettings singleClientSettings(K clientKey);

        /**
         * Get a map of client name to client settings for all clients.
         */
        abstract Map<String, S3ClientSettings> allClientSettings();

        /**
         * Get the project id for which this clients holder is associated with.
         */
        abstract ProjectId projectId();

        /**
         * Similar to {@link #singleClientSettings(K)} but from the given repository metadata.
         */
        S3ClientSettings singleClientSettings(RepositoryMetadata repositoryMetadata) {
            return singleClientSettings(clientKey(repositoryMetadata));
        }

        /**
         * Retrieves an {@link AmazonS3Reference} for the given repository metadata. If a cached client exists and can be
         * referenced, it is returned. Otherwise, a new client is created, added to the cache, and returned.
         *
         * @param repositoryMetadata The metadata of the repository for which the Amazon S3 client is required.
         * @return An {@link AmazonS3Reference} instance corresponding to the repository metadata.
         * @throws IllegalArgumentException If no client settings exist for the given repository metadata.
         * @throws AlreadyClosedException If either the clients manager or the holder is closed
         */
        final AmazonS3Reference client(RepositoryMetadata repositoryMetadata) {
            final var clientKey = clientKey(repositoryMetadata);
            final var clientReference = clientsCache.get(clientKey);
            // It is ok to retrieve an existing client when the cache is being cleared or the holder is closing.
            // As long as there are paired incRef/decRef calls, the client will be closed when the last reference is released
            // by either the caller of this method or the clearCache() method.
            if (clientReference != null && clientReference.tryIncRef()) {
                return clientReference;
            }

            final var settings = singleClientSettings(clientKey);
            synchronized (this) {
                final var existing = clientsCache.get(clientKey);
                if (existing != null && existing.tryIncRef()) {
                    return existing;
                }
                if (closed.get()) {
                    // Not adding a new client once the clients holder is closed since there won't be anything to close it
                    throw new AlreadyClosedException("Project [" + projectId() + "] clients holder is closed");
                }
                if (managerClosed.get()) {
                    // This clients holder must be added after the manager is closed. It must have no cached clients.
                    assert clientsCache.isEmpty() : "expect empty cache, but got " + clientsCache;
                    throw new AlreadyClosedException("s3 clients manager is closed");
                }
                // The close() method maybe called after we checked it, it is ok since we are already inside the synchronized block.
                // The close method calls clearCache() which will clear the newly added client.
                final var newClientReference = clientBuilder.apply(settings);
                clientsCache = Maps.copyMapWithAddedEntry(clientsCache, clientKey, newClientReference);
                return newClientReference;
            }
        }

        /**
         * Clear the cache by closing and clearing out all clients. Subsequent {@link #client(RepositoryMetadata)} calls will recreate
         * the clients and populate the cache again.
         */
        final synchronized void clearCache() {
            // the clients will shutdown when they will not be used anymore
            IOUtils.closeWhileHandlingException(clientsCache.values());
            // clear previously cached clients, they will be built lazily
            clientsCache = Collections.emptyMap();
            doClearCache();
        }

        void doClearCache() {}

        /**
         * Similar to {@link #clearCache()} but also flag the holder to be closed so that no new client can be created.
         */
        public final void close() {
            if (closed.compareAndSet(false, true)) {
                clearCache();
            }
        }

        // visible for tests
        final boolean isClosed() {
            return closed.get();
        }
    }

    /**
     * S3 clients holder for a single project. The client cache is keyed by the client name.
     */
    final class PerProjectClientsHolder extends ClientsHolder<String> {
        private final ProjectId projectId;
        private final Map<String, S3ClientSettings> clientSettings;

        PerProjectClientsHolder(ProjectId projectId, Map<String, S3ClientSettings> clientSettings) {
            assert ProjectId.DEFAULT.equals(projectId) == false;
            this.projectId = projectId;
            this.clientSettings = clientSettings;
        }

        @Override
        Map<String, S3ClientSettings> allClientSettings() {
            return clientSettings;
        }

        @Override
        String clientKey(RepositoryMetadata repositoryMetadata) {
            return S3Repository.CLIENT_NAME.get(repositoryMetadata.settings());
        }

        @Override
        S3ClientSettings singleClientSettings(String clientKey) {
            final S3ClientSettings settings = clientSettings.get(clientKey);
            if (settings == null) {
                throw new IllegalArgumentException("s3 client [" + clientKey + "] does not exist for project [" + projectId + "]");
            }
            return settings;
        }

        @Override
        ProjectId projectId() {
            return projectId;
        }
    }

    /**
     * S3 clients holder for the cluster. The client cache is keyed by the derived client settings.
     * The derived client settings are computed by combining the static client settings with overrides from the repository metadata.
     */
    final class ClusterClientsHolder extends ClientsHolder<S3ClientSettings> {

        /**
         * Client settings calculated from static configuration and settings in the keystore.
         */
        private volatile Map<String, S3ClientSettings> staticClientSettings = Map.of(
            "default",
            S3ClientSettings.getClientSettings(Settings.EMPTY, "default")
        );

        /**
         * Client settings derived from those in {@link #staticClientSettings} by combining them with settings
         * in the {@link RepositoryMetadata}.
         */
        private volatile Map<Settings, S3ClientSettings> derivedClientSettings = emptyMap();

        /**
         * Either fetches {@link S3ClientSettings} for a given {@link RepositoryMetadata} from cached settings or creates them
         * by overriding static client settings from {@link #staticClientSettings} with settings found in the repository metadata.
         * @param repositoryMetadata Repository Metadata
         * @return S3ClientSettings
         */
        @Override
        S3ClientSettings clientKey(RepositoryMetadata repositoryMetadata) {
            final Settings settings = repositoryMetadata.settings();
            {
                final S3ClientSettings existing = derivedClientSettings.get(settings);
                if (existing != null) {
                    return existing;
                }
            }
            final String clientName = S3Repository.CLIENT_NAME.get(settings);
            final S3ClientSettings staticSettings = staticClientSettings.get(clientName);
            if (staticSettings != null) {
                synchronized (this) {
                    final S3ClientSettings existing = derivedClientSettings.get(settings);
                    if (existing != null) {
                        return existing;
                    }
                    final S3ClientSettings newSettings = staticSettings.refine(settings);
                    derivedClientSettings = Maps.copyMapWithAddedOrReplacedEntry(derivedClientSettings, settings, newSettings);
                    return newSettings;
                }
            }
            throw new IllegalArgumentException(
                "Unknown s3 client name ["
                    + clientName
                    + "]. Existing client configs: "
                    + Strings.collectionToDelimitedString(staticClientSettings.keySet(), ",")
            );
        }

        @Override
        S3ClientSettings singleClientSettings(S3ClientSettings clientKey) {
            return clientKey;
        }

        @Override
        Map<String, S3ClientSettings> allClientSettings() {
            return staticClientSettings;
        }

        @Override
        void doClearCache() {
            // clear the derived settings, they will be built lazily
            derivedClientSettings = emptyMap();
        }

        @Override
        ProjectId projectId() {
            return ProjectId.DEFAULT;
        }

        /**
         * Refreshes the settings for the AmazonS3 clients and clears the cache of
         * existing clients. New clients will be built using these new settings. Old
         * clients are usable until released. On release, they will be destroyed instead
         * of being returned to the cache.
         */
        synchronized void refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings) {
            // shutdown all unused clients
            // others will shutdown on their respective release
            clearCache(); // clears client cache and derived settings
            this.staticClientSettings = Maps.ofEntries(clientsSettings.entrySet());
            assert this.staticClientSettings.containsKey("default") : "always at least have 'default'";
            /* clients are built lazily by {@link #client} */
        }
    }
}
