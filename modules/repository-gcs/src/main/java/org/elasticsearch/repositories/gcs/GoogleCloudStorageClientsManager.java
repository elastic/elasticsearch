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
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.core.Strings.format;

public class GoogleCloudStorageClientsManager implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageClientsManager.class);

    private final CheckedBiFunction<
        GoogleCloudStorageClientSettings,
        GcsRepositoryStatsCollector,
        MeteredStorage,
        IOException> clientBuilder;
    private final ClientsHolder clusterClientsHolder;

    public GoogleCloudStorageClientsManager(
        CheckedBiFunction<GoogleCloudStorageClientSettings, GcsRepositoryStatsCollector, MeteredStorage, IOException> clientBuilder
    ) {
        this.clientBuilder = clientBuilder;
        this.clusterClientsHolder = new ClientsHolder();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {

    }

    void refreshAndClearCache(Map<String, GoogleCloudStorageClientSettings> clientsSettings) {
        clusterClientsHolder.refreshAndClearCache(clientsSettings);
    }

    MeteredStorage client(final String clientName, final String repositoryName, final GcsRepositoryStatsCollector statsCollector)
        throws IOException {
        return clusterClientsHolder.client(clientName, repositoryName, statsCollector);
    }

    void closeRepositoryClients(String repositoryName) {
        clusterClientsHolder.closeRepositoryClients(repositoryName);
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
