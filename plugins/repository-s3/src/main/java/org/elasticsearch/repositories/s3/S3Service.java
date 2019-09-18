/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;

import java.io.Closeable;
import java.util.Map;

import static java.util.Collections.emptyMap;


class S3Service implements Closeable {
    private static final Logger logger = LogManager.getLogger(S3Service.class);

    private volatile Map<S3ClientSettings, AmazonS3Reference> clientsCache = emptyMap();

    /**
     * Client settings calculated from static configuration and settings in the keystore.
     */
    private volatile Map<String, S3ClientSettings> staticClientSettings =
            Map.of("default", S3ClientSettings.getClientSettings(Settings.EMPTY, "default"));

    /**
     * Client settings derived from those in {@link #staticClientSettings} by combining them with settings
     * in the {@link RepositoryMetaData}.
     */
    private volatile Map<S3ClientSettings, Map<RepositoryMetaData, S3ClientSettings>> derivedClientSettings = emptyMap();

    /**
     * Refreshes the settings for the AmazonS3 clients and clears the cache of
     * existing clients. New clients will be build using these new settings. Old
     * clients are usable until released. On release they will be destroyed instead
     * of being returned to the cache.
     */
    public synchronized void refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings) {
        // shutdown all unused clients
        // others will shutdown on their respective release
        releaseCachedClients();
        this.staticClientSettings = Maps.ofEntries(clientsSettings.entrySet());
        derivedClientSettings = emptyMap();
        assert this.staticClientSettings.containsKey("default") : "always at least have 'default'";
        // clients are built lazily by {@link client}
    }

    /**
     * Attempts to retrieve a client by its repository metadata and settings from the cache.
     * If the client does not exist it will be created.
     */
    public AmazonS3Reference client(RepositoryMetaData repositoryMetaData) {
        final S3ClientSettings clientSettings = settings(repositoryMetaData);
        {
            final AmazonS3Reference clientReference = clientsCache.get(clientSettings);
            if (clientReference != null && clientReference.tryIncRef()) {
                return clientReference;
            }
        }
        synchronized (this) {
            final AmazonS3Reference existing = clientsCache.get(clientSettings);
            if (existing != null && existing.tryIncRef()) {
                return existing;
            }
            final AmazonS3Reference clientReference = new AmazonS3Reference(buildClient(clientSettings));
            clientReference.incRef();
            clientsCache = Maps.copyMapWithAddedEntry(clientsCache, clientSettings, clientReference);
            return clientReference;
        }
    }

    /**
     * Either fetches {@link S3ClientSettings} for a given {@link RepositoryMetaData} from cached settings or creates them
     * by overriding static client settings from {@link #staticClientSettings} with settings found in the repository metadata.
     * @param repositoryMetaData Repository Metadata
     * @return S3ClientSettings
     */
    S3ClientSettings settings(RepositoryMetaData repositoryMetaData) {
        final String clientName = S3Repository.CLIENT_NAME.get(repositoryMetaData.settings());
        final S3ClientSettings staticSettings = staticClientSettings.get(clientName);
        if (staticSettings != null) {
            {
                final S3ClientSettings existing = derivedClientSettings.getOrDefault(staticSettings, emptyMap()).get(repositoryMetaData);
                if (existing != null) {
                    return existing;
                }
            }
            synchronized (this) {
                final Map<RepositoryMetaData, S3ClientSettings> derivedSettings =
                    derivedClientSettings.getOrDefault(staticSettings, emptyMap());
                final S3ClientSettings existing = derivedSettings.get(repositoryMetaData);
                if (existing != null) {
                    return existing;
                }
                final S3ClientSettings newSettings = staticSettings.refine(repositoryMetaData);
                derivedClientSettings =
                        Maps.copyMayWithAddedOrReplacedEntry(
                                derivedClientSettings,
                                staticSettings,
                                Maps.copyMapWithAddedEntry(derivedSettings, repositoryMetaData, newSettings));
                return newSettings;
            }
        }
        throw new IllegalArgumentException("Unknown s3 client name [" + clientName + "]. Existing client configs: "
            + Strings.collectionToDelimitedString(staticClientSettings.keySet(), ","));
    }

    // proxy for testing
    AmazonS3 buildClient(final S3ClientSettings clientSettings) {
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(buildCredentials(logger, clientSettings));
        builder.withClientConfiguration(buildConfiguration(clientSettings));

        final String endpoint = Strings.hasLength(clientSettings.endpoint) ? clientSettings.endpoint : Constants.S3_HOSTNAME;
        logger.debug("using endpoint [{}]", endpoint);

        // If the endpoint configuration isn't set on the builder then the default behaviour is to try
        // and work out what region we are in and use an appropriate endpoint - see AwsClientBuilder#setRegion.
        // In contrast, directly-constructed clients use s3.amazonaws.com unless otherwise instructed. We currently
        // use a directly-constructed client, and need to keep the existing behaviour to avoid a breaking change,
        // so to move to using the builder we must set it explicitly to keep the existing behaviour.
        //
        // We do this because directly constructing the client is deprecated (was already deprecated in 1.1.223 too)
        // so this change removes that usage of a deprecated API.
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, null));
        if (clientSettings.pathStyleAccess) {
            builder.enablePathStyleAccess();
        }
        if (clientSettings.disableChunkedEncoding) {
            builder.disableChunkedEncoding();
        }
        return builder.build();
    }

    // pkg private for tests
    static ClientConfiguration buildConfiguration(S3ClientSettings clientSettings) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setProtocol(clientSettings.protocol);

        if (Strings.hasText(clientSettings.proxyHost)) {
            // TODO: remove this leniency, these settings should exist together and be validated
            clientConfiguration.setProxyHost(clientSettings.proxyHost);
            clientConfiguration.setProxyPort(clientSettings.proxyPort);
            clientConfiguration.setProxyUsername(clientSettings.proxyUsername);
            clientConfiguration.setProxyPassword(clientSettings.proxyPassword);
        }

        clientConfiguration.setMaxErrorRetry(clientSettings.maxRetries);
        clientConfiguration.setUseThrottleRetries(clientSettings.throttleRetries);
        clientConfiguration.setSocketTimeout(clientSettings.readTimeoutMillis);

        return clientConfiguration;
    }

    // pkg private for tests
    static AWSCredentialsProvider buildCredentials(Logger logger, S3ClientSettings clientSettings) {
        final S3BasicCredentials credentials = clientSettings.credentials;
        if (credentials == null) {
            logger.debug("Using instance profile credentials");
            return new PrivilegedInstanceProfileCredentialsProvider();
        } else {
            logger.debug("Using basic key/secret credentials");
            return new AWSStaticCredentialsProvider(credentials);
        }
    }

    private synchronized void releaseCachedClients() {
        // the clients will shutdown when they will not be used anymore
        for (final AmazonS3Reference clientReference : clientsCache.values()) {
            clientReference.decRef();
        }
        // clear previously cached clients, they will be build lazily
        clientsCache = emptyMap();
        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }

    static class PrivilegedInstanceProfileCredentialsProvider implements AWSCredentialsProvider {
        private final AWSCredentialsProvider credentials;

        private PrivilegedInstanceProfileCredentialsProvider() {
            // InstanceProfileCredentialsProvider as last item of chain
            this.credentials = new EC2ContainerCredentialsProviderWrapper();
        }

        @Override
        public AWSCredentials getCredentials() {
            return SocketAccess.doPrivileged(credentials::getCredentials);
        }

        @Override
        public void refresh() {
            SocketAccess.doPrivilegedVoid(credentials::refresh);
        }
    }

    @Override
    public void close() {
        releaseCachedClients();
    }

}
