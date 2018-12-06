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
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;


class S3Service implements Closeable {
    
    private static final Logger logger = LogManager.getLogger(S3Service.class);

    private volatile Map<String, AmazonS3Reference> clientsCache = emptyMap();
    private volatile Map<String, S3ClientSettings> clientsSettings = emptyMap();

    /**
     * Refreshes the settings for the AmazonS3 clients and clears the cache of
     * existing clients. New clients will be build using these new settings. Old
     * clients are usable until released. On release they will be destroyed instead
     * to being returned to the cache.
     */
    public synchronized Map<String, S3ClientSettings> refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings) {
        // shutdown all unused clients
        // others will shutdown on their respective release
        releaseCachedClients();
        final Map<String, S3ClientSettings> prevSettings = this.clientsSettings;
        this.clientsSettings = MapBuilder.newMapBuilder(clientsSettings).immutableMap();
        assert this.clientsSettings.containsKey("default") : "always at least have 'default'";
        // clients are built lazily by {@link client(String)}
        return prevSettings;
    }

    /**
     * Attempts to retrieve a client by name from the cache. If the client does not
     * exist it will be created.
     */
    public AmazonS3Reference client(String clientName) {
        AmazonS3Reference clientReference = clientsCache.get(clientName);
        if ((clientReference != null) && clientReference.tryIncRef()) {
            return clientReference;
        }
        synchronized (this) {
            clientReference = clientsCache.get(clientName);
            if ((clientReference != null) && clientReference.tryIncRef()) {
                return clientReference;
            }
            final S3ClientSettings clientSettings = clientsSettings.get(clientName);
            if (clientSettings == null) {
                throw new IllegalArgumentException("Unknown s3 client name [" + clientName + "]. Existing client configs: "
                        + Strings.collectionToDelimitedString(clientsSettings.keySet(), ","));
            }
            logger.debug("creating S3 client with client_name [{}], endpoint [{}]", clientName, clientSettings.endpoint);
            clientReference = new AmazonS3Reference(buildClient(clientSettings));
            clientReference.incRef();
            clientsCache = MapBuilder.newMapBuilder(clientsCache).put(clientName, clientReference).immutableMap();
            return clientReference;
        }
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
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, null))
            .enablePathStyleAccess();

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
        final AWSCredentials credentials = clientSettings.credentials;
        if (credentials == null) {
            logger.debug("Using instance profile credentials");
            return new PrivilegedInstanceProfileCredentialsProvider();
        } else {
            logger.debug("Using basic key/secret credentials");
            return new StaticCredentialsProvider(credentials);
        }
    }

    protected synchronized void releaseCachedClients() {
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
    public void close() throws IOException {
        releaseCachedClients();
    }

}
