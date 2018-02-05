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
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


class InternalAwsS3Service extends AbstractLifecycleComponent implements AwsS3Service {

    private final ReadWriteLock lock;
    private final ConcurrentHashMap<String, AmazonS3> clientsCache;
    private volatile Map<String, S3ClientSettings> clientsSettings;

    InternalAwsS3Service(Settings settings) {
        super(settings);
        this.lock = new ReentrantReadWriteLock();
        this.clientsCache = new ConcurrentHashMap<>();
        updateClientSettings(settings);
    }

    @Override
    public void updateClientSettings(Settings settings) {
        lock.writeLock().lock();
        try {
            // clear previously cached clients
            assert clientsSettings.containsKey("default") : "always at least have 'default'";
            for (final AmazonS3 client : clientsCache.values()) {
                client.shutdown();
            }
            clientsCache.clear();
            // reload secure settings
            clientsSettings = S3ClientSettings.load(settings);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public AmazonS3Wrapper client(String clientName) {
        return new AmazonS3Wrapper() {
            private final AmazonS3 client;
            {
                lock.readLock().lock();
                client = rawClient(clientName);
            }

            @Override
            public void close() {
                lock.readLock().unlock();
            }

            @Override
            public AmazonS3 client() {
                return client;
            }
        };
    }

    private AmazonS3 rawClient(String clientName) {
        // the mapping function is applied at most once
        return clientsCache.computeIfAbsent(clientName, c -> buildClient(c));
    }

    // does not require synchronization because it is called inside computeIfAbsent
    private AmazonS3 buildClient(String clientName) {
        final S3ClientSettings clientSettings = clientsSettings.get(clientName);
        if (clientSettings == null) {
            throw new IllegalArgumentException("Unknown s3 client name [" + clientName + "]. Existing client configs: " +
                Strings.collectionToDelimitedString(clientsSettings.keySet(), ","));
        }
        logger.debug("creating S3 client with client_name [{}], endpoint [{}]", clientName, clientSettings.endpoint);
        final AWSCredentialsProvider credentials = buildCredentials(clientSettings);
        final ClientConfiguration configuration = buildConfiguration(clientSettings);
        final AmazonS3Client client = new AmazonS3Client(credentials, configuration);
        if (Strings.hasText(clientSettings.endpoint)) {
            client.setEndpoint(clientSettings.endpoint);
        }
        return client;
    }

    // pkg private for tests
    ClientConfiguration buildConfiguration(S3ClientSettings clientSettings) {
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
    AWSCredentialsProvider buildCredentials(S3ClientSettings clientSettings) {
        final BasicAWSCredentials credentials = clientSettings.credentials;
        if (credentials == null) {
            logger.debug("Using instance profile credentials");
            return new PrivilegedInstanceProfileCredentialsProvider();
        } else {
            logger.debug("Using basic key/secret credentials");
            return new StaticCredentialsProvider(credentials);
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        for (final AmazonS3 client : clientsCache.values()) {
            client.shutdown();
        }
        // Ensure that IdleConnectionReaper is shutdown
        IdleConnectionReaper.shutdown();
    }

    static class PrivilegedInstanceProfileCredentialsProvider implements AWSCredentialsProvider {
        private final InstanceProfileCredentialsProvider credentials;

        private PrivilegedInstanceProfileCredentialsProvider() {
            this.credentials = new InstanceProfileCredentialsProvider();
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
}
