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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;


class InternalAwsS3Service extends AbstractLifecycleComponent implements AwsS3Service {

    // pkg private for tests
    static final Setting<String> CLIENT_NAME = new Setting<>("client", "default", Function.identity());

    private final Map<String, S3ClientSettings> clientsSettings;

    private final Map<String, AmazonS3Client> clientsCache = new HashMap<>();

    InternalAwsS3Service(Settings settings, Map<String, S3ClientSettings> clientsSettings) {
        super(settings);
        this.clientsSettings = clientsSettings;
    }

    @Override
    public synchronized AmazonS3 client(Settings repositorySettings) {
        String clientName = CLIENT_NAME.get(repositorySettings);
        AmazonS3Client client = clientsCache.get(clientName);
        if (client != null) {
            return client;
        }

        S3ClientSettings clientSettings = clientsSettings.get(clientName);
        if (clientSettings == null) {
            throw new IllegalArgumentException("Unknown s3 client name [" + clientName + "]. Existing client configs: " +
                Strings.collectionToDelimitedString(clientsSettings.keySet(), ","));
        }

        logger.debug("creating S3 client with client_name [{}], endpoint [{}]", clientName, clientSettings.endpoint);

        AWSCredentialsProvider credentials = buildCredentials(logger, deprecationLogger, clientSettings, repositorySettings);
        ClientConfiguration configuration = buildConfiguration(clientSettings, repositorySettings);

        client = new AmazonS3Client(credentials, configuration);

        if (Strings.hasText(clientSettings.endpoint)) {
            client.setEndpoint(clientSettings.endpoint);
        }

        clientsCache.put(clientName, client);
        return client;
    }

    // pkg private for tests
    static ClientConfiguration buildConfiguration(S3ClientSettings clientSettings, Settings repositorySettings) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
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
    static AWSCredentialsProvider buildCredentials(Logger logger, DeprecationLogger deprecationLogger,
                                                   S3ClientSettings clientSettings, Settings repositorySettings) {


        BasicAWSCredentials credentials = clientSettings.credentials;
        if (S3Repository.ACCESS_KEY_SETTING.exists(repositorySettings)) {
            if (S3Repository.SECRET_KEY_SETTING.exists(repositorySettings) == false) {
                throw new IllegalArgumentException("Repository setting [" + S3Repository.ACCESS_KEY_SETTING.getKey() +
                    " must be accompanied by setting [" + S3Repository.SECRET_KEY_SETTING.getKey() + "]");
            }
            try (SecureString key = S3Repository.ACCESS_KEY_SETTING.get(repositorySettings);
                 SecureString secret = S3Repository.SECRET_KEY_SETTING.get(repositorySettings)) {
                credentials = new BasicAWSCredentials(key.toString(), secret.toString());
            }
            // backcompat for reading keys out of repository settings
            deprecationLogger.deprecated("Using s3 access/secret key from repository settings. Instead " +
                "store these in named clients and the elasticsearch keystore for secure settings.");
        } else if (S3Repository.SECRET_KEY_SETTING.exists(repositorySettings)) {
            throw new IllegalArgumentException("Repository setting [" + S3Repository.SECRET_KEY_SETTING.getKey() +
                " must be accompanied by setting [" + S3Repository.ACCESS_KEY_SETTING.getKey() + "]");
        }
        if (credentials == null) {
            logger.debug("Using instance profile credentials");
            return new PrivilegedInstanceProfileCredentialsProvider();
        } else {
            logger.debug("Using basic key/secret credentials");
            return new StaticCredentialsProvider(credentials);
        }
    }

    /** Returns the value for a given setting from the repository, or returns the fallback value. */
    private static <T> T getRepoValue(Settings repositorySettings, Setting<T> repositorySetting, T fallback) {
        if (repositorySetting.exists(repositorySettings)) {
            return repositorySetting.get(repositorySettings);
        }
        return fallback;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        for (AmazonS3Client client : clientsCache.values()) {
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
