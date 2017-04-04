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

package org.elasticsearch.cloud.aws;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cloud.aws.util.SocketAccess;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.s3.S3Repository;

public class InternalAwsS3Service extends AbstractLifecycleComponent implements AwsS3Service {

    // pkg private for tests
    static final Setting<String> CLIENT_NAME = new Setting<>("client", "default", Function.identity());

    /**
     * (acceskey, endpoint) -&gt; client
     */
    private Map<Tuple<String, String>, AmazonS3Client> clients = new HashMap<>();

    public InternalAwsS3Service(Settings settings) {
        super(settings);
    }

    @Override
    public synchronized AmazonS3 client(Settings repositorySettings, Integer maxRetries,
                                              boolean useThrottleRetries, Boolean pathStyleAccess) {
        String clientName = CLIENT_NAME.get(repositorySettings);
        String foundEndpoint = findEndpoint(logger, repositorySettings, settings, clientName);

        AWSCredentialsProvider credentials = buildCredentials(logger, deprecationLogger, settings, repositorySettings, clientName);

        Tuple<String, String> clientDescriptor = new Tuple<>(foundEndpoint, credentials.getCredentials().getAWSAccessKeyId());
        AmazonS3Client client = clients.get(clientDescriptor);
        if (client != null) {
            return client;
        }

        client = new AmazonS3Client(
            credentials,
            buildConfiguration(logger, repositorySettings, settings, clientName, maxRetries, foundEndpoint, useThrottleRetries));

        if (pathStyleAccess != null) {
            client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(pathStyleAccess));
        }

        if (!foundEndpoint.isEmpty()) {
            client.setEndpoint(foundEndpoint);
        }

        clients.put(clientDescriptor, client);
        return client;
    }

    // pkg private for tests
    static ClientConfiguration buildConfiguration(Logger logger, Settings repositorySettings, Settings settings,
                                                         String clientName, Integer maxRetries, String endpoint,
                                                         boolean useThrottleRetries) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);
        Protocol protocol = getConfigValue(repositorySettings, settings, clientName, S3Repository.PROTOCOL_SETTING,
                                           S3Repository.Repository.PROTOCOL_SETTING, S3Repository.Repositories.PROTOCOL_SETTING);
        clientConfiguration.setProtocol(protocol);

        String proxyHost = getConfigValue(null, settings, clientName,
                                          S3Repository.PROXY_HOST_SETTING, null, CLOUD_S3.PROXY_HOST_SETTING);
        if (Strings.hasText(proxyHost)) {
            Integer proxyPort = getConfigValue(null, settings, clientName,
                                               S3Repository.PROXY_PORT_SETTING, null, CLOUD_S3.PROXY_PORT_SETTING);
            try (SecureString proxyUsername = getConfigValue(null, settings, clientName,
                                                             S3Repository.PROXY_USERNAME_SETTING, null, CLOUD_S3.PROXY_USERNAME_SETTING);
                 SecureString proxyPassword = getConfigValue(null, settings, clientName,
                                                             S3Repository.PROXY_PASSWORD_SETTING, null, CLOUD_S3.PROXY_PASSWORD_SETTING)) {

                clientConfiguration
                    .withProxyHost(proxyHost)
                    .withProxyPort(proxyPort)
                    .withProxyUsername(proxyUsername.toString())
                    .withProxyPassword(proxyPassword.toString());
            }
        }

        if (maxRetries != null) {
            // If not explicitly set, default to 3 with exponential backoff policy
            clientConfiguration.setMaxErrorRetry(maxRetries);
        }
        clientConfiguration.setUseThrottleRetries(useThrottleRetries);

        // #155: we might have 3rd party users using older S3 API version
        String awsSigner = CLOUD_S3.SIGNER_SETTING.get(settings);
        if (Strings.hasText(awsSigner)) {
            logger.debug("using AWS API signer [{}]", awsSigner);
            AwsSigner.configureSigner(awsSigner, clientConfiguration, endpoint);
        }

        TimeValue readTimeout = getConfigValue(null, settings, clientName,
                                               S3Repository.READ_TIMEOUT_SETTING, null, CLOUD_S3.READ_TIMEOUT);
        clientConfiguration.setSocketTimeout((int)readTimeout.millis());

        return clientConfiguration;
    }

    public static AWSCredentialsProvider buildCredentials(Logger logger, DeprecationLogger deprecationLogger,
                                                          Settings settings, Settings repositorySettings, String clientName) {
        try (SecureString key = getConfigValue(repositorySettings, settings, clientName, S3Repository.ACCESS_KEY_SETTING,
                                               S3Repository.Repository.KEY_SETTING, S3Repository.Repositories.KEY_SETTING);
             SecureString secret = getConfigValue(repositorySettings, settings, clientName, S3Repository.SECRET_KEY_SETTING,
                                                  S3Repository.Repository.SECRET_SETTING, S3Repository.Repositories.SECRET_SETTING)) {

            if (key.length() == 0 && secret.length() == 0) {
                logger.debug("Using instance profile credentials");
                return new PrivilegedInstanceProfileCredentialsProvider();
            } else {
                logger.debug("Using basic key/secret credentials");
                return new StaticCredentialsProvider(new BasicAWSCredentials(key.toString(), secret.toString()));
            }
        }
    }

    // pkg private for tests
    /** Returns the endpoint the client should use, based on the available endpoint settings found. */
    static String findEndpoint(Logger logger, Settings repositorySettings, Settings settings, String clientName) {
        String endpoint = getConfigValue(repositorySettings, settings, clientName, S3Repository.ENDPOINT_SETTING,
                                         S3Repository.Repository.ENDPOINT_SETTING, S3Repository.Repositories.ENDPOINT_SETTING);
        if (Strings.isNullOrEmpty(endpoint)) {
            // No region has been set so we will use the default endpoint
            if (CLOUD_S3.ENDPOINT_SETTING.exists(settings)) {
                endpoint = CLOUD_S3.ENDPOINT_SETTING.get(settings);
                logger.debug("using explicit s3 endpoint [{}]", endpoint);
            }
        } else {
            logger.debug("using repository level endpoint [{}]", endpoint);
        }

        return endpoint;
    }

    /**
     * Find the setting value, trying first with named configs,
     * then falling back to repository and global repositories settings.
     */
    private static <T> T getConfigValue(Settings repositorySettings, Settings globalSettings, String clientName,
                                        Setting.AffixSetting<T> configSetting, Setting<T> repositorySetting, Setting<T> globalSetting) {
        Setting<T> concreteSetting = configSetting.getConcreteSettingForNamespace(clientName);
        if (concreteSetting.exists(globalSettings)) {
            return concreteSetting.get(globalSettings);
        } else if (repositorySetting == null) {
            // no repository setting, just use global setting
            return globalSetting.get(globalSettings);
        } else {
            return S3Repository.getValue(repositorySettings, globalSettings, repositorySetting, globalSetting);
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
        for (AmazonS3Client client : clients.values()) {
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
