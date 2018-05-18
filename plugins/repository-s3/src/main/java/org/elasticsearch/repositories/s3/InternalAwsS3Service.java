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
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.util.AwsHostNameUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static com.amazonaws.services.s3.AmazonS3Client.S3_SERVICE_NAME;


class InternalAwsS3Service extends AbstractLifecycleComponent implements AwsS3Service {

    // pkg private for tests
    static final Setting<String> CLIENT_NAME = new Setting<>("client", "default", Function.identity());

    private final Map<String, S3ClientSettings> clientsSettings;

    private final Map<String, AmazonS3> clientsCache = new HashMap<>();

    InternalAwsS3Service(Settings settings, Map<String, S3ClientSettings> clientsSettings) {
        super(settings);
        this.clientsSettings = clientsSettings;
    }

    @Override
    public synchronized AmazonS3 client(Settings repositorySettings) {
        final String clientName = CLIENT_NAME.get(repositorySettings);
        AmazonS3 client = clientsCache.get(clientName);
        if (client != null) {
            return client;
        }

        final S3ClientSettings clientSettings = clientsSettings.get(clientName);
        if (clientSettings == null) {
            throw new IllegalArgumentException("Unknown s3 client name [" + clientName + "]. Existing client configs: " +
                Strings.collectionToDelimitedString(clientsSettings.keySet(), ","));
        }

        final AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();
        logger.debug("creating client with name [{}] and endpoint [{}]", clientName, clientSettings.endpoint);

        final AWSCredentialsProvider credentials = buildCredentials(logger, deprecationLogger, clientSettings, repositorySettings);
        clientBuilder.withCredentials(credentials);

        final ClientConfiguration configuration = buildConfiguration(clientSettings);
        clientBuilder.withClientConfiguration(configuration);

        final EndpointConfiguration endpoint = buildEndpoint(clientSettings);
        clientBuilder.withEndpointConfiguration(endpoint);
        if (Constants.S3_HOSTNAME.equals(endpoint.getServiceEndpoint())) {
            // Enable global bucket access when using the default AWS endpoint. We rely on the AWS SDK to
            // execute the first HTTP request using the default endpoint and to catch the redirect HTTP
            // response. This response contains a "x-amz-bucket-region" header which indicates the region
            // of the bucket. The AWS SDK also keeps an internal bucket/region cache.
            clientBuilder.enableForceGlobalBucketAccess();
        }

        client = clientBuilder.build();
        logger.debug("client [{}] created with service endpoint [{}] and signing region [{}]",
            clientName, endpoint.getServiceEndpoint(), endpoint.getSigningRegion());

        clientsCache.put(clientName, client);
        return client;
    }

    // pkg private for tests
    static ClientConfiguration buildConfiguration(final S3ClientSettings clientSettings) {
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

    /**
     * Builds a {@link EndpointConfiguration} from the client settings.
     *
     * When no endpoint is defined in the client settings, the default "s3.amazonaws.com" endpoint is used with the
     * default region provided by the AWS SDK. We rely on the AWS SDK and {@link AmazonS3ClientBuilder#enableForceGlobalBucketAccess()}
     * to later change this default region to the appropriate one.
     *
     * When the endpoint is defined in the client settings, the region is derived from the endpoint using the AWS SDK
     * if the endpoint is a known Amazon S3 endpoint. If it's not an Amazon S3 endpoint, then the region is null.
     *
     * @param clientSettings the client settings
     * @return a new {@link EndpointConfiguration}
     */
    static EndpointConfiguration buildEndpoint(final S3ClientSettings clientSettings) {
        final String serviceEndpoint = Strings.hasText(clientSettings.endpoint) ? clientSettings.endpoint : Constants.S3_HOSTNAME;

        String signingRegion = null;
        if (isAwsStandardEndpoint(serviceEndpoint)) {
            signingRegion = AwsHostNameUtils.parseRegion(serviceEndpoint, S3_SERVICE_NAME);
            if (signingRegion == null) {
                throw new IllegalArgumentException("Unable to find region for endpoint [" + serviceEndpoint + "]");
            }
        }
        return new EndpointConfiguration(serviceEndpoint, signingRegion);
    }

    static boolean isAwsStandardEndpoint(final String endpoint) {
        return endpoint.endsWith(".amazonaws.com") || endpoint.endsWith(".amazonaws.com.cn");
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        for (AmazonS3 client : clientsCache.values()) {
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
