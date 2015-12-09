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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.*;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class InternalAwsS3Service extends AbstractLifecycleComponent<AwsS3Service> implements AwsS3Service {

    /**
     * (acceskey, endpoint) -&gt; client
     */
    private Map<Tuple<String, String>, AmazonS3Client> clients = new HashMap<Tuple<String,String>, AmazonS3Client>();

    @Inject
    public InternalAwsS3Service(Settings settings, SettingsFilter settingsFilter) {
        super(settings);
        settingsFilter.addFilter(CLOUD_AWS.KEY);
        settingsFilter.addFilter(CLOUD_AWS.SECRET);
        settingsFilter.addFilter(CLOUD_AWS.PROXY_PASSWORD);
        settingsFilter.addFilter(CLOUD_S3.KEY);
        settingsFilter.addFilter(CLOUD_S3.SECRET);
        settingsFilter.addFilter(CLOUD_S3.PROXY_PASSWORD);
        settingsFilter.addFilter("access_key");
        settingsFilter.addFilter("secret_key");
    }

    @Override
    public synchronized AmazonS3 client() {
        String endpoint = getDefaultEndpoint();
        String account = settings.get(CLOUD_S3.KEY, settings.get(CLOUD_AWS.KEY));
        String key = settings.get(CLOUD_S3.SECRET, settings.get(CLOUD_AWS.SECRET));
        return getClient(endpoint, null, account, key, null);
    }

    @Override
    public AmazonS3 client(String endpoint, String protocol, String region, String account, String key) {
        return client(endpoint, protocol, region, account, key, null);
    }

    @Override
    public synchronized AmazonS3 client(String endpoint, String protocol, String region, String account, String key, Integer maxRetries) {
        if (region != null && endpoint == null) {
            endpoint = getEndpoint(region);
            logger.debug("using s3 region [{}], with endpoint [{}]", region, endpoint);
        } else if (endpoint == null) {
            endpoint = getDefaultEndpoint();
        }
        if (account == null || key == null) {
            account = settings.get(CLOUD_S3.KEY, settings.get(CLOUD_AWS.KEY));
            key = settings.get(CLOUD_S3.SECRET, settings.get(CLOUD_AWS.SECRET));
        }

        return getClient(endpoint, protocol, account, key, maxRetries);
    }


    private synchronized AmazonS3 getClient(String endpoint, String protocol, String account, String key, Integer maxRetries) {
        Tuple<String, String> clientDescriptor = new Tuple<String, String>(endpoint, account);
        AmazonS3Client client = clients.get(clientDescriptor);
        if (client != null) {
            return client;
        }

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);
        if (protocol == null) {
            protocol = settings.get(CLOUD_AWS.PROTOCOL, "https").toLowerCase(Locale.ROOT);
            protocol = settings.get(CLOUD_S3.PROTOCOL, protocol).toLowerCase(Locale.ROOT);
        }

        if ("http".equals(protocol)) {
            clientConfiguration.setProtocol(Protocol.HTTP);
        } else if ("https".equals(protocol)) {
            clientConfiguration.setProtocol(Protocol.HTTPS);
        } else {
            throw new IllegalArgumentException("No protocol supported [" + protocol + "], can either be [http] or [https]");
        }

        String proxyHost = settings.get(CLOUD_AWS.PROXY_HOST, settings.get(CLOUD_AWS.DEPRECATED_PROXY_HOST));
        proxyHost = settings.get(CLOUD_S3.PROXY_HOST, settings.get(CLOUD_S3.DEPRECATED_PROXY_HOST, proxyHost));
        if (proxyHost != null) {
            String portString = settings.get(CLOUD_AWS.PROXY_PORT, settings.get(CLOUD_AWS.DEPRECATED_PROXY_PORT, "80"));
            portString = settings.get(CLOUD_S3.PROXY_PORT, settings.get(CLOUD_S3.DEPRECATED_PROXY_PORT, portString));
            Integer proxyPort;
            try {
                proxyPort = Integer.parseInt(portString, 10);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("The configured proxy port value [" + portString + "] is invalid", ex);
            }
            String proxyUsername = settings.get(CLOUD_S3.PROXY_USERNAME, settings.get(CLOUD_AWS.PROXY_USERNAME));
            String proxyPassword = settings.get(CLOUD_S3.PROXY_PASSWORD, settings.get(CLOUD_AWS.PROXY_PASSWORD));

            clientConfiguration
                .withProxyHost(proxyHost)
                .withProxyPort(proxyPort)
                .withProxyUsername(proxyUsername)
                .withProxyPassword(proxyPassword);
        }

        if (maxRetries != null) {
            // If not explicitly set, default to 3 with exponential backoff policy
            clientConfiguration.setMaxErrorRetry(maxRetries);
        }

        // #155: we might have 3rd party users using older S3 API version
        String awsSigner = settings.get(CLOUD_S3.SIGNER, settings.get(CLOUD_AWS.SIGNER));
        if (awsSigner != null) {
            logger.debug("using AWS API signer [{}]", awsSigner);
            AwsSigner.configureSigner(awsSigner, clientConfiguration, endpoint);
        }

        AWSCredentialsProvider credentials;

        if (account == null && key == null) {
            credentials = new AWSCredentialsProviderChain(
                    new EnvironmentVariableCredentialsProvider(),
                    new SystemPropertiesCredentialsProvider(),
                    new InstanceProfileCredentialsProvider()
            );
        } else {
            credentials = new AWSCredentialsProviderChain(
                    new StaticCredentialsProvider(new BasicAWSCredentials(account, key))
            );
        }
        client = new AmazonS3Client(credentials, clientConfiguration);

        if (endpoint != null) {
            client.setEndpoint(endpoint);
        }
        clients.put(clientDescriptor, client);
        return client;
    }

    private String getDefaultEndpoint() {
        String endpoint = null;
        if (settings.get(CLOUD_S3.ENDPOINT) != null) {
            endpoint = settings.get(CLOUD_S3.ENDPOINT);
            logger.debug("using explicit s3 endpoint [{}]", endpoint);
        } else if (settings.get(CLOUD_AWS.REGION) != null) {
            String region = settings.get(CLOUD_AWS.REGION).toLowerCase(Locale.ROOT);
            endpoint = getEndpoint(region);
            logger.debug("using s3 region [{}], with endpoint [{}]", region, endpoint);
        }
        return endpoint;
    }

    private static String getEndpoint(String region) {
        if ("us-east".equals(region) || "us-east-1".equals(region)) {
            return "s3.amazonaws.com";
        } else if ("us-west".equals(region) || "us-west-1".equals(region)) {
            return "s3-us-west-1.amazonaws.com";
        } else if ("us-west-2".equals(region)) {
            return "s3-us-west-2.amazonaws.com";
        } else if ("ap-southeast".equals(region) || "ap-southeast-1".equals(region)) {
            return "s3-ap-southeast-1.amazonaws.com";
        } else if ("ap-southeast-2".equals(region)) {
            return "s3-ap-southeast-2.amazonaws.com";
        } else if ("ap-northeast".equals(region) || "ap-northeast-1".equals(region)) {
            return "s3-ap-northeast-1.amazonaws.com";
        } else if ("eu-west".equals(region) || "eu-west-1".equals(region)) {
            return "s3-eu-west-1.amazonaws.com";
        } else if ("eu-central".equals(region) || "eu-central-1".equals(region)) {
            return "s3.eu-central-1.amazonaws.com";
        } else if ("sa-east".equals(region) || "sa-east-1".equals(region)) {
            return "s3-sa-east-1.amazonaws.com";
        } else if ("cn-north".equals(region) || "cn-north-1".equals(region)) {
            return "s3.cn-north-1.amazonaws.com.cn";
        } else if ("us-gov-west".equals(region) || "us-gov-west-1".equals(region)) {
            return "s3-us-gov-west-1.amazonaws.com";
        } else {
            throw new IllegalArgumentException("No automatic endpoint could be derived from region [" + region + "]");
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
}
