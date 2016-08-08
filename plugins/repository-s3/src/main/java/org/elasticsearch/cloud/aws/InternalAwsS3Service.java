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
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.s3.S3Repository;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.repositories.s3.S3Repository.getValue;

/**
 *
 */
public class InternalAwsS3Service extends AbstractLifecycleComponent implements AwsS3Service {

    /**
     * (acceskey, endpoint) -&gt; client
     */
    private Map<Tuple<String, String>, AmazonS3Client> clients = new HashMap<>();

    public InternalAwsS3Service(Settings settings) {
        super(settings);
    }

    @Override
    public synchronized AmazonS3 client(Settings repositorySettings, String endpoint, Protocol protocol, String region, Integer maxRetries,
                                        boolean useThrottleRetries, Boolean pathStyleAccess) {
        String foundEndpoint = findEndpoint(logger, settings, endpoint, region);

        AWSCredentialsProvider credentials = buildCredentials(logger, settings, repositorySettings);

        Tuple<String, String> clientDescriptor = new Tuple<>(foundEndpoint, credentials.getCredentials().getAWSAccessKeyId());
        AmazonS3Client client = clients.get(clientDescriptor);
        if (client != null) {
            return client;
        }

        client = new AmazonS3Client(
            credentials,
            buildConfiguration(logger, settings, protocol, maxRetries, foundEndpoint, useThrottleRetries));

        if (pathStyleAccess != null) {
            client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(pathStyleAccess));
        }

        if (!foundEndpoint.isEmpty()) {
            client.setEndpoint(foundEndpoint);
        }

        clients.put(clientDescriptor, client);
        return client;
    }

    public static ClientConfiguration buildConfiguration(ESLogger logger, Settings settings, Protocol protocol, Integer maxRetries,
                                                         String endpoint, boolean useThrottleRetries) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setProtocol(protocol);

        String proxyHost = CLOUD_S3.PROXY_HOST_SETTING.get(settings);
        if (Strings.hasText(proxyHost)) {
            Integer proxyPort = CLOUD_S3.PROXY_PORT_SETTING.get(settings);
            String proxyUsername = CLOUD_S3.PROXY_USERNAME_SETTING.get(settings);
            String proxyPassword = CLOUD_S3.PROXY_PASSWORD_SETTING.get(settings);

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
        clientConfiguration.setUseThrottleRetries(useThrottleRetries);

        // #155: we might have 3rd party users using older S3 API version
        String awsSigner = CLOUD_S3.SIGNER_SETTING.get(settings);
        if (Strings.hasText(awsSigner)) {
            logger.debug("using AWS API signer [{}]", awsSigner);
            AwsSigner.configureSigner(awsSigner, clientConfiguration, endpoint);
        }

        return clientConfiguration;
    }

    public static AWSCredentialsProvider buildCredentials(ESLogger logger, Settings settings, Settings repositorySettings) {
        AWSCredentialsProvider credentials;
        String key = getValue(repositorySettings, settings,
            S3Repository.Repository.KEY_SETTING, S3Repository.Repositories.KEY_SETTING);
        String secret = getValue(repositorySettings, settings,
            S3Repository.Repository.SECRET_SETTING, S3Repository.Repositories.SECRET_SETTING);

        if (key.isEmpty() && secret.isEmpty()) {
            logger.debug("Using either environment variables, system properties or instance profile credentials");
            credentials = new DefaultAWSCredentialsProviderChain();
        } else {
            logger.debug("Using basic key/secret credentials");
            credentials = new StaticCredentialsProvider(new BasicAWSCredentials(key, secret));
        }

        return credentials;
    }

    protected static String findEndpoint(ESLogger logger, Settings settings, String endpoint, String region) {
        if (Strings.isNullOrEmpty(endpoint)) {
            logger.debug("no repository level endpoint has been defined. Trying to guess from repository region [{}]", region);
            if (!region.isEmpty()) {
                endpoint = getEndpoint(region);
                logger.debug("using s3 region [{}], with endpoint [{}]", region, endpoint);
            } else {
                // No region has been set so we will use the default endpoint
                if (CLOUD_S3.ENDPOINT_SETTING.exists(settings)) {
                    endpoint = CLOUD_S3.ENDPOINT_SETTING.get(settings);
                    logger.debug("using explicit s3 endpoint [{}]", endpoint);
                } else if (REGION_SETTING.exists(settings) || CLOUD_S3.REGION_SETTING.exists(settings)) {
                    region = CLOUD_S3.REGION_SETTING.get(settings);
                    endpoint = getEndpoint(region);
                    logger.debug("using s3 region [{}], with endpoint [{}]", region, endpoint);
                }
            }
        } else {
            logger.debug("using repository level endpoint [{}]", endpoint);
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
        } else if (region.equals("ap-south-1")) {
            return "s3-ap-south-1.amazonaws.com";
        } else if ("ap-southeast".equals(region) || "ap-southeast-1".equals(region)) {
            return "s3-ap-southeast-1.amazonaws.com";
        } else if ("ap-southeast-2".equals(region)) {
            return "s3-ap-southeast-2.amazonaws.com";
        } else if ("ap-northeast".equals(region) || "ap-northeast-1".equals(region)) {
            return "s3-ap-northeast-1.amazonaws.com";
        } else if ("ap-northeast-2".equals(region)) {
            return "s3-ap-northeast-2.amazonaws.com";
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
