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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.s3.S3Repository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.repositories.s3.S3Repository.getValue;

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

        AWSCredentialsProvider credentials = buildCredentials(logger, deprecationLogger, settings, repositorySettings);

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

    public static ClientConfiguration buildConfiguration(Logger logger, Settings settings, Protocol protocol, Integer maxRetries,
                                                         String endpoint, boolean useThrottleRetries) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setProtocol(protocol);

        String proxyHost = CLOUD_S3.PROXY_HOST_SETTING.get(settings);
        if (Strings.hasText(proxyHost)) {
            Integer proxyPort = CLOUD_S3.PROXY_PORT_SETTING.get(settings);
            try (SecureString proxyUsername = CLOUD_S3.PROXY_USERNAME_SETTING.get(settings);
                 SecureString proxyPassword = CLOUD_S3.PROXY_PASSWORD_SETTING.get(settings)) {

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

        clientConfiguration.setSocketTimeout((int) CLOUD_S3.READ_TIMEOUT.get(settings).millis());

        return clientConfiguration;
    }

    public static AWSCredentialsProvider buildCredentials(Logger logger, DeprecationLogger deprecationLogger,
                                                          Settings settings, Settings repositorySettings) {
        AWSCredentialsProvider credentials;
        try (SecureString key = getValue(repositorySettings, settings,
                                         S3Repository.Repository.KEY_SETTING, S3Repository.Repositories.KEY_SETTING);
             SecureString secret = getValue(repositorySettings, settings,
                                            S3Repository.Repository.SECRET_SETTING, S3Repository.Repositories.SECRET_SETTING)) {

            if (key.length() == 0 && secret.length() == 0) {
                // create a "manual" chain of providers here, so we can log deprecation of unsupported methods
                AWSCredentials envCredentials = getDeprecatedCredentials(logger, deprecationLogger,
                    new EnvironmentVariableCredentialsProvider(), "environment variables");
                if (envCredentials != null) {
                    credentials = new StaticCredentialsProvider(envCredentials);
                } else {
                    AWSCredentials syspropCredentials = getDeprecatedCredentials(logger, deprecationLogger,
                        new SystemPropertiesCredentialsProvider(), "system properties");
                    if (syspropCredentials != null) {
                        credentials = new StaticCredentialsProvider(syspropCredentials);
                    } else {
                        AWSCredentials profileCredentials = getDeprecatedCredentials(logger, deprecationLogger,
                            new ProfileCredentialsProvider(), "profile file");
                        if (profileCredentials != null) {
                            credentials = new StaticCredentialsProvider(profileCredentials);
                        } else {
                            logger.debug("Using instance profile credentials");
                            credentials = new InstanceProfileCredentialsProvider();
                        }
                    }
                }
            } else {
                logger.debug("Using basic key/secret credentials");
                credentials = new StaticCredentialsProvider(new BasicAWSCredentials(key.toString(), secret.toString()));
            }
        }

        return credentials;
    }

    /** Return credentials from the given provider, or null if full credentials are not available */
    private static AWSCredentials getDeprecatedCredentials(Logger logger, DeprecationLogger deprecationLogger,
                                                           AWSCredentialsProvider provider, String description) {
        try {
            AWSCredentials credentials = provider.getCredentials();
            if (credentials.getAWSAccessKeyId() != null && credentials.getAWSSecretKey() != null) {
                logger.debug("Using " + description + " credentials");
                deprecationLogger.deprecated("Supplying S3 credentials through " + description + " is deprecated. " +
                    "See the breaking changes lists in the documentation for details.");
                return credentials;
            }
        } catch (Exception e) {
            logger.debug("Failed to get aws credentials from " + description, e);
        }
        return null;
    }

    protected static String findEndpoint(Logger logger, Settings settings, String endpoint, String region) {
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
        final String endpoint;
        switch (region) {
            case "us-east":
            case "us-east-1":
                endpoint = "s3.amazonaws.com";
                break;
            case "us-east-2":
                endpoint = "s3.us-east-2.amazonaws.com";
                break;
            case "us-west":
            case "us-west-1":
                endpoint = "s3-us-west-1.amazonaws.com";
                break;
            case "us-west-2":
                endpoint = "s3-us-west-2.amazonaws.com";
                break;
            case "ap-south":
            case "ap-south-1":
                endpoint = "s3-ap-south-1.amazonaws.com";
                break;
            case "ap-southeast":
            case "ap-southeast-1":
                endpoint = "s3-ap-southeast-1.amazonaws.com";
                break;
            case "ap-southeast-2":
                endpoint = "s3-ap-southeast-2.amazonaws.com";
                break;
            case "ap-northeast":
            case "ap-northeast-1":
                endpoint = "s3-ap-northeast-1.amazonaws.com";
                break;
            case "ap-northeast-2":
                endpoint = "s3-ap-northeast-2.amazonaws.com";
                break;
            case "eu-west":
            case "eu-west-1":
                endpoint = "s3-eu-west-1.amazonaws.com";
                break;
            case "eu-west-2":
                endpoint = "s3-eu-west-2.amazonaws.com";
                break;
            case "eu-central":
            case "eu-central-1":
                endpoint = "s3.eu-central-1.amazonaws.com";
                break;
            case "sa-east":
            case "sa-east-1":
                endpoint = "s3-sa-east-1.amazonaws.com";
                break;
            case "cn-north":
            case "cn-north-1":
                endpoint = "s3.cn-north-1.amazonaws.com.cn";
                break;
            case "us-gov-west":
            case "us-gov-west-1":
                endpoint = "s3-us-gov-west-1.amazonaws.com";
                break;
            case "ca-central":
            case "ca-central-1":
                endpoint = "s3.ca-central-1.amazonaws.com";
                break;
            default:
                throw new IllegalArgumentException("No automatic endpoint could be derived from region [" + region + "]");
        }

        return endpoint;
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
