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
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.repositories.s3.S3Repository.getValue;

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
            throw new IllegalArgumentException("Unknown s3 client name [" + clientName + "]. " +
                "Existing client configs: " +
                Strings.collectionToDelimitedString(clientsSettings.keySet(), ","));
        }

        String endpoint = findEndpoint(logger, deprecationLogger, clientSettings, repositorySettings);
        // If the user defined a path style access setting, we rely on it,
        // otherwise we use the default value set by the SDK
        Boolean pathStyleAccess = null;
        if (S3Repository.Repository.PATH_STYLE_ACCESS_SETTING.exists(repositorySettings) ||
            S3Repository.Repositories.PATH_STYLE_ACCESS_SETTING.exists(settings)) {
            pathStyleAccess = getValue(repositorySettings, settings,
                S3Repository.Repository.PATH_STYLE_ACCESS_SETTING,
                S3Repository.Repositories.PATH_STYLE_ACCESS_SETTING);
        }

        logger.debug("creating S3 client with client_name [{}], endpoint [{}], path_style_access [{}]",
            clientName, endpoint, pathStyleAccess);

        AWSCredentialsProvider credentials = buildCredentials(logger, deprecationLogger, clientSettings, repositorySettings);
        ClientConfiguration configuration = buildConfiguration(logger, clientSettings, repositorySettings, endpoint);

        client = new AmazonS3Client(credentials, configuration);

        if (pathStyleAccess != null) {
            client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(pathStyleAccess));
        }

        if (Strings.hasText(endpoint)) {
            client.setEndpoint(endpoint);
        }

        clientsCache.put(clientName, client);
        return client;
    }

    // pkg private for tests
    static ClientConfiguration buildConfiguration(Logger logger, S3ClientSettings clientSettings,
                                                  Settings repositorySettings, String endpoint) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);
        Protocol protocol = getRepoValue(repositorySettings, S3Repository.Repository.PROTOCOL_SETTING, clientSettings.protocol);
        clientConfiguration.setProtocol(protocol);

        if (Strings.hasText(clientSettings.proxyHost)) {
            // TODO: remove this leniency, these settings should exist together and be validated
            clientConfiguration.setProxyHost(clientSettings.proxyHost);
            clientConfiguration.setProxyPort(clientSettings.proxyPort);
            clientConfiguration.setProxyUsername(clientSettings.proxyUsername);
            clientConfiguration.setProxyPassword(clientSettings.proxyPassword);
        }

        Integer maxRetries = getRepoValue(repositorySettings, S3Repository.Repository.MAX_RETRIES_SETTING, clientSettings.maxRetries);
        if (maxRetries != null) {
            // If not explicitly set, default to 3 with exponential backoff policy
            clientConfiguration.setMaxErrorRetry(maxRetries);
        }
        boolean useThrottleRetries = getRepoValue(repositorySettings,
            S3Repository.Repository.USE_THROTTLE_RETRIES_SETTING, clientSettings.throttleRetries);
        clientConfiguration.setUseThrottleRetries(useThrottleRetries);

        // #155: we might have 3rd party users using older S3 API version
        if (Strings.hasText(clientSettings.awsSigner)) {
            logger.debug("using AWS API signer [{}]", clientSettings.awsSigner);
            AwsSigner.configureSigner(clientSettings.awsSigner, clientConfiguration, endpoint);
        }
        clientConfiguration.setSocketTimeout(clientSettings.readTimeoutMillis);

        return clientConfiguration;
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

    // pkg private for tests
    static AWSCredentialsProvider buildCredentials(Logger logger, DeprecationLogger deprecationLogger,
                                                   S3ClientSettings clientSettings, Settings repositorySettings) {
        BasicAWSCredentials credentials = clientSettings.credentials;
        if (S3Repository.Repository.KEY_SETTING.exists(repositorySettings)) {
            if (S3Repository.Repository.SECRET_SETTING.exists(repositorySettings) == false) {
                throw new IllegalArgumentException("Repository setting [" + S3Repository.Repository.KEY_SETTING +
                    " must be accompanied by setting [" + S3Repository.Repository.SECRET_SETTING + "]");
            }
            // backcompat for reading keys out of repository settings
            deprecationLogger.deprecated("Using s3 access/secret key from repository settings. Instead " +
                "store these in named clients and the elasticsearch keystore for secure settings.");
            try (SecureString key = S3Repository.Repository.KEY_SETTING.get(repositorySettings);
                 SecureString secret = S3Repository.Repository.SECRET_SETTING.get(repositorySettings)) {
                credentials = new BasicAWSCredentials(key.toString(), secret.toString());
            }
        } else if (S3Repository.Repository.SECRET_SETTING.exists(repositorySettings)) {
            throw new IllegalArgumentException("Repository setting [" + S3Repository.Repository.SECRET_SETTING +
                " must be accompanied by setting [" + S3Repository.Repository.KEY_SETTING + "]");
        }
        if (credentials == null) {
            // create a "manual" chain of providers here, so we can log deprecation of unsupported methods
            AWSCredentials envCredentials = getDeprecatedCredentials(logger, deprecationLogger,
                new EnvironmentVariableCredentialsProvider(), "environment variables");
            if (envCredentials != null) {
                return new StaticCredentialsProvider(envCredentials);
            } else {
                AWSCredentials syspropCredentials = getDeprecatedCredentials(logger, deprecationLogger,
                    new SystemPropertiesCredentialsProvider(), "system properties");
                if (syspropCredentials != null) {
                    return new StaticCredentialsProvider(syspropCredentials);
                } else {
                    logger.debug("Using instance profile credentials");
                    return new InstanceProfileCredentialsProvider();
                }
            }
        } else {
            logger.debug("Using basic key/secret credentials");
            return new StaticCredentialsProvider(credentials);
        }
    }

    // pkg private for tests
    /** Returns the endpoint the client should use, based on the available endpoint settings found. */
    static String findEndpoint(Logger logger, DeprecationLogger deprecationLogger,
                               S3ClientSettings clientSettings, Settings repositorySettings) {
        String region = getRepoValue(repositorySettings, S3Repository.Repository.REGION_SETTING, clientSettings.region);
        String endpoint = getRepoValue(repositorySettings, S3Repository.Repository.ENDPOINT_SETTING, clientSettings.endpoint);
        if (Strings.isNullOrEmpty(endpoint)) {
            logger.debug("no repository level endpoint has been defined. Trying to guess from repository region [{}]", region);
            if (!region.isEmpty()) {
                deprecationLogger.deprecated("Specifying region for an s3 repository is deprecated. Use endpoint " +
                                             "to specify the region endpoint if the default behavior is not sufficient.");
                endpoint = getEndpoint(region);
                logger.debug("using s3 region [{}], with endpoint [{}]", region, endpoint);
            }
        } else {
            logger.debug("using repository level endpoint [{}]", endpoint);
        }
        return endpoint;
    }

    /**
     * Return the region configured, or empty string.
     * TODO: remove after https://github.com/elastic/elasticsearch/issues/22761 */
    public static String getRegion(Settings repositorySettings, Settings settings) {
        return S3Repository.getValue(repositorySettings, settings,
            S3Repository.Repository.REGION_SETTING, S3Repository.Repositories.REGION_SETTING);
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
}
