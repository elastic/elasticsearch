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
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
<<<<<<< HEAD:plugins/repository-s3/src/main/java/org/elasticsearch/cloud/aws/InternalAwsS3Service.java
=======

import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;
>>>>>>> 98d508f... Add client-side encryption:src/main/java/org/elasticsearch/cloud/aws/InternalAwsS3Service.java
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class InternalAwsS3Service extends AbstractLifecycleComponent<AwsS3Service> implements AwsS3Service {

    /**
<<<<<<< HEAD:plugins/repository-s3/src/main/java/org/elasticsearch/cloud/aws/InternalAwsS3Service.java
     * (acceskey, endpoint) -&gt; client
     */
    private Map<Tuple<String, String>, AmazonS3Client> clients = new HashMap<>();
=======
     * (acceskey, (endpoint, clientSideEncryptionKey)) -> client
     */
    private Map<Tuple<String, Tuple<String, EncryptionMaterials>>, AmazonS3Client> clients = new HashMap<Tuple<String,Tuple<String, EncryptionMaterials>>, AmazonS3Client>();
>>>>>>> 98d508f... Add client-side encryption:src/main/java/org/elasticsearch/cloud/aws/InternalAwsS3Service.java

    @Inject
    public InternalAwsS3Service(Settings settings) {
        super(settings);
    }

    @Override
<<<<<<< HEAD:plugins/repository-s3/src/main/java/org/elasticsearch/cloud/aws/InternalAwsS3Service.java
    public synchronized AmazonS3 client(String endpoint, Protocol protocol, String region, String account, String key, Integer maxRetries) {
        if (Strings.isNullOrEmpty(endpoint)) {
            // We need to set the endpoint based on the region
            if (region != null) {
                endpoint = getEndpoint(region);
                logger.debug("using s3 region [{}], with endpoint [{}]", region, endpoint);
            } else {
                // No region has been set so we will use the default endpoint
                endpoint = getDefaultEndpoint();
            }
=======
    public synchronized AmazonS3 client() {
        String endpoint = getDefaultEndpoint();
        String account = settings.get("cloud.aws.access_key", settings.get("cloud.account"));
        String key = settings.get("cloud.aws.secret_key", settings.get("cloud.key"));

        return getClient(endpoint, null, account, key, null, null);
    }

    @Override
    public AmazonS3 client(String endpoint, String protocol, String region, String account, String key) {
        return client(endpoint, protocol, region, account, key, null);
    }

    @Override
    public synchronized AmazonS3 client(String endpoint, String protocol, String region, String account, String key, Integer maxRetries) {
        return client(endpoint, protocol, region, account, key, maxRetries, null);
    }

    @Override
    public synchronized AmazonS3 client(String endpoint, String protocol, String region, String account, String key, Integer maxRetries, EncryptionMaterials clientSideEncryptionMaterials) {
        if (region != null && endpoint == null) {
            endpoint = getEndpoint(region);
            logger.debug("using s3 region [{}], with endpoint [{}]", region, endpoint);
        } else if (endpoint == null) {
            endpoint = getDefaultEndpoint();
        }
        if (account == null || key == null) {
            account = settings.get("cloud.aws.access_key", settings.get("cloud.account"));
            key = settings.get("cloud.aws.secret_key", settings.get("cloud.key"));
>>>>>>> 98d508f... Add client-side encryption:src/main/java/org/elasticsearch/cloud/aws/InternalAwsS3Service.java
        }

        return getClient(endpoint, protocol, account, key, maxRetries, clientSideEncryptionMaterials);
    }

<<<<<<< HEAD:plugins/repository-s3/src/main/java/org/elasticsearch/cloud/aws/InternalAwsS3Service.java
    private synchronized AmazonS3 getClient(String endpoint, Protocol protocol, String account, String key, Integer maxRetries) {
        Tuple<String, String> clientDescriptor = new Tuple<>(endpoint, account);
=======

    private synchronized AmazonS3 getClient(String endpoint, String protocol, String account, String key, Integer maxRetries, EncryptionMaterials clientSideEncryptionMaterials) {
        Tuple<String, Tuple<String, EncryptionMaterials>> clientDescriptor = new Tuple<String, Tuple<String, EncryptionMaterials>>(endpoint, new Tuple(account, clientSideEncryptionMaterials));
>>>>>>> 98d508f... Add client-side encryption:src/main/java/org/elasticsearch/cloud/aws/InternalAwsS3Service.java
        AmazonS3Client client = clients.get(clientDescriptor);
        if (client != null) {
            return client;
        }

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

        // #155: we might have 3rd party users using older S3 API version
        String awsSigner = CLOUD_S3.SIGNER_SETTING.get(settings);
        if (Strings.hasText(awsSigner)) {
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

        if(clientSideEncryptionMaterials != null) {
            EncryptionMaterialsProvider encryptionMaterialsProvider = new StaticEncryptionMaterialsProvider(clientSideEncryptionMaterials);
            CryptoConfiguration cryptoConfiguration = new CryptoConfiguration();
            client = new AmazonS3EncryptionClient(
                    credentials,
                    encryptionMaterialsProvider,
                    clientConfiguration,
                    cryptoConfiguration
            );
        } else {
            client = new AmazonS3Client(credentials, clientConfiguration);
        }

        if (endpoint != null) {
            client.setEndpoint(endpoint);
        }
        clients.put(clientDescriptor, client);
        return client;
    }

    private String getDefaultEndpoint() {
        String endpoint = null;
        if (CLOUD_S3.ENDPOINT_SETTING.exists(settings)) {
            endpoint = CLOUD_S3.ENDPOINT_SETTING.get(settings);
            logger.debug("using explicit s3 endpoint [{}]", endpoint);
        } else if (CLOUD_S3.REGION_SETTING.exists(settings)) {
            String region = CLOUD_S3.REGION_SETTING.get(settings);
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
