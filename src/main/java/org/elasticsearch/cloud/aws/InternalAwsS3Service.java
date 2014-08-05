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
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class InternalAwsS3Service extends AbstractLifecycleComponent<AwsS3Service> implements AwsS3Service {

    /**
     * (acceskey, endpoint) -> client
     */
    private Map<Tuple<String, String>, AmazonS3Client> clients = new HashMap<Tuple<String,String>, AmazonS3Client>();

    @Inject
    public InternalAwsS3Service(Settings settings, SettingsFilter settingsFilter) {
        super(settings);

        settingsFilter.addFilter(new AwsSettingsFilter());
    }

    @Override
    public synchronized AmazonS3 client() {
        String endpoint = getDefaultEndpoint();
        String account = componentSettings.get("access_key", settings.get("cloud.account"));
        String key = componentSettings.get("secret_key", settings.get("cloud.key"));

        return getClient(endpoint, account, key);
    }

    @Override
    public synchronized AmazonS3 client(String region, String account, String key) {
        String endpoint;
        if (region == null) {
            endpoint = getDefaultEndpoint();
        } else {
            endpoint = getEndpoint(region);
            logger.debug("using s3 region [{}], with endpoint [{}]", region, endpoint);
        }
        if (account == null || key == null) {
            account = componentSettings.get("access_key", settings.get("cloud.account"));
            key = componentSettings.get("secret_key", settings.get("cloud.key"));
        }

        return getClient(endpoint, account, key);
    }


    private synchronized AmazonS3 getClient(String endpoint, String account, String key) {
        Tuple<String, String> clientDescriptor = new Tuple<String, String>(endpoint, account);
        AmazonS3Client client = clients.get(clientDescriptor);
        if (client != null) {
            return client;
        }

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        String protocol = componentSettings.get("protocol", "https").toLowerCase();
        protocol = componentSettings.get("s3.protocol", protocol).toLowerCase();
        if ("http".equals(protocol)) {
            clientConfiguration.setProtocol(Protocol.HTTP);
        } else if ("https".equals(protocol)) {
            clientConfiguration.setProtocol(Protocol.HTTPS);
        } else {
            throw new ElasticsearchIllegalArgumentException("No protocol supported [" + protocol + "], can either be [http] or [https]");
        }

        String proxyHost = componentSettings.get("proxy_host");
        if (proxyHost != null) {
            String portString = componentSettings.get("proxy_port", "80");
            Integer proxyPort;
            try {
                proxyPort = Integer.parseInt(portString, 10);
            } catch (NumberFormatException ex) {
                throw new ElasticsearchIllegalArgumentException("The configured proxy port value [" + portString + "] is invalid", ex);
            }
            clientConfiguration.withProxyHost(proxyHost).setProxyPort(proxyPort);
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
        if (componentSettings.get("s3.endpoint") != null) {
            endpoint = componentSettings.get("s3.endpoint");
            logger.debug("using explicit s3 endpoint [{}]", endpoint);
        } else if (componentSettings.get("region") != null) {
            String region = componentSettings.get("region").toLowerCase();
            endpoint = getEndpoint(region);
            logger.debug("using s3 region [{}], with endpoint [{}]", region, endpoint);
        }
        return endpoint;
    }

    private static String getEndpoint(String region) {
        if ("us-east".equals(region)) {
            return "s3.amazonaws.com";
        } else if ("us-east-1".equals(region)) {
            return "s3.amazonaws.com";
        } else if ("us-west".equals(region)) {
            return "s3-us-west-1.amazonaws.com";
        } else if ("us-west-1".equals(region)) {
            return "s3-us-west-1.amazonaws.com";
        } else if ("us-west-2".equals(region)) {
            return "s3-us-west-2.amazonaws.com";
        } else if ("ap-southeast".equals(region)) {
            return "s3-ap-southeast-1.amazonaws.com";
        } else if ("ap-southeast-1".equals(region)) {
            return "s3-ap-southeast-1.amazonaws.com";
        } else if ("ap-southeast-2".equals(region)) {
            return "s3-ap-southeast-2.amazonaws.com";
        } else if ("ap-northeast".equals(region)) {
            return "s3-ap-northeast-1.amazonaws.com";
        } else if ("ap-northeast-1".equals(region)) {
            return "s3-ap-northeast-1.amazonaws.com";
        } else if ("eu-west".equals(region)) {
            return "s3-eu-west-1.amazonaws.com";
        } else if ("eu-west-1".equals(region)) {
            return "s3-eu-west-1.amazonaws.com";
        } else if ("sa-east".equals(region)) {
            return "s3-sa-east-1.amazonaws.com";
        } else if ("sa-east-1".equals(region)) {
            return "s3-sa-east-1.amazonaws.com";
        } else {
            throw new ElasticsearchIllegalArgumentException("No automatic endpoint could be derived from region [" + region + "]");
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
    }
}
