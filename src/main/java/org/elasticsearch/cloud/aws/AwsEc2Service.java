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
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.cloud.aws.network.Ec2NameResolver;
import org.elasticsearch.cloud.aws.node.Ec2CustomNodeAttributes;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;

/**
 *
 */
public class AwsEc2Service extends AbstractLifecycleComponent<AwsEc2Service> {

    public static final String EC2_METADATA_URL = "http://169.254.169.254/latest/meta-data/";

    private AmazonEC2Client client;

    @Inject
    public AwsEc2Service(Settings settings, SettingsFilter settingsFilter, NetworkService networkService, DiscoveryNodeService discoveryNodeService) {
        super(settings);
        settingsFilter.addFilter(new AwsSettingsFilter());
        // add specific ec2 name resolver
        networkService.addCustomNameResolver(new Ec2NameResolver(settings));
        discoveryNodeService.addCustomAttributeProvider(new Ec2CustomNodeAttributes(settings));
    }

    public synchronized AmazonEC2 client() {
        if (client != null) {
            return client;
        }

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        String protocol = componentSettings.get("protocol", "https").toLowerCase();
        protocol = componentSettings.get("ec2.protocol", protocol).toLowerCase();
        if ("http".equals(protocol)) {
            clientConfiguration.setProtocol(Protocol.HTTP);
        } else if ("https".equals(protocol)) {
            clientConfiguration.setProtocol(Protocol.HTTPS);
        } else {
            throw new ElasticsearchIllegalArgumentException("No protocol supported [" + protocol + "], can either be [http] or [https]");
        }
        String account = componentSettings.get("access_key", settings.get("cloud.account"));
        String key = componentSettings.get("secret_key", settings.get("cloud.key"));

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

        this.client = new AmazonEC2Client(credentials, clientConfiguration);

        if (componentSettings.get("ec2.endpoint") != null) {
            String endpoint = componentSettings.get("ec2.endpoint");
            logger.debug("using explicit ec2 endpoint [{}]", endpoint);
            client.setEndpoint(endpoint);
        } else if (componentSettings.get("region") != null) {
            String region = componentSettings.get("region").toLowerCase();
            String endpoint;
            if (region.equals("us-east-1") || region.equals("us-east")) {
                endpoint = "ec2.us-east-1.amazonaws.com";
            } else if (region.equals("us-west") || region.equals("us-west-1")) {
                endpoint = "ec2.us-west-1.amazonaws.com";
            } else if (region.equals("us-west-2")) {
                endpoint = "ec2.us-west-2.amazonaws.com";
            } else if (region.equals("ap-southeast") || region.equals("ap-southeast-1")) {
                endpoint = "ec2.ap-southeast-1.amazonaws.com";
            } else if (region.equals("ap-southeast-2")) {
                endpoint = "ec2.ap-southeast-2.amazonaws.com";
            } else if (region.equals("ap-northeast") || region.equals("ap-northeast-1")) {
                endpoint = "ec2.ap-northeast-1.amazonaws.com";
            } else if (region.equals("eu-west") || region.equals("eu-west-1")) {
                endpoint = "ec2.eu-west-1.amazonaws.com";
            } else if (region.equals("sa-east") || region.equals("sa-east-1")) {
                endpoint = "ec2.sa-east-1.amazonaws.com";
            } else {
                throw new ElasticsearchIllegalArgumentException("No automatic endpoint could be derived from region [" + region + "]");
            }
            if (endpoint != null) {
                logger.debug("using ec2 region [{}], with endpoint [{}]", region, endpoint);
                client.setEndpoint(endpoint);
            }
        }

        return this.client;

    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        if (client != null) {
            client.shutdown();
        }
    }
}
