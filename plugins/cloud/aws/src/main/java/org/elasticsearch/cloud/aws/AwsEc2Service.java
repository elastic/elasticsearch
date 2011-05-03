/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;

/**
 * @author kimchy (shay.banon)
 */
public class AwsEc2Service extends AbstractLifecycleComponent<AwsEc2Service> {

    private AmazonEC2Client client;

    @Inject public AwsEc2Service(Settings settings, SettingsFilter settingsFilter) {
        super(settings);

        settingsFilter.addFilter(new AwsSettingsFilter());
    }

    public synchronized AmazonEC2 client() {
        if (client != null) {
            return client;
        }

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        String protocol = componentSettings.get("protocol", "http").toLowerCase();
        if ("http".equals(protocol)) {
            clientConfiguration.setProtocol(Protocol.HTTP);
        } else if ("https".equals(protocol)) {
            clientConfiguration.setProtocol(Protocol.HTTPS);
        } else {
            throw new ElasticSearchIllegalArgumentException("No protocol supported [" + protocol + "], can either be [http] or [https]");
        }
        String account = componentSettings.get("access_key", settings.get("cloud.account"));
        String key = componentSettings.get("secret_key", settings.get("cloud.key"));

        if (account == null) {
            throw new ElasticSearchIllegalArgumentException("No s3 access_key defined for s3 gateway");
        }
        if (key == null) {
            throw new ElasticSearchIllegalArgumentException("No s3 secret_key defined for s3 gateway");
        }

        String proxyHost = componentSettings.get("proxy_host");
        if (proxyHost != null) {
            String portString = componentSettings.get("proxy_port", "80");
            Integer proxyPort;
            try {
                proxyPort = Integer.parseInt(portString, 10);
            } catch (NumberFormatException ex) {
                throw new ElasticSearchIllegalArgumentException("The configured proxy port value [" + portString + "] is invalid", ex);
            }
            clientConfiguration.withProxyHost(proxyHost).setProxyPort(proxyPort);
        }

        this.client = new AmazonEC2Client(new BasicAWSCredentials(account, key), clientConfiguration);

        if (componentSettings.get("ec2.endpoint") != null) {
            client.setEndpoint(componentSettings.get("ec2.endpoint"));
        } else if (componentSettings.get("region") != null) {
            String endpoint;
            String region = componentSettings.get("region");
            if ("us-east".equals(region.toLowerCase())) {
                endpoint = "ec2.us-east-1.amazonaws.com";
            } else if ("us-east-1".equals(region.toLowerCase())) {
                endpoint = "ec2.us-east-1.amazonaws.com";
            } else if ("us-west".equals(region.toLowerCase())) {
                endpoint = "ec2.us-west-1.amazonaws.com";
            } else if ("us-west-1".equals(region.toLowerCase())) {
                endpoint = "ec2.us-west-1.amazonaws.com";
            } else if ("ap-southeast".equals(region.toLowerCase())) {
                endpoint = "ec2.ap-southeast-1.amazonaws.com";
            } else if ("ap-southeast-1".equals(region.toLowerCase())) {
                endpoint = "ec2.ap-southeast-1.amazonaws.com";
            } else if ("eu-west".equals(region.toLowerCase())) {
                endpoint = "ec2.eu-west-1.amazonaws.com";
            } else if ("eu-west-1".equals(region.toLowerCase())) {
                endpoint = "ec2.eu-west-1.amazonaws.com";
            } else {
                throw new ElasticSearchIllegalArgumentException("No automatic endpoint could be derived from region [" + region + "]");
            }
            if (endpoint != null) {
                client.setEndpoint(endpoint);
            }
        }

        return this.client;

    }

    @Override protected void doStart() throws ElasticSearchException {
    }

    @Override protected void doStop() throws ElasticSearchException {
    }

    @Override protected void doClose() throws ElasticSearchException {
        if (client != null) {
            client.shutdown();
        }
    }
}
