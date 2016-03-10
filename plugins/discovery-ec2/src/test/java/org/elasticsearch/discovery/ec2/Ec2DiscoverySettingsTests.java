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

package org.elasticsearch.discovery.ec2;

import com.amazonaws.Protocol;
import org.elasticsearch.cloud.aws.AwsEc2Service;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;

public class Ec2DiscoverySettingsTests extends ESTestCase {

    private static final Settings AWS = Settings.builder()
        .put(AwsEc2Service.KEY_SETTING.getKey(), "global-key")
        .put(AwsEc2Service.SECRET_SETTING.getKey(), "global-secret")
        .put(AwsEc2Service.PROTOCOL_SETTING.getKey(), "https")
        .put(AwsEc2Service.PROXY_HOST_SETTING.getKey(), "global-proxy-host")
        .put(AwsEc2Service.PROXY_PORT_SETTING.getKey(), 10000)
        .put(AwsEc2Service.PROXY_USERNAME_SETTING.getKey(), "global-proxy-username")
        .put(AwsEc2Service.PROXY_PASSWORD_SETTING.getKey(), "global-proxy-password")
        .put(AwsEc2Service.SIGNER_SETTING.getKey(), "global-signer")
        .put(AwsEc2Service.REGION_SETTING.getKey(), "global-region")
        .build();

    private static final Settings EC2 = Settings.builder()
        .put(AwsEc2Service.CLOUD_EC2.KEY_SETTING.getKey(), "ec2-key")
        .put(AwsEc2Service.CLOUD_EC2.SECRET_SETTING.getKey(), "ec2-secret")
        .put(AwsEc2Service.CLOUD_EC2.PROTOCOL_SETTING.getKey(), "http")
        .put(AwsEc2Service.CLOUD_EC2.PROXY_HOST_SETTING.getKey(), "ec2-proxy-host")
        .put(AwsEc2Service.CLOUD_EC2.PROXY_PORT_SETTING.getKey(), 20000)
        .put(AwsEc2Service.CLOUD_EC2.PROXY_USERNAME_SETTING.getKey(), "ec2-proxy-username")
        .put(AwsEc2Service.CLOUD_EC2.PROXY_PASSWORD_SETTING.getKey(), "ec2-proxy-password")
        .put(AwsEc2Service.CLOUD_EC2.SIGNER_SETTING.getKey(), "ec2-signer")
        .put(AwsEc2Service.CLOUD_EC2.REGION_SETTING.getKey(), "ec2-region")
        .put(AwsEc2Service.CLOUD_EC2.ENDPOINT_SETTING.getKey(), "ec2-endpoint")
        .build();

    /**
     * We test when only cloud.aws settings are set
     */
    public void testRepositorySettingsGlobalOnly() {
        Settings nodeSettings = buildSettings(AWS);
        assertThat(AwsEc2Service.CLOUD_EC2.KEY_SETTING.get(nodeSettings), is("global-key"));
        assertThat(AwsEc2Service.CLOUD_EC2.SECRET_SETTING.get(nodeSettings), is("global-secret"));
        assertThat(AwsEc2Service.CLOUD_EC2.PROTOCOL_SETTING.get(nodeSettings), is(Protocol.HTTPS));
        assertThat(AwsEc2Service.CLOUD_EC2.PROXY_HOST_SETTING.get(nodeSettings), is("global-proxy-host"));
        assertThat(AwsEc2Service.CLOUD_EC2.PROXY_PORT_SETTING.get(nodeSettings), is(10000));
        assertThat(AwsEc2Service.CLOUD_EC2.PROXY_USERNAME_SETTING.get(nodeSettings), is("global-proxy-username"));
        assertThat(AwsEc2Service.CLOUD_EC2.PROXY_PASSWORD_SETTING.get(nodeSettings), is("global-proxy-password"));
        assertThat(AwsEc2Service.CLOUD_EC2.SIGNER_SETTING.get(nodeSettings), is("global-signer"));
        assertThat(AwsEc2Service.CLOUD_EC2.REGION_SETTING.get(nodeSettings), is("global-region"));
        assertThat(AwsEc2Service.CLOUD_EC2.ENDPOINT_SETTING.get(nodeSettings), isEmptyString());
    }

    /**
     * We test when cloud.aws settings are overloaded by cloud.aws.ec2 settings
     */
    public void testRepositorySettingsGlobalOverloadedByEC2() {
        Settings nodeSettings = buildSettings(AWS, EC2);
        assertThat(AwsEc2Service.CLOUD_EC2.KEY_SETTING.get(nodeSettings), is("ec2-key"));
        assertThat(AwsEc2Service.CLOUD_EC2.SECRET_SETTING.get(nodeSettings), is("ec2-secret"));
        assertThat(AwsEc2Service.CLOUD_EC2.PROTOCOL_SETTING.get(nodeSettings), is(Protocol.HTTP));
        assertThat(AwsEc2Service.CLOUD_EC2.PROXY_HOST_SETTING.get(nodeSettings), is("ec2-proxy-host"));
        assertThat(AwsEc2Service.CLOUD_EC2.PROXY_PORT_SETTING.get(nodeSettings), is(20000));
        assertThat(AwsEc2Service.CLOUD_EC2.PROXY_USERNAME_SETTING.get(nodeSettings), is("ec2-proxy-username"));
        assertThat(AwsEc2Service.CLOUD_EC2.PROXY_PASSWORD_SETTING.get(nodeSettings), is("ec2-proxy-password"));
        assertThat(AwsEc2Service.CLOUD_EC2.SIGNER_SETTING.get(nodeSettings), is("ec2-signer"));
        assertThat(AwsEc2Service.CLOUD_EC2.REGION_SETTING.get(nodeSettings), is("ec2-region"));
        assertThat(AwsEc2Service.CLOUD_EC2.ENDPOINT_SETTING.get(nodeSettings), is("ec2-endpoint"));
    }

    private Settings buildSettings(Settings... global) {
        Settings.Builder builder = Settings.builder();
        for (Settings settings : global) {
            builder.put(settings);
        }
        return builder.build();
    }
}
