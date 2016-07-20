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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import org.elasticsearch.cloud.aws.AwsEc2Service;
import org.elasticsearch.cloud.aws.AwsEc2ServiceImpl;
import org.elasticsearch.cloud.aws.AwsModule;
import org.elasticsearch.cloud.aws.AwsService;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class Ec2CredentialSettingsTests extends ESTestCase {

    public void testAwsSettings() {
        Settings settings = Settings.builder()
                .put(AwsService.CLOUD_AWS.KEY, "aws_key")
                .put(AwsService.CLOUD_AWS.SECRET, "aws_secret")
                .build();

        AWSCredentialsProvider credentials = AwsEc2ServiceImpl.buildCredentials(settings);
        assertThat(credentials.getCredentials().getAWSAccessKeyId(), is("aws_key"));
        assertThat(credentials.getCredentials().getAWSSecretKey(), is("aws_secret"));
    }

    public void testEc2Settings() {
        Settings settings = Settings.builder()
            .put(AwsService.CLOUD_AWS.KEY, "aws_key")
            .put(AwsService.CLOUD_AWS.SECRET, "aws_secret")
            .put(AwsEc2Service.CLOUD_EC2.KEY, "ec2_key")
            .put(AwsEc2Service.CLOUD_EC2.SECRET, "ec2_secret")
            .build();

        AWSCredentialsProvider credentials = AwsEc2ServiceImpl.buildCredentials(settings);
        assertThat(credentials.getCredentials().getAWSAccessKeyId(), is("ec2_key"));
        assertThat(credentials.getCredentials().getAWSSecretKey(), is("ec2_secret"));
    }
}
