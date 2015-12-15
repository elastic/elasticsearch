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

import com.amazonaws.services.ec2.AmazonEC2;
import org.elasticsearch.common.component.LifecycleComponent;

public interface AwsEc2Service extends LifecycleComponent<AwsEc2Service> {
    final class CLOUD_AWS {
        public static final String KEY = "cloud.aws.access_key";
        public static final String SECRET = "cloud.aws.secret_key";
        public static final String PROTOCOL = "cloud.aws.protocol";
        public static final String PROXY_HOST = "cloud.aws.proxy.host";
        public static final String PROXY_PORT = "cloud.aws.proxy.port";
        public static final String PROXY_USERNAME = "cloud.aws.proxy.username";
        public static final String PROXY_PASSWORD = "cloud.aws.proxy.password";
        public static final String SIGNER = "cloud.aws.signer";
        public static final String REGION = "cloud.aws.region";
        @Deprecated
        public static final String DEPRECATED_PROXY_HOST = "cloud.aws.proxy_host";
        @Deprecated
        public static final String DEPRECATED_PROXY_PORT = "cloud.aws.proxy_port";
    }

    final class CLOUD_EC2 {
        public static final String KEY = "cloud.aws.ec2.access_key";
        public static final String SECRET = "cloud.aws.ec2.secret_key";
        public static final String PROTOCOL = "cloud.aws.ec2.protocol";
        public static final String PROXY_HOST = "cloud.aws.ec2.proxy.host";
        public static final String PROXY_PORT = "cloud.aws.ec2.proxy.port";
        public static final String PROXY_USERNAME = "cloud.aws.ec2.proxy.username";
        public static final String PROXY_PASSWORD = "cloud.aws.ec2.proxy.password";
        public static final String SIGNER = "cloud.aws.ec2.signer";
        public static final String ENDPOINT = "cloud.aws.ec2.endpoint";
        @Deprecated
        public static final String DEPRECATED_PROXY_HOST = "cloud.aws.ec2.proxy_host";
        @Deprecated
        public static final String DEPRECATED_PROXY_PORT = "cloud.aws.ec2.proxy_port";
    }

    final class DISCOVERY_EC2 {
        public static final String HOST_TYPE = "discovery.ec2.host_type";
        public static final String ANY_GROUP = "discovery.ec2.any_group";
        public static final String GROUPS = "discovery.ec2.groups";
        public static final String TAG_PREFIX = "discovery.ec2.tag.";
        public static final String AVAILABILITY_ZONES = "discovery.ec2.availability_zones";
        public static final String NODE_CACHE_TIME = "discovery.ec2.node_cache_time";
    }

    AmazonEC2 client();
}
