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

import com.amazonaws.Protocol;
import com.amazonaws.services.ec2.AmazonEC2;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

public interface AwsEc2Service {
    Setting<Boolean> AUTO_ATTRIBUTE_SETTING = Setting.boolSetting("cloud.node.auto_attributes", false, Property.NodeScope);

    // Global AWS settings (shared between discovery-ec2 and repository-s3)
    // Each setting starting with `cloud.aws` also exists in repository-s3 project. Don't forget to update
    // the code there if you change anything here.
    /**
     * cloud.aws.access_key: AWS Access key. Shared with repository-s3 plugin
     */
    Setting<String> KEY_SETTING =
        Setting.simpleString("cloud.aws.access_key", Property.NodeScope, Property.Filtered, Property.Shared);
    /**
     * cloud.aws.secret_key: AWS Secret key. Shared with repository-s3 plugin
     */
    Setting<String> SECRET_SETTING =
        Setting.simpleString("cloud.aws.secret_key", Property.NodeScope, Property.Filtered, Property.Shared);
    /**
     * cloud.aws.protocol: Protocol for AWS API: http or https. Defaults to https. Shared with repository-s3 plugin
     */
    Setting<Protocol> PROTOCOL_SETTING = new Setting<>("cloud.aws.protocol", "https", s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)),
        Property.NodeScope, Property.Shared);
    /**
     * cloud.aws.proxy.host: In case of proxy, define its hostname/IP. Shared with repository-s3 plugin
     */
    Setting<String> PROXY_HOST_SETTING = Setting.simpleString("cloud.aws.proxy.host", Property.NodeScope, Property.Shared);
    /**
     * cloud.aws.proxy.port: In case of proxy, define its port. Defaults to 80. Shared with repository-s3 plugin
     */
    Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("cloud.aws.proxy.port", 80, 0, 1<<16, Property.NodeScope,
        Property.Shared);
    /**
     * cloud.aws.proxy.username: In case of proxy with auth, define the username. Shared with repository-s3 plugin
     */
    Setting<String> PROXY_USERNAME_SETTING = Setting.simpleString("cloud.aws.proxy.username", Property.NodeScope, Property.Shared);
    /**
     * cloud.aws.proxy.password: In case of proxy with auth, define the password. Shared with repository-s3 plugin
     */
    Setting<String> PROXY_PASSWORD_SETTING =
        Setting.simpleString("cloud.aws.proxy.password", Property.NodeScope, Property.Filtered, Property.Shared);
    /**
     * cloud.aws.signer: If you are using an old AWS API version, you can define a Signer. Shared with repository-s3 plugin
     */
    Setting<String> SIGNER_SETTING = Setting.simpleString("cloud.aws.signer", Property.NodeScope, Property.Shared);
    /**
     * cloud.aws.region: Region. Shared with repository-s3 plugin
     */
    Setting<String> REGION_SETTING =
        new Setting<>("cloud.aws.region", "", s -> s.toLowerCase(Locale.ROOT), Property.NodeScope, Property.Shared);

    /**
     * Defines specific ec2 settings starting with cloud.aws.ec2.
     */
    interface CLOUD_EC2 {
        /**
         * cloud.aws.ec2.access_key: AWS Access key specific for EC2 API calls. Defaults to cloud.aws.access_key.
         * @see AwsEc2Service#KEY_SETTING
         */
        Setting<String> KEY_SETTING = new Setting<>("cloud.aws.ec2.access_key", AwsEc2Service.KEY_SETTING, Function.identity(),
            Property.NodeScope, Property.Filtered);
        /**
         * cloud.aws.ec2.secret_key: AWS Secret key specific for EC2 API calls. Defaults to cloud.aws.secret_key.
         * @see AwsEc2Service#SECRET_SETTING
         */
        Setting<String> SECRET_SETTING = new Setting<>("cloud.aws.ec2.secret_key", AwsEc2Service.SECRET_SETTING, Function.identity(),
            Property.NodeScope, Property.Filtered);
        /**
         * cloud.aws.ec2.protocol: Protocol for AWS API specific for EC2 API calls: http or https.  Defaults to cloud.aws.protocol.
         * @see AwsEc2Service#PROTOCOL_SETTING
         */
        Setting<Protocol> PROTOCOL_SETTING = new Setting<>("cloud.aws.ec2.protocol", AwsEc2Service.PROTOCOL_SETTING,
            s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope);
        /**
         * cloud.aws.ec2.proxy.host: In case of proxy, define its hostname/IP specific for EC2 API calls. Defaults to cloud.aws.proxy.host.
         * @see AwsEc2Service#PROXY_HOST_SETTING
         */
        Setting<String> PROXY_HOST_SETTING = new Setting<>("cloud.aws.ec2.proxy.host", AwsEc2Service.PROXY_HOST_SETTING,
            Function.identity(), Property.NodeScope);
        /**
         * cloud.aws.ec2.proxy.port: In case of proxy, define its port specific for EC2 API calls.  Defaults to cloud.aws.proxy.port.
         * @see AwsEc2Service#PROXY_PORT_SETTING
         */
        Setting<Integer> PROXY_PORT_SETTING = new Setting<>("cloud.aws.ec2.proxy.port", AwsEc2Service.PROXY_PORT_SETTING,
            s -> Setting.parseInt(s, 0, 1<<16, "cloud.aws.ec2.proxy.port"), Property.NodeScope);
        /**
         * cloud.aws.ec2.proxy.username: In case of proxy with auth, define the username specific for EC2 API calls.
         * Defaults to cloud.aws.proxy.username.
         * @see AwsEc2Service#PROXY_USERNAME_SETTING
         */
        Setting<String> PROXY_USERNAME_SETTING = new Setting<>("cloud.aws.ec2.proxy.username", AwsEc2Service.PROXY_USERNAME_SETTING,
            Function.identity(), Property.NodeScope);
        /**
         * cloud.aws.ec2.proxy.password: In case of proxy with auth, define the password specific for EC2 API calls.
         * Defaults to cloud.aws.proxy.password.
         * @see AwsEc2Service#PROXY_PASSWORD_SETTING
         */
        Setting<String> PROXY_PASSWORD_SETTING = new Setting<>("cloud.aws.ec2.proxy.password", AwsEc2Service.PROXY_PASSWORD_SETTING,
            Function.identity(), Property.NodeScope, Property.Filtered);
        /**
         * cloud.aws.ec2.signer: If you are using an old AWS API version, you can define a Signer. Specific for EC2 API calls.
         * Defaults to cloud.aws.signer.
         * @see AwsEc2Service#SIGNER_SETTING
         */
        Setting<String> SIGNER_SETTING = new Setting<>("cloud.aws.ec2.signer", AwsEc2Service.SIGNER_SETTING, Function.identity(),
            Property.NodeScope);
        /**
         * cloud.aws.ec2.region: Region specific for EC2 API calls. Defaults to cloud.aws.region.
         * @see AwsEc2Service#REGION_SETTING
         */
        Setting<String> REGION_SETTING = new Setting<>("cloud.aws.ec2.region", AwsEc2Service.REGION_SETTING,
            s -> s.toLowerCase(Locale.ROOT), Property.NodeScope);
        /**
         * cloud.aws.ec2.endpoint: Endpoint. If not set, endpoint will be guessed based on region setting.
         */
        Setting<String> ENDPOINT_SETTING = Setting.simpleString("cloud.aws.ec2.endpoint", Property.NodeScope);
    }

    /**
     * Defines discovery settings for ec2. Starting with discovery.ec2.
     */
    interface DISCOVERY_EC2 {
        enum HostType {
            PRIVATE_IP,
            PUBLIC_IP,
            PRIVATE_DNS,
            PUBLIC_DNS
        }

        /**
         * discovery.ec2.host_type: The type of host type to use to communicate with other instances.
         * Can be one of private_ip, public_ip, private_dns, public_dns. Defaults to private_ip.
         */
        Setting<HostType> HOST_TYPE_SETTING =
            new Setting<>("discovery.ec2.host_type", HostType.PRIVATE_IP.name(), s -> HostType.valueOf(s.toUpperCase(Locale.ROOT)),
                Property.NodeScope);
        /**
         * discovery.ec2.any_group: If set to false, will require all security groups to be present for the instance to be used for the
         * discovery. Defaults to true.
         */
        Setting<Boolean> ANY_GROUP_SETTING =
            Setting.boolSetting("discovery.ec2.any_group", true, Property.NodeScope);
        /**
         * discovery.ec2.groups: Either a comma separated list or array based list of (security) groups. Only instances with the provided
         * security groups will be used in the cluster discovery. (NOTE: You could provide either group NAME or group ID.)
         */
        Setting<List<String>> GROUPS_SETTING =
            Setting.listSetting("discovery.ec2.groups", new ArrayList<>(), s -> s.toString(), Property.NodeScope);
        /**
         * discovery.ec2.availability_zones: Either a comma separated list or array based list of availability zones. Only instances within
         * the provided availability zones will be used in the cluster discovery.
         */
        Setting<List<String>> AVAILABILITY_ZONES_SETTING =
            Setting.listSetting("discovery.ec2.availability_zones", Collections.emptyList(), s -> s.toString(),
                Property.NodeScope);
        /**
         * discovery.ec2.node_cache_time: How long the list of hosts is cached to prevent further requests to the AWS API. Defaults to 10s.
         */
        Setting<TimeValue> NODE_CACHE_TIME_SETTING =
            Setting.timeSetting("discovery.ec2.node_cache_time", TimeValue.timeValueSeconds(10), Property.NodeScope);

        /**
         * discovery.ec2.tag.*: The ec2 discovery can filter machines to include in the cluster based on tags (and not just groups).
         * The settings to use include the discovery.ec2.tag. prefix. For example, setting discovery.ec2.tag.stage to dev will only filter
         * instances with a tag key set to stage, and a value of dev. Several tags set will require all of those tags to be set for the
         * instance to be included.
         */
        Setting<Settings> TAG_SETTING = Setting.groupSetting("discovery.ec2.tag.", Property.NodeScope);
    }

    AmazonEC2 client();
}
