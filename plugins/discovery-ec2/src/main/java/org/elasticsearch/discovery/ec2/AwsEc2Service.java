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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.ec2.AmazonEC2;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

interface AwsEc2Service {
    Setting<Boolean> AUTO_ATTRIBUTE_SETTING = Setting.boolSetting("cloud.node.auto_attributes", false, Property.NodeScope);

    // Global AWS settings (shared between discovery-ec2 and repository-s3)
    // Each setting starting with `cloud.aws` also exists in repository-s3 project. Don't forget to update
    // the code there if you change anything here.
    /**
     * cloud.aws.access_key: AWS Access key. Shared with repository-s3 plugin
     */
    Setting<SecureString> KEY_SETTING = new Setting<>("cloud.aws.access_key", "", SecureString::new,
            Property.NodeScope, Property.Filtered, Property.Shared, Property.Deprecated);
    /**
     * cloud.aws.secret_key: AWS Secret key. Shared with repository-s3 plugin
     */
    Setting<SecureString> SECRET_SETTING = new Setting<>("cloud.aws.secret_key", "", SecureString::new,
        Property.NodeScope, Property.Filtered, Property.Shared, Property.Deprecated);
    /**
     * cloud.aws.protocol: Protocol for AWS API: http or https. Defaults to https. Shared with repository-s3 plugin
     */
    Setting<Protocol> PROTOCOL_SETTING = new Setting<>("cloud.aws.protocol", "https", s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)),
        Property.NodeScope, Property.Shared, Property.Deprecated);
    /**
     * cloud.aws.proxy.host: In case of proxy, define its hostname/IP. Shared with repository-s3 plugin
     */
    Setting<String> PROXY_HOST_SETTING = Setting.simpleString("cloud.aws.proxy.host",
        Property.NodeScope, Property.Shared, Property.Deprecated);
    /**
     * cloud.aws.proxy.port: In case of proxy, define its port. Defaults to 80. Shared with repository-s3 plugin
     */
    Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("cloud.aws.proxy.port", 80, 0, 1<<16,
        Property.NodeScope, Property.Shared, Property.Deprecated);
    /**
     * cloud.aws.proxy.username: In case of proxy with auth, define the username. Shared with repository-s3 plugin
     */
    Setting<SecureString> PROXY_USERNAME_SETTING = new Setting<>("cloud.aws.proxy.username", "", SecureString::new,
        Property.NodeScope, Property.Filtered, Property.Shared, Property.Deprecated);
    /**
     * cloud.aws.proxy.password: In case of proxy with auth, define the password. Shared with repository-s3 plugin
     */
    Setting<SecureString> PROXY_PASSWORD_SETTING = new Setting<>("cloud.aws.proxy.password", "", SecureString::new,
        Property.NodeScope, Property.Filtered, Property.Shared, Property.Deprecated);
    /**
     * cloud.aws.signer: If you are using an old AWS API version, you can define a Signer. Shared with repository-s3 plugin
     */
    Setting<String> SIGNER_SETTING = Setting.simpleString("cloud.aws.signer", Property.NodeScope, Property.Shared, Property.Deprecated);
    /**
     * cloud.aws.region: Region. Shared with repository-s3 plugin
     */
    Setting<String> REGION_SETTING =
        new Setting<>("cloud.aws.region", "", s -> s.toLowerCase(Locale.ROOT), Property.NodeScope, Property.Shared, Property.Deprecated);
    /**
     * cloud.aws.read_timeout: Socket read timeout. Shared with repository-s3 plugin
     */
    Setting<TimeValue> READ_TIMEOUT = Setting.timeSetting("cloud.aws.read_timeout",
        TimeValue.timeValueMillis(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT), Property.NodeScope, Property.Shared, Property.Deprecated);

    /**
     * Defines specific ec2 settings starting with cloud.aws.ec2.
     */
    interface CLOUD_EC2 {
        /**
         * cloud.aws.ec2.access_key: AWS Access key specific for EC2 API calls. Defaults to cloud.aws.access_key.
         * @see AwsEc2Service#KEY_SETTING
         */
        Setting<SecureString> KEY_SETTING = new Setting<>("cloud.aws.ec2.access_key", AwsEc2Service.KEY_SETTING,
            SecureString::new, Property.NodeScope, Property.Filtered, Property.Deprecated);

        /**
         * cloud.aws.ec2.secret_key: AWS Secret key specific for EC2 API calls. Defaults to cloud.aws.secret_key.
         * @see AwsEc2Service#SECRET_SETTING
         */
        Setting<SecureString> SECRET_SETTING = new Setting<>("cloud.aws.ec2.secret_key", AwsEc2Service.SECRET_SETTING,
            SecureString::new, Property.NodeScope, Property.Filtered, Property.Deprecated);
        /**
         * cloud.aws.ec2.protocol: Protocol for AWS API specific for EC2 API calls: http or https.  Defaults to cloud.aws.protocol.
         * @see AwsEc2Service#PROTOCOL_SETTING
         */
        Setting<Protocol> PROTOCOL_SETTING = new Setting<>("cloud.aws.ec2.protocol", AwsEc2Service.PROTOCOL_SETTING,
            s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope, Property.Deprecated);
        /**
         * cloud.aws.ec2.proxy.host: In case of proxy, define its hostname/IP specific for EC2 API calls. Defaults to cloud.aws.proxy.host.
         * @see AwsEc2Service#PROXY_HOST_SETTING
         */
        Setting<String> PROXY_HOST_SETTING = new Setting<>("cloud.aws.ec2.proxy.host", AwsEc2Service.PROXY_HOST_SETTING,
            Function.identity(), Property.NodeScope, Property.Deprecated);
        /**
         * cloud.aws.ec2.proxy.port: In case of proxy, define its port specific for EC2 API calls.  Defaults to cloud.aws.proxy.port.
         * @see AwsEc2Service#PROXY_PORT_SETTING
         */
        Setting<Integer> PROXY_PORT_SETTING = new Setting<>("cloud.aws.ec2.proxy.port", AwsEc2Service.PROXY_PORT_SETTING,
            s -> Setting.parseInt(s, 0, 1<<16, "cloud.aws.ec2.proxy.port"), Property.NodeScope, Property.Deprecated);
        /**
         * cloud.aws.ec2.proxy.username: In case of proxy with auth, define the username specific for EC2 API calls.
         * Defaults to cloud.aws.proxy.username.
         * @see AwsEc2Service#PROXY_USERNAME_SETTING
         */
        Setting<SecureString> PROXY_USERNAME_SETTING = new Setting<>("cloud.aws.ec2.proxy.username", AwsEc2Service.PROXY_USERNAME_SETTING,
            SecureString::new, Property.NodeScope, Property.Filtered, Property.Deprecated);
        /**
         * cloud.aws.ec2.proxy.password: In case of proxy with auth, define the password specific for EC2 API calls.
         * Defaults to cloud.aws.proxy.password.
         * @see AwsEc2Service#PROXY_PASSWORD_SETTING
         */
        Setting<SecureString> PROXY_PASSWORD_SETTING = new Setting<>("cloud.aws.ec2.proxy.password", AwsEc2Service.PROXY_PASSWORD_SETTING,
            SecureString::new, Property.NodeScope, Property.Filtered, Property.Deprecated);
        /**
         * cloud.aws.ec2.signer: If you are using an old AWS API version, you can define a Signer. Specific for EC2 API calls.
         * Defaults to cloud.aws.signer.
         * @see AwsEc2Service#SIGNER_SETTING
         */
        Setting<String> SIGNER_SETTING = new Setting<>("cloud.aws.ec2.signer", AwsEc2Service.SIGNER_SETTING, Function.identity(),
            Property.NodeScope, Property.Deprecated);
        /**
         * cloud.aws.ec2.region: Region specific for EC2 API calls. Defaults to cloud.aws.region.
         * @see AwsEc2Service#REGION_SETTING
         */
        Setting<String> REGION_SETTING = new Setting<>("cloud.aws.ec2.region", AwsEc2Service.REGION_SETTING,
            s -> s.toLowerCase(Locale.ROOT), Property.NodeScope, Property.Deprecated);
        /**
         * cloud.aws.ec2.endpoint: Endpoint. If not set, endpoint will be guessed based on region setting.
         */
        Setting<String> ENDPOINT_SETTING = Setting.simpleString("cloud.aws.ec2.endpoint", Property.NodeScope, Property.Deprecated);
        /**
         * cloud.aws.ec2.read_timeout: Socket read timeout. Defaults to cloud.aws.read_timeout
         * @see AwsEc2Service#READ_TIMEOUT
         */
        Setting<TimeValue> READ_TIMEOUT =
            Setting.timeSetting("cloud.aws.ec2.read_timeout", AwsEc2Service.READ_TIMEOUT, Property.NodeScope, Property.Deprecated);
    }

    /**
     * Defines discovery settings for ec2. Starting with discovery.ec2.
     */
    interface DISCOVERY_EC2 {
        class HostType {
            public static final String PRIVATE_IP = "private_ip";
            public static final String PUBLIC_IP = "public_ip";
            public static final String PRIVATE_DNS = "private_dns";
            public static final String PUBLIC_DNS = "public_dns";
            public static final String TAG_PREFIX = "tag:";
        }

        /** The access key (ie login id) for connecting to ec2. */
        Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.secureString("discovery.ec2.access_key", CLOUD_EC2.KEY_SETTING);

        /** The secret key (ie password) for connecting to ec2. */
        Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.secureString("discovery.ec2.secret_key", CLOUD_EC2.SECRET_SETTING);

        /** An override for the ec2 endpoint to connect to. */
        Setting<String> ENDPOINT_SETTING = new Setting<>("discovery.ec2.endpoint", CLOUD_EC2.ENDPOINT_SETTING,
            s -> s.toLowerCase(Locale.ROOT), Setting.Property.NodeScope);

        /** The protocol to use to connect to to ec2. */
        Setting<Protocol> PROTOCOL_SETTING = new Setting<>("discovery.ec2.protocol", CLOUD_EC2.PROTOCOL_SETTING,
            s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), Setting.Property.NodeScope);

        /** The host name of a proxy to connect to ec2 through. */
        Setting<String> PROXY_HOST_SETTING = new Setting<>("discovery.ec2.proxy.host", CLOUD_EC2.PROXY_HOST_SETTING,
            Function.identity(), Setting.Property.NodeScope);

        /** The port of a proxy to connect to ec2 through. */
        Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("discovery.ec2.proxy.port", CLOUD_EC2.PROXY_PORT_SETTING,
            0, Setting.Property.NodeScope);

        /** The username of a proxy to connect to s3 through. */
        Setting<SecureString> PROXY_USERNAME_SETTING = SecureSetting.secureString("discovery.ec2.proxy.username",
            CLOUD_EC2.PROXY_USERNAME_SETTING);

        /** The password of a proxy to connect to s3 through. */
        Setting<SecureString> PROXY_PASSWORD_SETTING =  SecureSetting.secureString("discovery.ec2.proxy.password",
            CLOUD_EC2.PROXY_PASSWORD_SETTING);

        /** The socket timeout for connecting to s3. */
        Setting<TimeValue> READ_TIMEOUT_SETTING = Setting.timeSetting("discovery.ec2.read_timeout",
            CLOUD_EC2.READ_TIMEOUT, Setting.Property.NodeScope);

        /**
         * discovery.ec2.host_type: The type of host type to use to communicate with other instances.
         * Can be one of private_ip, public_ip, private_dns, public_dns or tag:XXXX where
         * XXXX refers to a name of a tag configured for all EC2 instances. Instances which don't
         * have this tag set will be ignored by the discovery process. Defaults to private_ip.
         */
        Setting<String> HOST_TYPE_SETTING =
            new Setting<>("discovery.ec2.host_type", HostType.PRIVATE_IP, Function.identity(), Property.NodeScope);
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
