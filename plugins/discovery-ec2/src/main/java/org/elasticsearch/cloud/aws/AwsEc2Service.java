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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

public interface AwsEc2Service {
    Setting<Boolean> AUTO_ATTRIBUTE_SETTING = Setting.boolSetting("cloud.node.auto_attributes", false, false, Setting.Scope.CLUSTER);

    // Global AWS settings (shared between discovery-ec2 and repository-s3)
    // Each setting starting with `cloud.aws` also exists in repository-s3 project. Don't forget to update
    // the code there if you change anything here.
    Setting<String> KEY_SETTING = Setting.simpleString("cloud.aws.access_key", false, Setting.Scope.CLUSTER);
    Setting<String> SECRET_SETTING = Setting.simpleString("cloud.aws.secret_key", false, Setting.Scope.CLUSTER);
    Setting<Protocol> PROTOCOL_SETTING = new Setting<>("cloud.aws.protocol", "https", s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)),
        false, Setting.Scope.CLUSTER);
    Setting<String> PROXY_HOST_SETTING = Setting.simpleString("cloud.aws.proxy.host", false, Setting.Scope.CLUSTER);
    Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("cloud.aws.proxy.port", 80, 0, 1<<16, false, Setting.Scope.CLUSTER);
    Setting<String> PROXY_USERNAME_SETTING = Setting.simpleString("cloud.aws.proxy.username", false, Setting.Scope.CLUSTER);
    Setting<String> PROXY_PASSWORD_SETTING = Setting.simpleString("cloud.aws.proxy.password", false, Setting.Scope.CLUSTER);
    Setting<String> SIGNER_SETTING = Setting.simpleString("cloud.aws.signer", false, Setting.Scope.CLUSTER);
    Setting<String> REGION_SETTING = new Setting<>("cloud.aws.region", "", s -> s.toLowerCase(Locale.ROOT), false, Setting.Scope.CLUSTER);

    interface CLOUD_EC2 {
        Setting<String> KEY_SETTING = new Setting<>("cloud.aws.ec2.access_key", AwsEc2Service.KEY_SETTING, Function.identity(), false,
            Setting.Scope.CLUSTER);
        Setting<String> SECRET_SETTING = new Setting<>("cloud.aws.ec2.secret_key", AwsEc2Service.SECRET_SETTING, Function.identity(), false,
            Setting.Scope.CLUSTER);
        Setting<Protocol> PROTOCOL_SETTING = new Setting<>("cloud.aws.ec2.protocol", AwsEc2Service.PROTOCOL_SETTING,
            s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), false, Setting.Scope.CLUSTER);
        Setting<String> PROXY_HOST_SETTING = new Setting<>("cloud.aws.ec2.proxy.host", AwsEc2Service.PROXY_HOST_SETTING,
            Function.identity(), false, Setting.Scope.CLUSTER);
        Setting<Integer> PROXY_PORT_SETTING = new Setting<>("cloud.aws.ec2.proxy.port", AwsEc2Service.PROXY_PORT_SETTING,
            s -> Setting.parseInt(s, 0, 1<<16, "cloud.aws.ec2.proxy.port"), false, Setting.Scope.CLUSTER);
        Setting<String> PROXY_USERNAME_SETTING = new Setting<>("cloud.aws.ec2.proxy.username", AwsEc2Service.PROXY_USERNAME_SETTING,
            Function.identity(), false, Setting.Scope.CLUSTER);
        Setting<String> PROXY_PASSWORD_SETTING = new Setting<>("cloud.aws.ec2.proxy.password", AwsEc2Service.PROXY_PASSWORD_SETTING,
            Function.identity(), false, Setting.Scope.CLUSTER);
        Setting<String> SIGNER_SETTING = new Setting<>("cloud.aws.ec2.signer", AwsEc2Service.SIGNER_SETTING, Function.identity(),
            false, Setting.Scope.CLUSTER);
        Setting<String> REGION_SETTING = new Setting<>("cloud.aws.ec2.region", AwsEc2Service.REGION_SETTING,
            s -> s.toLowerCase(Locale.ROOT), false, Setting.Scope.CLUSTER);
        Setting<String> ENDPOINT_SETTING = Setting.simpleString("cloud.aws.ec2.endpoint", false, Setting.Scope.CLUSTER);
    }

    interface DISCOVERY_EC2 {
        enum HostType {
            PRIVATE_IP,
            PUBLIC_IP,
            PRIVATE_DNS,
            PUBLIC_DNS
        }

        Setting<HostType> HOST_TYPE_SETTING =
            new Setting<>("discovery.ec2.host_type", HostType.PRIVATE_IP.name(), s -> HostType.valueOf(s.toUpperCase(Locale.ROOT)), false,
                Setting.Scope.CLUSTER);
        Setting<Boolean> ANY_GROUP_SETTING =
            Setting.boolSetting("discovery.ec2.any_group", true, false, Setting.Scope.CLUSTER);
        Setting<List<String>> GROUPS_SETTING =
            Setting.listSetting("discovery.ec2.groups", new ArrayList<>(), s -> s.toString(), false, Setting.Scope.CLUSTER);
        Setting<List<String>> AVAILABILITY_ZONES_SETTING =
            Setting.listSetting("discovery.ec2.availability_zones", Collections.emptyList(), s -> s.toString(), false,
                Setting.Scope.CLUSTER);
        Setting<TimeValue> NODE_CACHE_TIME_SETTING =
            Setting.timeSetting("discovery.ec2.node_cache_time", TimeValue.timeValueSeconds(10), false, Setting.Scope.CLUSTER);

        Setting<Settings> TAG_SETTING = Setting.groupSetting("discovery.ec2.tag.", false,Setting.Scope.CLUSTER);
    }

    AmazonEC2 client();
}
