/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.ec2;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.TimeValue;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

interface AwsEc2Service extends Closeable {
    Setting<Boolean> AUTO_ATTRIBUTE_SETTING = Setting.boolSetting("cloud.node.auto_attributes", false, Property.NodeScope);

    class HostType {
        public static final String PRIVATE_IP = "private_ip";
        public static final String PUBLIC_IP = "public_ip";
        public static final String PRIVATE_DNS = "private_dns";
        public static final String PUBLIC_DNS = "public_dns";
        public static final String TAG_PREFIX = "tag:";
    }

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
    Setting<Boolean> ANY_GROUP_SETTING = Setting.boolSetting("discovery.ec2.any_group", true, Property.NodeScope);
    /**
     * discovery.ec2.groups: Either a comma separated list or array based list of (security) groups. Only instances with the provided
     * security groups will be used in the cluster discovery. (NOTE: You could provide either group NAME or group ID.)
     */
    Setting<List<String>> GROUPS_SETTING = Setting.listSetting("discovery.ec2.groups", new ArrayList<>(), s -> s.toString(),
            Property.NodeScope);
    /**
     * discovery.ec2.availability_zones: Either a comma separated list or array based list of availability zones. Only instances within
     * the provided availability zones will be used in the cluster discovery.
     */
    Setting<List<String>> AVAILABILITY_ZONES_SETTING = Setting.listSetting("discovery.ec2.availability_zones", Collections.emptyList(),
            s -> s.toString(), Property.NodeScope);
    /**
     * discovery.ec2.node_cache_time: How long the list of hosts is cached to prevent further requests to the AWS API. Defaults to 10s.
     */
    Setting<TimeValue> NODE_CACHE_TIME_SETTING = Setting.timeSetting("discovery.ec2.node_cache_time", TimeValue.timeValueSeconds(10),
            Property.NodeScope);

    /**
     * discovery.ec2.tag.*: The ec2 discovery can filter machines to include in the cluster based on tags (and not just groups).
     * The settings to use include the discovery.ec2.tag. prefix. For example, setting discovery.ec2.tag.stage to dev will only filter
     * instances with a tag key set to stage, and a value of dev. Several tags set will require all of those tags to be set for the
     * instance to be included.
     */
    Setting.AffixSetting<List<String>> TAG_SETTING = Setting.prefixKeySetting("discovery.ec2.tag.",
            key -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), Property.NodeScope));

    /**
     * Builds then caches an {@code AmazonEC2} client using the current client
     * settings. Returns an {@code AmazonEc2Reference} wrapper which should be
     * released as soon as it is not required anymore.
     */
    AmazonEc2Reference client();

    /**
     * Updates the settings for building the client and releases the cached one.
     * Future client requests will use the new settings to lazily built the new
     * client.
     *
     * @param clientSettings the new refreshed settings
     */
    void refreshAndClearCache(Ec2ClientSettings clientSettings);

}
