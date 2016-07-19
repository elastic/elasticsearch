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

package org.elasticsearch.cloud.azure.arm;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;

public interface AzureManagementService {

    /**
     * Host Type
     */
    public enum HostType {
        PRIVATE_IP("private_ip"),
        PUBLIC_IP("public_ip");

        private String type ;

        HostType(String type) {
            this.type = type ;
        }

        public static HostType fromString(String type) {
            for (HostType hostType : values()) {
                if (hostType.type.equalsIgnoreCase(type)) {
                    return hostType;
                }
            }
            throw new IllegalArgumentException("invalid value for host type [" + type + "]");
        }
    }

    /**
     * Settings for authentication to access Azure ARM APIs.
     * Prefixed by "cloud.azure-arm.management"
     */
    final class Management {
        /**
         * Azure ARM subscription Id
         */
        public static final Setting<String> SUBSCRIPTION_ID_SETTING =
            Setting.simpleString("cloud.azure-arm.subscription_id", Property.NodeScope, Property.Filtered);
        /**
         * Azure ARM client Id
         */
        public static final Setting<String> CLIENT_ID_SETTING =
            Setting.simpleString("cloud.azure-arm.client_id", Property.NodeScope, Property.Filtered);
        /**
         * Azure ARM secret
         */
        public static final Setting<String> SECRET_SETTING =
            Setting.simpleString("cloud.azure-arm.secret", Property.NodeScope, Property.Filtered);
        /**
         * Azure ARM tenant Id
         */
        public static final Setting<String> TENANT_ID_SETTING =
            Setting.simpleString("cloud.azure-arm.tenant_id", Property.NodeScope, Property.Filtered);
    }

    /**
     * Settings for vms discovery.
     * Prefixed by "discovery.azure-arm"
     */
    final class Discovery {
        public static final Setting<TimeValue> REFRESH_SETTING =
            Setting.positiveTimeSetting("discovery.azure-arm.refresh_interval", TimeValue.timeValueSeconds(0),
                Property.NodeScope, Property.Dynamic);
        public static final Setting<HostType> HOST_TYPE_SETTING =
            new Setting<>("discovery.azure-arm.host.type", HostType.PRIVATE_IP.name(),
                HostType::fromString, Property.NodeScope, Property.Dynamic);
        public static final Setting<String> HOST_NAME_SETTING = Setting.simpleString("discovery.azure-arm.host.name",
            Property.NodeScope, Property.Dynamic);
        public static final Setting<String> HOST_GROUP_NAME_SETTING = Setting.simpleString("discovery.azure-arm.host.group_name",
            Property.NodeScope, Property.Dynamic);
        public static final Setting<String> REGION_SETTING = Setting.simpleString("discovery.azure-arm.region",
            Property.NodeScope, Property.Dynamic);
    }

    /**
     * Get the list of azure virtual machines
     * @param groupName If provided (not null), we can filter by group name
     * @return a list of azure virtual machines
     */
    List<AzureVirtualMachine> getVirtualMachines(String groupName);
}
