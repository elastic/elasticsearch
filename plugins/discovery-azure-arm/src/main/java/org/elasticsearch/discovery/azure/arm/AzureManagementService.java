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

package org.elasticsearch.discovery.azure.arm;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;

public interface AzureManagementService {

    /**
     * Host Type
     */
    enum HostType {
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
     * Azure ARM subscription Id: "discovery.azure-arm.subscription_id"
     */
    Setting<SecureString> SUBSCRIPTION_ID_SETTING = SecureSetting.secureString("discovery.azure-arm.subscription_id", null);

    /**
     * Azure ARM client Id: "discovery.azure-arm.client_id"
     */
    Setting<SecureString> CLIENT_ID_SETTING = SecureSetting.secureString("discovery.azure-arm.client_id", null);

    /**
     * Azure ARM secret: "discovery.azure-arm.secret"
     */
    Setting<SecureString> SECRET_SETTING = SecureSetting.secureString("discovery.azure-arm.secret", null);

    /**
     * Azure ARM tenant Id: "discovery.azure-arm.tenant_id"
     */
    Setting<SecureString> TENANT_ID_SETTING = SecureSetting.secureString("discovery.azure-arm.tenant_id", null);

    /**
     * `discovery.azure-arm.refresh_interval`: the Azure ARM plugin can cache the list of nodes for a given period you can set with
     * this setting. It will avoid calling the Azure API too often. Values can be a negative time value like `-1s` for infinite
     * caching, a zero value `0s` for no cache (default) or any positive value like `10s` to define the duration of the cache.
     */
    Setting<TimeValue> REFRESH_SETTING =
        Setting.positiveTimeSetting("discovery.azure-arm.refresh_interval", TimeValue.timeValueSeconds(0),
            Property.NodeScope, Property.Dynamic);

    /**
     * `discovery.azure-arm.host.type`: We will read the VMs IP addresses from either the `private_ip` (default)
     * (see {@link HostType#PRIVATE_IP}) or the `public_ip` (see {@link HostType#PUBLIC_IP}).
     */
    Setting<HostType> HOST_TYPE_SETTING =
        new Setting<>("discovery.azure-arm.host.type", HostType.PRIVATE_IP.name(),
            HostType::fromString, Property.NodeScope, Property.Dynamic);

    /**
     * `discovery.azure-arm.host.name`: you can filter virtual machines you would like to connect to by entering a name here.
     * It can be a wildcard like `azure-esnode-*`.
     */
    Setting<String> HOST_NAME_SETTING = Setting.simpleString("discovery.azure-arm.host.name",
        Property.NodeScope, Property.Dynamic);

    /**
     * `discovery.azure-arm.host.resource_group`: you can filter virtual machines you would like to connect to by entering the
     * resource group they belongs to. It can be a wildcard. For example `azure-preprod-*` will match any machine belonging to any
     * resource group which name starts with `azure-preprod-`.
     */
    Setting<String> HOST_RESOURCE_GROUP_SETTING = Setting.simpleString("discovery.azure-arm.host.resource_group",
        Property.NodeScope, Property.Dynamic);

    /**
     * `discovery.azure-arm.region`: the region name. For example: `westeurope` or `eastus`. Note that `region` is not mandatory but
     * it's highly recommended to set it to avoid having nodes joining across multiple regions.
     */
    Setting<String> REGION_SETTING = Setting.simpleString("discovery.azure-arm.region",
        Property.NodeScope, Property.Dynamic);

    /**
     * Get the list of azure virtual machines
     * @param groupName If provided (null or empty are allowed), we can filter by group name
     * @return a list of azure virtual machines
     */
    List<AzureVirtualMachine> getVirtualMachines(String groupName);
}
