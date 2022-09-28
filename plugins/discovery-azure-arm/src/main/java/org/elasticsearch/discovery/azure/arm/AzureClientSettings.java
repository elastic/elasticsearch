/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery.azure.arm;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.TimeValue;

import java.util.Locale;

/**
 * A container for settings used to create an Azure client.
 */
final class AzureClientSettings {

    enum HostType {
        PRIVATE_IP,
        PUBLIC_IP;

        public static HostType fromString(String type) {
            return valueOf(type.toUpperCase(Locale.ROOT));
        }
    }

    /**
     * Azure ARM subscription Id: "discovery.azure-arm.subscription_id"
     */
    static final Setting<SecureString> SUBSCRIPTION_ID_SETTING = SecureSetting.secureString("discovery.azure-arm.subscription_id", null);

    /**
     * Azure ARM client Id: "discovery.azure-arm.client_id"
     */
    static final Setting<SecureString> CLIENT_ID_SETTING = SecureSetting.secureString("discovery.azure-arm.client_id", null);

    /**
     * Azure ARM secret: "discovery.azure-arm.secret"
     */
    static final Setting<SecureString> SECRET_SETTING = SecureSetting.secureString("discovery.azure-arm.secret", null);

    /**
     * Azure ARM tenant Id: "discovery.azure-arm.tenant_id"
     */
    static final Setting<SecureString> TENANT_ID_SETTING = SecureSetting.secureString("discovery.azure-arm.tenant_id", null);

    /**
     * `discovery.azure-arm.refresh_interval`: the Azure ARM plugin can cache the list of nodes for a given period you can set with
     * this setting. It will avoid calling the Azure API too often. Values can be a negative time value like `-1s` for infinite
     * caching, a zero value `0s` for no cache (default) or any positive value like `10s` to define the duration of the cache.
     */
    static final Setting<TimeValue> REFRESH_SETTING = Setting.positiveTimeSetting(
        "discovery.azure-arm.refresh_interval",
        TimeValue.timeValueSeconds(0),
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * `discovery.azure-arm.host.type`: We will read the VMs IP addresses from either the `private_ip` (default)
     * (see {@link HostType#PRIVATE_IP}) or the `public_ip` (see {@link HostType#PUBLIC_IP}).
     */
    static final Setting<HostType> HOST_TYPE_SETTING = new Setting<>(
        "discovery.azure-arm.host.type",
        HostType.PRIVATE_IP.name(),
        HostType::fromString,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * `discovery.azure-arm.host.name`: you can filter virtual machines you would like to connect to by entering a name here.
     * It can be a wildcard like `azure-esnode-*`.
     */
    static final Setting<String> HOST_NAME_SETTING = Setting.simpleString(
        "discovery.azure-arm.host.name",
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * `discovery.azure-arm.host.resource_group`: you can filter virtual machines you would like to connect to by entering the
     * resource group they belongs to. It can be a wildcard. For example `azure-preprod-*` will match any machine belonging to any
     * resource group which name starts with `azure-preprod-`.
     */
    static final Setting<String> HOST_RESOURCE_GROUP_SETTING = Setting.simpleString(
        "discovery.azure-arm.host.resource_group",
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * `discovery.azure-arm.region`: the region name. For example: `westeurope` or `eastus`. Note that `region` is not mandatory but
     * it's highly recommended to set it to avoid having nodes joining across multiple regions.
     */
    static final Setting<String> REGION_SETTING = Setting.simpleString("discovery.azure-arm.region", Property.NodeScope, Property.Dynamic);
}
