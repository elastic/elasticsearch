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

package org.elasticsearch.cloud.azure.management;

import com.microsoft.windowsazure.core.utils.KeyStoreType;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.SettingsProperty;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.azure.AzureUnicastHostsProvider;

public interface AzureComputeService {

    final class Management {
        public static final Setting<String> SUBSCRIPTION_ID_SETTING =
            Setting.simpleString("cloud.azure.management.subscription.id", false, SettingsProperty.ClusterScope, SettingsProperty.Filtered);
        public static final Setting<String> SERVICE_NAME_SETTING =
            Setting.simpleString("cloud.azure.management.cloud.service.name", false, SettingsProperty.ClusterScope);

        // Keystore settings
        public static final Setting<String> KEYSTORE_PATH_SETTING =
            Setting.simpleString("cloud.azure.management.keystore.path", false, SettingsProperty.ClusterScope, SettingsProperty.Filtered);
        public static final Setting<String> KEYSTORE_PASSWORD_SETTING =
            Setting.simpleString("cloud.azure.management.keystore.password", false, SettingsProperty.ClusterScope,
                SettingsProperty.Filtered);
        public static final Setting<KeyStoreType> KEYSTORE_TYPE_SETTING =
            new Setting<>("cloud.azure.management.keystore.type", KeyStoreType.pkcs12.name(), KeyStoreType::fromString, false,
                SettingsProperty.ClusterScope, SettingsProperty.Filtered);
    }

    final class Discovery {
        public static final Setting<TimeValue> REFRESH_SETTING =
            Setting.positiveTimeSetting("discovery.azure.refresh_interval", TimeValue.timeValueSeconds(0), false,
                SettingsProperty.ClusterScope);

        public static final Setting<AzureUnicastHostsProvider.HostType> HOST_TYPE_SETTING =
            new Setting<>("discovery.azure.host.type", AzureUnicastHostsProvider.HostType.PRIVATE_IP.name(),
                AzureUnicastHostsProvider.HostType::fromString, false, SettingsProperty.ClusterScope);

        public static final String ENDPOINT_NAME = "discovery.azure.endpoint.name";
        public static final String DEPLOYMENT_NAME = "discovery.azure.deployment.name";
        public static final String DEPLOYMENT_SLOT = "discovery.azure.deployment.slot";
    }

    HostedServiceGetDetailedResponse getServiceDetails();
}
