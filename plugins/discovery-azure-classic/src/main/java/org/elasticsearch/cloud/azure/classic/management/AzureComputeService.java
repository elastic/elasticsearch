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

package org.elasticsearch.cloud.azure.classic.management;

import com.microsoft.windowsazure.core.utils.KeyStoreType;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.azure.classic.AzureUnicastHostsProvider;
import org.elasticsearch.discovery.azure.classic.AzureUnicastHostsProvider.Deployment;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Function;

public interface AzureComputeService {

    final class Management {
        public static final Setting<String> SUBSCRIPTION_ID_SETTING =
            Setting.simpleString("cloud.azure.management.subscription.id", Property.NodeScope, Property.Filtered);
        public static final Setting<String> SERVICE_NAME_SETTING =
            Setting.simpleString("cloud.azure.management.cloud.service.name", Property.NodeScope);

        // Keystore settings
        public static final Setting<String> KEYSTORE_PATH_SETTING =
            Setting.simpleString("cloud.azure.management.keystore.path", Property.NodeScope, Property.Filtered);
        public static final Setting<String> KEYSTORE_PASSWORD_SETTING =
            Setting.simpleString("cloud.azure.management.keystore.password", Property.NodeScope,
                Property.Filtered);
        public static final Setting<KeyStoreType> KEYSTORE_TYPE_SETTING =
            new Setting<>("cloud.azure.management.keystore.type", KeyStoreType.pkcs12.name(), KeyStoreType::fromString,
                Property.NodeScope, Property.Filtered);

        // so that it can overridden for tests
        public static final Setting<URI> ENDPOINT_SETTING = new Setting<URI>("cloud.azure.management.endpoint",
            "https://management.core.windows.net/", s -> {
                try {
                    return new URI(s);
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException(e);
                }
            }, Property.NodeScope);
    }

    final class Discovery {
        public static final Setting<TimeValue> REFRESH_SETTING =
            Setting.positiveTimeSetting("discovery.azure.refresh_interval", TimeValue.timeValueSeconds(0), Property.NodeScope);
        public static final Setting<AzureUnicastHostsProvider.HostType> HOST_TYPE_SETTING =
            new Setting<>("discovery.azure.host.type", AzureUnicastHostsProvider.HostType.PRIVATE_IP.name(),
                AzureUnicastHostsProvider.HostType::fromString, Property.NodeScope);
        public static final Setting<String> ENDPOINT_NAME_SETTING = new Setting<>("discovery.azure.endpoint.name", "elasticsearch",
            Function.identity(), Property.NodeScope);
        public static final Setting<String> DEPLOYMENT_NAME_SETTING = Setting.simpleString("discovery.azure.deployment.name",
            Property.NodeScope);
        public static final Setting<Deployment> DEPLOYMENT_SLOT_SETTING = new Setting<>("discovery.azure.deployment.slot",
            Deployment.PRODUCTION.name(), Deployment::fromString, Property.NodeScope);
    }

    HostedServiceGetDetailedResponse getServiceDetails();
}
