/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cloud.azure.classic.management;

import com.microsoft.windowsazure.core.utils.KeyStoreType;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.azure.classic.AzureSeedHostsProvider;
import org.elasticsearch.discovery.azure.classic.AzureSeedHostsProvider.Deployment;

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
        public static final Setting<AzureSeedHostsProvider.HostType> HOST_TYPE_SETTING =
            new Setting<>("discovery.azure.host.type", AzureSeedHostsProvider.HostType.PRIVATE_IP.name(),
                AzureSeedHostsProvider.HostType::fromString, Property.NodeScope);
        public static final Setting<String> ENDPOINT_NAME_SETTING = new Setting<>("discovery.azure.endpoint.name", "elasticsearch",
            Function.identity(), Property.NodeScope);
        public static final Setting<String> DEPLOYMENT_NAME_SETTING = Setting.simpleString("discovery.azure.deployment.name",
            Property.NodeScope);
        public static final Setting<Deployment> DEPLOYMENT_SLOT_SETTING = new Setting<>("discovery.azure.deployment.slot",
            Deployment.PRODUCTION.name(), Deployment::fromString, Property.NodeScope);
    }

    HostedServiceGetDetailedResponse getServiceDetails();
}
