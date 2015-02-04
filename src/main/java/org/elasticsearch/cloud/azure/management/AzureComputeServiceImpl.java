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

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.core.utils.KeyStoreType;
import com.microsoft.windowsazure.management.compute.ComputeManagementClient;
import com.microsoft.windowsazure.management.compute.ComputeManagementService;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cloud.azure.AzureServiceDisableException;
import org.elasticsearch.cloud.azure.AzureServiceRemoteException;
import org.elasticsearch.cloud.azure.AzureSettingsFilter;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 */
public class AzureComputeServiceImpl extends AbstractLifecycleComponent<AzureComputeServiceImpl>
    implements AzureComputeService {

    static final class Azure {
        private static final String ENDPOINT = "https://management.core.windows.net/";
    }

    private final ComputeManagementClient computeManagementClient;
    private final String serviceName;

    @Inject
    public AzureComputeServiceImpl(Settings settings, SettingsFilter settingsFilter) {
        super(settings);
        settingsFilter.addFilter(new AzureSettingsFilter());

        String subscriptionId = componentSettings.get(Fields.SUBSCRIPTION_ID, settings.get("cloud.azure." + Fields.SUBSCRIPTION_ID_DEPRECATED));

        serviceName = componentSettings.get(Fields.SERVICE_NAME, settings.get("cloud.azure." + Fields.SERVICE_NAME_DEPRECATED));
        String keystorePath = componentSettings.get(Fields.KEYSTORE_PATH, settings.get("cloud.azure." + Fields.KEYSTORE_DEPRECATED));
        String keystorePassword = componentSettings.get(Fields.KEYSTORE_PASSWORD, settings.get("cloud.azure." + Fields.PASSWORD_DEPRECATED));
        String strKeyStoreType = componentSettings.get(Fields.KEYSTORE_TYPE, KeyStoreType.pkcs12.name());
        KeyStoreType tmpKeyStoreType = KeyStoreType.pkcs12;
        try {
            tmpKeyStoreType = KeyStoreType.fromString(strKeyStoreType);
        } catch (Exception e) {
            logger.warn("wrong value for [{}]: [{}]. falling back to [{}]...", Fields.KEYSTORE_TYPE,
                    strKeyStoreType, KeyStoreType.pkcs12.name());
        }
        KeyStoreType keystoreType = tmpKeyStoreType;

        // Check that we have all needed properties
        Configuration configuration;
        try {
            configuration = ManagementConfiguration.configure(new URI(Azure.ENDPOINT),
                    subscriptionId, keystorePath, keystorePassword, keystoreType);
        } catch (IOException|URISyntaxException e) {
            logger.error("can not start azure client: {}", e.getMessage());
            computeManagementClient = null;
            return;
        }
        logger.trace("creating new Azure client for [{}], [{}]", subscriptionId, serviceName);
        computeManagementClient = ComputeManagementService.create(configuration);
    }

    @Override
    public HostedServiceGetDetailedResponse getServiceDetails() {
        if (computeManagementClient == null) {
            // Azure plugin is disabled
            throw new AzureServiceDisableException("azure plugin is disabled.");
        }

        try {
            return computeManagementClient.getHostedServicesOperations().getDetailed(serviceName);
        } catch (Exception e) {
            throw new AzureServiceRemoteException("can not get list of azure nodes", e);
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        if (computeManagementClient != null) {
            try {
                computeManagementClient.close();
            } catch (IOException e) {
                logger.error("error while closing Azure client", e);
            }
        }
    }
}
