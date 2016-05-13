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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

/**
 *
 */
public class AzureComputeServiceImpl extends AbstractLifecycleComponent<AzureComputeServiceImpl>
    implements AzureComputeService {

    private final ComputeManagementClient computeManagementClient;
    private final String serviceName;

    @Inject
    public AzureComputeServiceImpl(Settings settings) {
        super(settings);
        String subscriptionId = Management.SUBSCRIPTION_ID_SETTING.get(settings);

        serviceName = Management.SERVICE_NAME_SETTING.get(settings);
        String keystorePath = Management.KEYSTORE_PATH_SETTING.get(settings);
        String keystorePassword = Management.KEYSTORE_PASSWORD_SETTING.get(settings);
        KeyStoreType keystoreType = Management.KEYSTORE_TYPE_SETTING.get(settings);

        logger.trace("creating new Azure client for [{}], [{}]", subscriptionId, serviceName);
        ComputeManagementClient result;
        try {
            // Check that we have all needed properties
            Configuration configuration = ManagementConfiguration.configure(Management.ENDPOINT_SETTING.get(settings),
                subscriptionId, keystorePath, keystorePassword, keystoreType);
            result = ComputeManagementService.create(configuration);
        } catch (IOException e) {
            logger.error("can not start azure client: {}", e.getMessage());
            result = null;
        }
        this.computeManagementClient = result;
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
