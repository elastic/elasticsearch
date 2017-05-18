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

import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.azure.management.compute.ComputeManagementClient;
import com.microsoft.azure.management.compute.ComputeManagementService;
import com.microsoft.azure.utility.AuthHelper;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.URI;

import static org.elasticsearch.cloud.azure.management.AzureComputeService.Management.*;

public class AzureComputeServiceImpl extends AbstractLifecycleComponent<AzureComputeServiceImpl>
        implements AzureComputeService {

    static final class Azure {
        private static final String ENDPOINT = "https://management.core.windows.net/";
        private static final String AUTH_ENDPOINT = "https://login.windows.net/";
    }

    private final ComputeManagementClient computeManagementClient;
    private final String resourceGroupName;
    private final Configuration configuration;

    @Inject
    public AzureComputeServiceImpl(Settings settings) {
        super(settings);
        String subscriptionId = settings.get(SUBSCRIPTION_ID);
        String tenantId = settings.get(TENANT_ID);
        String appId = settings.get(APP_ID);
        String appSecret = settings.get(APP_SECRET);

        resourceGroupName = settings.get(Management.RESOURCE_GROUP_NAME);

        Configuration conf;
        try {
            AuthenticationResult authRes = AuthHelper.getAccessTokenFromServicePrincipalCredentials(
                    Azure.ENDPOINT,
                    Azure.AUTH_ENDPOINT,
                    tenantId,
                    appId,
                    appSecret);
            conf = ManagementConfiguration.configure(
                    null,
                    (URI) null,
                    subscriptionId, // subscription id
                    authRes.getAccessToken()
            );
        } catch (Exception e) {
            logger.error("can not start azure client: {}", e.getMessage());
            computeManagementClient = null;
            configuration = null;
            return;
        }
        logger.trace("creating new Azure client for [{}], [{}]", subscriptionId, resourceGroupName);
        configuration = conf;
        computeManagementClient = ComputeManagementService.create(configuration);

    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
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
