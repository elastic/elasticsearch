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

package org.elasticsearch.cloud.azure;

import com.microsoft.windowsazure.management.compute.models.DeploymentSlot;
import com.microsoft.windowsazure.management.compute.models.DeploymentStatus;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import com.microsoft.windowsazure.management.compute.models.InstanceEndpoint;
import com.microsoft.windowsazure.management.compute.models.RoleInstance;
import org.elasticsearch.cloud.azure.management.AzureComputeServiceAbstractMock;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;

import java.net.InetAddress;

/**
 * Mock Azure API with a single started node
 */
public class AzureComputeServiceSimpleMock extends AzureComputeServiceAbstractMock {

    public static class TestPlugin extends Plugin {
        public void onModule(AzureDiscoveryModule azureDiscoveryModule) {
            azureDiscoveryModule.computeServiceImpl = AzureComputeServiceSimpleMock.class;
        }
    }

    @Inject
    public AzureComputeServiceSimpleMock(Settings settings) {
        super(settings);
    }

    @Override
    public HostedServiceGetDetailedResponse getServiceDetails() {
        HostedServiceGetDetailedResponse response = new HostedServiceGetDetailedResponse();
        HostedServiceGetDetailedResponse.Deployment deployment = new HostedServiceGetDetailedResponse.Deployment();

        // Fake the deployment
        deployment.setName("dummy");
        deployment.setDeploymentSlot(DeploymentSlot.Production);
        deployment.setStatus(DeploymentStatus.Running);

        // Fake an instance
        RoleInstance instance = new RoleInstance();
        instance.setInstanceName("dummy1");

        // Fake the private IP
        instance.setIPAddress(InetAddress.getLoopbackAddress());

        // Fake the public IP
        InstanceEndpoint endpoint = new InstanceEndpoint();
        endpoint.setName("elasticsearch");
        endpoint.setVirtualIPAddress(InetAddress.getLoopbackAddress());
        endpoint.setPort(9400);
        instance.setInstanceEndpoints(CollectionUtils.newSingletonArrayList(endpoint));

        deployment.setRoleInstances(CollectionUtils.newSingletonArrayList(instance));
        response.setDeployments(CollectionUtils.newSingletonArrayList(deployment));

        return response;
    }
}
