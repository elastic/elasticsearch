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
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;

import static org.elasticsearch.common.util.CollectionUtils.newSingletonArrayList;


/**
 * Mock Azure API with two started nodes
 */
public class AzureComputeServiceTwoNodesMock extends AzureComputeServiceAbstractMock {
    public static class TestPlugin extends Plugin {
        public void onModule(AzureDiscoveryModule azureDiscoveryModule) {
            azureDiscoveryModule.computeServiceImpl = AzureComputeServiceTwoNodesMock.class;
        }
    }

    NetworkService networkService;

    @Inject
    protected AzureComputeServiceTwoNodesMock(Settings settings, NetworkService networkService) {
        super(settings);
        this.networkService = networkService;
    }

    @Override
    public HostedServiceGetDetailedResponse getServiceDetails() {
        HostedServiceGetDetailedResponse response = new HostedServiceGetDetailedResponse();
        HostedServiceGetDetailedResponse.Deployment deployment = new HostedServiceGetDetailedResponse.Deployment();

        // Fake the deployment
        deployment.setName("dummy");
        deployment.setDeploymentSlot(DeploymentSlot.Production);
        deployment.setStatus(DeploymentStatus.Running);

        // Fake a first instance
        RoleInstance instance1 = new RoleInstance();
        instance1.setInstanceName("dummy1");

        // Fake the private IP
        instance1.setIPAddress(InetAddress.getLoopbackAddress());

        // Fake the public IP
        InstanceEndpoint endpoint1 = new InstanceEndpoint();
        endpoint1.setName("elasticsearch");
        endpoint1.setVirtualIPAddress(InetAddress.getLoopbackAddress());
        endpoint1.setPort(9400);
        instance1.setInstanceEndpoints(newSingletonArrayList(endpoint1));

        // Fake a first instance
        RoleInstance instance2 = new RoleInstance();
        instance2.setInstanceName("dummy1");

        // Fake the private IP
        instance2.setIPAddress(InetAddress.getLoopbackAddress());

        // Fake the public IP
        InstanceEndpoint endpoint2 = new InstanceEndpoint();
        endpoint2.setName("elasticsearch");
        endpoint2.setVirtualIPAddress(InetAddress.getLoopbackAddress());
        endpoint2.setPort(9401);
        instance2.setInstanceEndpoints(newSingletonArrayList(endpoint2));

        deployment.setRoleInstances(new ArrayList<>(Arrays.asList(instance1, instance2)));

        response.setDeployments(newSingletonArrayList(deployment));

        return response;
    }
}
