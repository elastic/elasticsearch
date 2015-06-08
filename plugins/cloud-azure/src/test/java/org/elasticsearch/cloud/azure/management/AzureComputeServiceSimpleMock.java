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

import com.microsoft.windowsazure.management.compute.models.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;

import java.net.InetAddress;

/**
 * Mock Azure API with a single started node
 */
public class AzureComputeServiceSimpleMock extends AzureComputeServiceAbstractMock {

    @Inject
    protected AzureComputeServiceSimpleMock(Settings settings) {
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
        instance.setInstanceEndpoints(CollectionUtils.newArrayList(endpoint));

        deployment.setRoleInstances(CollectionUtils.newArrayList(instance));
        response.setDeployments(CollectionUtils.newArrayList(deployment));

        return response;
    }
}
