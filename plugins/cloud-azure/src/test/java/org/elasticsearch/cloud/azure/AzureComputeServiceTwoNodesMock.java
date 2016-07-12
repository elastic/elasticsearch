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

import com.microsoft.azure.management.network.models.NetworkInterface;
import com.microsoft.azure.management.network.models.NetworkInterfaceIpConfiguration;
import com.microsoft.azure.management.network.models.ResourceId;
import com.microsoft.azure.management.network.models.Subnet;
import com.microsoft.windowsazure.Configuration;
import org.elasticsearch.cloud.azure.management.AzureComputeServiceAbstractMock;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

/**
 * Mock Azure API with two started nodes
 */
public class AzureComputeServiceTwoNodesMock extends AzureComputeServiceAbstractMock {

    public static class TestPlugin extends Plugin {
        @Override
        public String name() {
            return "mock-compute-service";
        }
        @Override
        public String description() {
            return "plugs in a mock compute service for testing";
        }
        public void onModule(AzureModule azureModule) {
            azureModule.computeServiceImpl = AzureComputeServiceTwoNodesMock.class;
        }
    }

    NetworkService networkService;

    @Inject
    protected AzureComputeServiceTwoNodesMock(Settings settings, NetworkService networkService) {
        super(settings);
        this.networkService = networkService;
    }

    public List<Subnet> listSubnets(String rgName, String vnetName) {
        Subnet subnet = new Subnet();
        String dummyPrivateIP = "10.0.0.1";
        String dummyPrivateIP2 = "10.0.0.2";


        ResourceId resourceId = new ResourceId();
        resourceId.setId("/subscriptions/xx/resourceGroups/rgName/providers/Microsoft.Network/networkInterfaces/nic_dummy/ipConfigurations/Nic-IP-config");

        NetworkInterfaceIpConfiguration ipConfiguration = new NetworkInterfaceIpConfiguration();
        ipConfiguration.setPrivateIpAddress(dummyPrivateIP);

        NetworkInterfaceIpConfiguration ipConfiguration2 = new NetworkInterfaceIpConfiguration();
        ipConfiguration2.setPrivateIpAddress(dummyPrivateIP2);

        NetworkInterface nic = new NetworkInterface();
        nic.setName("nic_dummy");
        nic.setIpConfigurations(CollectionUtils.arrayAsArrayList(ipConfiguration, ipConfiguration2));
        subnet.setIpConfigurations(CollectionUtils.asArrayList(resourceId));

        return CollectionUtils.asArrayList(subnet);
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

}
