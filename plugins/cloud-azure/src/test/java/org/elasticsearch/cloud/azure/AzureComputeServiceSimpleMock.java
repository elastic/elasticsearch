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

import com.microsoft.azure.management.network.models.*;
import com.microsoft.windowsazure.Configuration;
import org.elasticsearch.cloud.azure.management.AzureComputeServiceAbstractMock;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Mock Azure API with a single started node
 */
public class AzureComputeServiceSimpleMock extends AzureComputeServiceAbstractMock {

    
    public List<Subnet> listSubnets(String rgName, String vnetName) {

        Subnet subnet = new Subnet();
        subnet.setName("dummySubnet");
        String dummyPrivateIP = "10.0.0.1";

        ResourceId resourceId = new ResourceId();
        resourceId.setId("/subscriptions/xx/resourceGroups/myrg/providers/Microsoft.Network/networkInterfaces/nic_dummy/ipConfigurations/Nic-IP-config");

        NetworkInterfaceIpConfiguration ipConfiguration = new NetworkInterfaceIpConfiguration();
        ipConfiguration.setPrivateIpAddress(dummyPrivateIP);

        NetworkInterface nic = new NetworkInterface();
        nic.setName("nic_dummy");
        nic.setIpConfigurations(CollectionUtils.asArrayList(ipConfiguration));
        subnet.setIpConfigurations(CollectionUtils.asArrayList(resourceId));

        return CollectionUtils.arrayAsArrayList(subnet);
    }

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
            azureModule.computeServiceImpl = AzureComputeServiceSimpleMock.class;
        }
    }

    @Inject
    public AzureComputeServiceSimpleMock(Settings settings) {
        super(settings);
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }
}
