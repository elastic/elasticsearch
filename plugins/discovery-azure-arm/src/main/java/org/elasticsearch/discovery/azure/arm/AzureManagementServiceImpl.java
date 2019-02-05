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

package org.elasticsearch.discovery.azure.arm;

import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.PagedList;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.compute.VirtualMachines;
import com.microsoft.azure.management.compute.implementation.ComputeManager;
import com.microsoft.azure.management.network.PublicIPAddress;
import com.microsoft.rest.RestClient;
import com.microsoft.rest.ServiceResponseBuilder;
import com.microsoft.rest.serializer.JacksonAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Azure Management Service Implementation
 */
public class AzureManagementServiceImpl implements AzureManagementService, AutoCloseable {

    private static final Logger logger = LogManager.getLogger(AzureManagementServiceImpl.class);

    // Should it be final or should we able to reinitialize it sometimes?
    private final ComputeManager computeManager;

    // Package private for test purposes
    final RestClient restClient;

    AzureManagementServiceImpl(Settings settings) {
        try (SecureString clientId = AzureClientSettings.CLIENT_ID_SETTING.get(settings);
             SecureString tenantId = AzureClientSettings.TENANT_ID_SETTING.get(settings);
             SecureString secret = AzureClientSettings.SECRET_SETTING.get(settings);
             SecureString subscriptionId = AzureClientSettings.SUBSCRIPTION_ID_SETTING.get(settings)) {
            this.restClient = initializeRestClient(clientId.toString(), tenantId.toString(), secret.toString());
            this.computeManager = initialize(restClient, subscriptionId.toString());
        }
    }

    /**
     * Create a computeManager instance
     * @param restClient        Rest computeManager to use
     * @param subscriptionId    Subscription ID
     * @return a computeManager Instance
     */
    private static ComputeManager initialize(RestClient restClient, String subscriptionId) {
        logger.debug("Initializing azure computeManager");

        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        ComputeManager computeManager = AccessController.doPrivileged((PrivilegedAction<ComputeManager>) () ->
            ComputeManager.authenticate(restClient, subscriptionId));

        logger.debug("Azure computeManager initialized");
        return computeManager;
    }

    /**
     * Create a rest computeManager instance
     * @param clientId  Client ID
     * @param tenantId  Tenant ID
     * @param secret    Secret
     * @return a rest computeManager Instance
     */
    private static RestClient initializeRestClient(String clientId, String tenantId, String secret) {
        logger.debug("Initializing azure computeManager");
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        return AccessController.doPrivileged((PrivilegedAction<RestClient>) () -> {
            RestClient restClient = new RestClient.Builder()
                .withBaseUrl(AzureEnvironment.AZURE, AzureEnvironment.Endpoint.RESOURCE_MANAGER)
                .withCredentials(new ApplicationTokenCredentials(clientId, tenantId, secret, AzureEnvironment.AZURE))
                .withResponseBuilderFactory(new ServiceResponseBuilder.Factory())
                .withSerializerAdapter(new JacksonAdapter())
                .build();

            logger.debug("Rest computeManager initialized");
            return restClient;
        });
    }


    /**
     * Get the list of azure virtual machines
     * @param groupName If provided (null or empty are allowed), we can filter by group name
     * @return a list of azure virtual machines
     */
    public List<AzureVirtualMachine> getVirtualMachines(String groupName) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        return AccessController.doPrivileged((PrivilegedAction<List<AzureVirtualMachine>>) () -> {
            List<AzureVirtualMachine> machines = new ArrayList<>();

            VirtualMachines virtualMachines = computeManager.virtualMachines();
            PagedList<VirtualMachine> vms;

            if (Strings.isNullOrEmpty(groupName)) {
                logger.debug("Retrieving list of all azure machines");
                vms = virtualMachines.list();
            } else {
                logger.debug("Retrieving list of azure machines belonging to [{}] group", groupName);
                vms = virtualMachines.listByResourceGroup(groupName);
            }

            // We iterate over all VMs and transform them to our internal objects
            for (VirtualMachine vm : vms) {
                machines.add(toAzureVirtualMachine(vm));
            }

            return machines;
        });
    }

    private AzureVirtualMachine toAzureVirtualMachine(VirtualMachine vm) {
        AzureVirtualMachine machine = new AzureVirtualMachine();
        machine.setGroupName(vm.resourceGroupName());
        machine.setName(vm.name());
        if (vm.region() != null) {
            machine.setRegion(vm.region().name());
        }
        machine.setPowerState(AzureVirtualMachine.PowerState.fromAzurePowerState(vm.powerState()));
        PublicIPAddress primaryPublicIPAddress = vm.getPrimaryPublicIPAddress();
        if (primaryPublicIPAddress != null) {
            machine.setPublicIp(primaryPublicIPAddress.ipAddress());
            if (primaryPublicIPAddress.getAssignedNetworkInterfaceIPConfiguration() != null) {
                machine.setPrivateIp(primaryPublicIPAddress.getAssignedNetworkInterfaceIPConfiguration().privateIPAddress());
            }
        } else if (vm.getPrimaryNetworkInterface() != null) {
            machine.setPrivateIp(vm.getPrimaryNetworkInterface().primaryPrivateIP());
        }
        return machine;
    }

    @Override
    public void close() {
        // Sadly we can't close a computeManager yet so we will have leaked resources
        logger.debug("Closing azure computeManager");
        // We try to close the restClient but it's useless as it it still failing
        try {
            restClient.closeAndWait();
        } catch (InterruptedException e) {
            throw new ElasticsearchException("Can't close the Azure internal Rest Client", e);
        }
        // computeManager.close();
    }
}
