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

package org.elasticsearch.cloud.azure.arm;

import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.PagedList;
import com.microsoft.azure.RestClient;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.compute.VirtualMachines;
import com.microsoft.azure.management.network.PublicIpAddress;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Azure Management Service Implementation
 */
public class AzureManagementServiceImpl implements AzureManagementService, AutoCloseable {

    private static final Logger logger = Loggers.getLogger(AzureManagementServiceImpl.class);

    // Should it be final or should we able to reinitialize it sometimes?
    private final Azure client;

    // Package private for test purposes
    final RestClient restClient;

    public AzureManagementServiceImpl(Settings settings) {
        try (SecureString clientId = CLIENT_ID_SETTING.get(settings);
             SecureString tenantId = TENANT_ID_SETTING.get(settings);
             SecureString secret = SECRET_SETTING.get(settings);
             SecureString subscriptionId = SUBSCRIPTION_ID_SETTING.get(settings)) {
            this.restClient = initializeRestClient(clientId.toString(), tenantId.toString(), secret.toString());
            this.client = initialize(restClient, tenantId.toString(), subscriptionId.toString());
        }
    }

    /**
     * Create a client instance
     * @param restClient        Rest client to use
     * @param tenantId          Tenant ID
     * @param subscriptionId    Subscription ID
     * @return a client Instance
     */
    private static Azure initialize(RestClient restClient, String tenantId, String subscriptionId) {
        logger.debug("Initializing azure client");

        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        Azure client = AccessController.doPrivileged((PrivilegedAction<Azure>) () ->
            Azure.authenticate(restClient, tenantId).withSubscription(subscriptionId));

        logger.debug("Azure client initialized");
        return client;
    }

    /**
     * Create a rest client instance
     * @param clientId  Client ID
     * @param tenantId  Tenant ID
     * @param secret    Secret
     * @return a rest client Instance
     */
    private static RestClient initializeRestClient(String clientId, String tenantId, String secret) {
        logger.debug("Initializing azure client");
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        return AccessController.doPrivileged((PrivilegedAction<RestClient>) () -> {
            RestClient restClient = new RestClient.Builder()
                .withBaseUrl(AzureEnvironment.AZURE, AzureEnvironment.Endpoint.RESOURCE_MANAGER)
                .withCredentials(new ApplicationTokenCredentials(clientId, tenantId, secret, AzureEnvironment.AZURE))
                .build();

            logger.debug("Rest client initialized");
            return restClient;
        });
    }


    @Override
    public List<AzureVirtualMachine> getVirtualMachines(String groupName) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        return AccessController.doPrivileged((PrivilegedAction<List<AzureVirtualMachine>>) () -> {
            List<AzureVirtualMachine> machines = new ArrayList<>();

            VirtualMachines virtualMachines = client.virtualMachines();
            PagedList<VirtualMachine> vms;

            if (Strings.isNullOrEmpty(groupName)) {
                logger.debug("Retrieving list of all azure machines");
                vms = virtualMachines.list();
            } else {
                logger.debug("Retrieving list of azure machines belonging to [{}] group", groupName);
                vms = virtualMachines.listByGroup(groupName);
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
        PublicIpAddress primaryPublicIpAddress = vm.getPrimaryPublicIpAddress();
        if (primaryPublicIpAddress != null) {
            machine.setPublicIp(primaryPublicIpAddress.ipAddress());
            if (primaryPublicIpAddress.getAssignedNetworkInterfaceIpConfiguration() != null) {
                machine.setPrivateIp(primaryPublicIpAddress.getAssignedNetworkInterfaceIpConfiguration().privateIpAddress());
            }
        } else if (vm.getPrimaryNetworkInterface() != null) {
            machine.setPrivateIp(vm.getPrimaryNetworkInterface().primaryPrivateIp());
        }
        return machine;
    }

    @Override
    public void close() throws IOException {
        // Sadly we can't close a client yet so we will have leaked resources
        logger.debug("Closing azure client");
        // client.close();
    }
}
