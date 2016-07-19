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
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.compute.VirtualMachines;
import com.microsoft.azure.management.network.PublicIpAddress;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.cloud.azure.arm.AzureManagementService.Management.CLIENT_ID_SETTING;
import static org.elasticsearch.cloud.azure.arm.AzureManagementService.Management.SECRET_SETTING;
import static org.elasticsearch.cloud.azure.arm.AzureManagementService.Management.SUBSCRIPTION_ID_SETTING;
import static org.elasticsearch.cloud.azure.arm.AzureManagementService.Management.TENANT_ID_SETTING;

/**
 * Azure Management Service Implementation
 */
public class AzureManagementServiceImpl implements AzureManagementService, AutoCloseable {

    private static final Logger logger = Loggers.getLogger(AzureManagementServiceImpl.class);

    // Should it be final or should we able to reinitialize it sometimes?
    private final Azure client;

    public AzureManagementServiceImpl(Settings settings) {
        this.client = initialize(settings);
    }

    /**
     * Create a client instance
     * @param settings Settings we will read the configuration from
     * @return a client Instance
     * @see Management for client properties
     */
    private static Azure initialize(Settings settings) {
        logger.debug("Initializing azure client");
        String clientId = CLIENT_ID_SETTING.get(settings);
        String tenantId = TENANT_ID_SETTING.get(settings);
        String secret = SECRET_SETTING.get(settings);
        String subscriptionId = SUBSCRIPTION_ID_SETTING.get(settings);

        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        Azure client = AccessController.doPrivileged((PrivilegedAction<Azure>) () ->
            Azure.authenticate(new ApplicationTokenCredentials(clientId, tenantId, secret, AzureEnvironment.AZURE))
                .withSubscription(subscriptionId));

        logger.debug("Azure client initialized");
        return client;
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
