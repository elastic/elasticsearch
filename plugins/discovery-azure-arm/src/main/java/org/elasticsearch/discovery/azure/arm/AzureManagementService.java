/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery.azure.arm;

import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.resourcemanager.compute.ComputeManager;
import com.azure.resourcemanager.compute.models.VirtualMachine;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;

import java.io.Closeable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

public class AzureManagementService implements Closeable {

    private final ComputeManager computeManager;

    AzureManagementService(Settings settings) {
        try (
            SecureString clientId = AzureClientSettings.CLIENT_ID_SETTING.get(settings);
            SecureString tenantId = AzureClientSettings.TENANT_ID_SETTING.get(settings);
            SecureString secret = AzureClientSettings.SECRET_SETTING.get(settings);
            SecureString subscriptionId = AzureClientSettings.SUBSCRIPTION_ID_SETTING.get(settings)
        ) {
            this.computeManager = AccessController.doPrivileged(
                (PrivilegedAction<ComputeManager>) () -> ComputeManager.authenticate(
                    new ClientSecretCredentialBuilder().clientId(clientId.toString())
                        .tenantId(clientId.toString())
                        .clientSecret(secret.toString())
                        .build(),
                    new AzureProfile(tenantId.toString(), subscriptionId.toString(), AzureEnvironment.AZURE)
                )
            );
        }
    }

    public List<AzureVirtualMachine> getVirtualMachines(String groupName) {
        return AccessController.doPrivileged((PrivilegedAction<List<AzureVirtualMachine>>) () -> {
            List<AzureVirtualMachine> machines = new ArrayList<>();
            for (VirtualMachine vm : Strings.isNullOrEmpty(groupName)
                ? computeManager.virtualMachines().list()
                : computeManager.virtualMachines().listByResourceGroup(groupName)) {
                machines.add(toAzureVirtualMachine(vm));
            }
            return machines;
        });
    }

    private static AzureVirtualMachine toAzureVirtualMachine(VirtualMachine vm) {
        String region = vm.region() != null ? vm.region().name() : null;
        String publicIp = vm.getPrimaryPublicIPAddress() != null ? vm.getPrimaryPublicIPAddress().ipAddress() : null;
        String privateIp;
        if (vm.getPrimaryPublicIPAddress() != null && vm.getPrimaryPublicIPAddress().getAssignedNetworkInterfaceIPConfiguration() != null) {
            privateIp = vm.getPrimaryPublicIPAddress().getAssignedNetworkInterfaceIPConfiguration().privateIpAddress();
        } else if (vm.getPrimaryNetworkInterface() != null) {
            privateIp = vm.getPrimaryNetworkInterface().primaryPrivateIP();
        } else {
            privateIp = null;
        }
        return new AzureVirtualMachine(vm.resourceGroupName(), vm.name(), vm.powerState(), region, publicIp, privateIp);
    }

    @Override
    public void close() {
        // TODO how to close the computeManager?
    }
}
