/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery.azure.arm;

import com.azure.resourcemanager.compute.models.PowerState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class AzureArmSeedHostsProvider implements SeedHostsProvider {

    private static final Logger logger = LogManager.getLogger(AzureArmSeedHostsProvider.class);

    private final Settings settings;
    private final AzureManagementService azureManagementService;
    private final TransportService transportService;

    private final TimeValue refreshInterval;
    private final String groupName;
    private final String hostName;
    private final AzureClientSettings.HostType hostType;

    private long lastRefresh;
    private List<TransportAddress> dynamicHosts;

    AzureArmSeedHostsProvider(Settings settings, AzureManagementService azureManagementService, TransportService transportService) {
        this.settings = settings;
        this.azureManagementService = azureManagementService;
        this.transportService = transportService;
        refreshInterval = AzureClientSettings.REFRESH_SETTING.get(settings);
        groupName = AzureClientSettings.HOST_RESOURCE_GROUP_SETTING.get(settings);
        hostName = AzureClientSettings.HOST_NAME_SETTING.get(settings);
        hostType = AzureClientSettings.HOST_TYPE_SETTING.get(settings);
    }

    @Override
    public List<TransportAddress> getSeedAddresses(HostsResolver hostsResolver) {
        if (refreshInterval.millis() != 0) {
            if (dynamicHosts != null
                && (refreshInterval.millis() < 0 || (System.currentTimeMillis() - lastRefresh) < refreshInterval.millis())) {
                return dynamicHosts;
            }
            lastRefresh = System.currentTimeMillis();
        }

        dynamicHosts = new ArrayList<>();
        for (AzureVirtualMachine vm : azureManagementService.getVirtualMachines(
            Strings.isNotEmpty(groupName) && Regex.isSimpleMatchPattern(groupName) == false ? groupName : ""
        )) {
            if (vm.powerState() != PowerState.STARTING && vm.powerState() != PowerState.RUNNING) {
                continue;
            }
            if (AzureClientSettings.REGION_SETTING.exists(settings)) {
                if (AzureClientSettings.REGION_SETTING.get(settings).equals(vm.region()) == false) {
                    continue;
                }
            }
            if (Strings.isNotEmpty(groupName)
                && (Regex.isSimpleMatchPattern(groupName)
                    ? Regex.simpleMatch(groupName, vm.groupName())
                    : groupName.equals(vm.groupName())) == false) {
                continue;
            }
            if (Strings.isNotEmpty(groupName)
                && (Regex.isSimpleMatchPattern(groupName)
                    ? Regex.simpleMatch(groupName, vm.groupName())
                    : groupName.equals(vm.groupName())) == false) {
                continue;
            }
            if (Strings.isNotEmpty(hostName)
                && (Regex.isSimpleMatchPattern(hostName) ? Regex.simpleMatch(hostName, vm.name()) : hostName.equals(vm.name())) == false) {
                continue;
            }

            InetAddress ip = switch (hostType) {
                case PRIVATE_IP -> vm.privateIp() != null ? InetAddresses.forString(vm.privateIp()) : null;
                case PUBLIC_IP -> vm.publicIp() != null ? InetAddresses.forString(vm.publicIp()) : null;
            };
            if (ip == null) {
                logger.warn("Unable to find network address [{}]...", vm.name());
                continue;
            }
            try {
                for (TransportAddress transportAddress : transportService.addressesFromString(InetAddresses.toUriString(ip))) {
                    dynamicHosts.add(transportAddress);
                    break;
                }
            } catch (Exception ignore) {
                logger.warn("Unable to convert [{}] to transport address", ip);
            }
        }
        return dynamicHosts;
    }
}
