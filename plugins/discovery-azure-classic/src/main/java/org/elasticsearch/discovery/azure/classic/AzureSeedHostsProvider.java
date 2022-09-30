/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.azure.classic;

import com.microsoft.windowsazure.management.compute.models.DeploymentSlot;
import com.microsoft.windowsazure.management.compute.models.DeploymentStatus;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import com.microsoft.windowsazure.management.compute.models.InstanceEndpoint;
import com.microsoft.windowsazure.management.compute.models.RoleInstance;

import org.elasticsearch.cloud.azure.classic.AzureServiceDisableException;
import org.elasticsearch.cloud.azure.classic.AzureServiceRemoteException;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService.Discovery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class AzureSeedHostsProvider implements SeedHostsProvider {

    private static final Logger logger = LogManager.getLogger(AzureSeedHostsProvider.class);

    public enum HostType {
        PRIVATE_IP("private_ip"),
        PUBLIC_IP("public_ip");

        private String type;

        HostType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public static HostType fromString(String type) {
            for (HostType hostType : values()) {
                if (hostType.type.equalsIgnoreCase(type)) {
                    return hostType;
                }
            }
            throw new IllegalArgumentException("invalid value for host type [" + type + "]");
        }
    }

    public enum Deployment {
        PRODUCTION("production", DeploymentSlot.Production),
        STAGING("staging", DeploymentSlot.Staging);

        private String deployment;
        private DeploymentSlot slot;

        Deployment(String deployment, DeploymentSlot slot) {
            this.deployment = deployment;
            this.slot = slot;
        }

        public static Deployment fromString(String string) {
            for (Deployment deployment : values()) {
                if (deployment.deployment.equalsIgnoreCase(string)) {
                    return deployment;
                }
            }
            throw new IllegalArgumentException("invalid value for deployment type [" + string + "]");
        }
    }

    private final Settings settings;
    private final AzureComputeService azureComputeService;
    private TransportService transportService;
    private NetworkService networkService;

    private final TimeValue refreshInterval;
    private long lastRefresh;
    private List<TransportAddress> dynamicHosts;
    private final HostType hostType;
    private final String publicEndpointName;
    private final String deploymentName;
    private final DeploymentSlot deploymentSlot;

    public AzureSeedHostsProvider(
        Settings settings,
        AzureComputeService azureComputeService,
        TransportService transportService,
        NetworkService networkService
    ) {
        this.settings = settings;
        this.azureComputeService = azureComputeService;
        this.transportService = transportService;
        this.networkService = networkService;

        this.refreshInterval = Discovery.REFRESH_SETTING.get(settings);

        this.hostType = Discovery.HOST_TYPE_SETTING.get(settings);
        this.publicEndpointName = Discovery.ENDPOINT_NAME_SETTING.get(settings);

        // Deployment name could be set with discovery.azure.deployment.name
        // Default to cloud.azure.management.cloud.service.name
        this.deploymentName = Discovery.DEPLOYMENT_NAME_SETTING.get(settings);

        // Reading deployment_slot
        this.deploymentSlot = Discovery.DEPLOYMENT_SLOT_SETTING.get(settings).slot;
    }

    /**
     * We build the list of Nodes from Azure Management API
     * Information can be cached using `cloud.azure.refresh_interval` property if needed.
     * Setting `cloud.azure.refresh_interval` to `-1` will cause infinite caching.
     * Setting `cloud.azure.refresh_interval` to `0` will disable caching (default).
     */
    @Override
    public List<TransportAddress> getSeedAddresses(HostsResolver hostsResolver) {
        if (refreshInterval.millis() != 0) {
            if (dynamicHosts != null
                && (refreshInterval.millis() < 0 || (System.currentTimeMillis() - lastRefresh) < refreshInterval.millis())) {
                logger.trace("using cache to retrieve node list");
                return dynamicHosts;
            }
            lastRefresh = System.currentTimeMillis();
        }
        logger.debug("start building nodes list using Azure API");

        dynamicHosts = new ArrayList<>();

        HostedServiceGetDetailedResponse detailed;
        try {
            detailed = azureComputeService.getServiceDetails();
        } catch (AzureServiceDisableException e) {
            logger.debug("Azure discovery service has been disabled. Returning empty list of nodes.");
            return dynamicHosts;
        } catch (AzureServiceRemoteException e) {
            // We got a remote exception
            logger.warn("can not get list of azure nodes: [{}]. Returning empty list of nodes.", e.getMessage());
            logger.trace("AzureServiceRemoteException caught", e);
            return dynamicHosts;
        }

        InetAddress ipAddress = null;
        try {
            ipAddress = networkService.resolvePublishHostAddresses(
                NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings).toArray(Strings.EMPTY_ARRAY)
            );
            logger.trace("ip of current node: [{}]", ipAddress);
        } catch (IOException e) {
            // We can't find the publish host address... Hmmm. Too bad :-(
            logger.trace("exception while finding ip", e);
        }

        for (HostedServiceGetDetailedResponse.Deployment deployment : detailed.getDeployments()) {
            // We check the deployment slot
            if (deployment.getDeploymentSlot() != deploymentSlot) {
                logger.debug(
                    "current deployment slot [{}] for [{}] is different from [{}]. skipping...",
                    deployment.getDeploymentSlot(),
                    deployment.getName(),
                    deploymentSlot
                );
                continue;
            }

            // If provided, we check the deployment name
            if (Strings.hasLength(deploymentName) && deploymentName.equals(deployment.getName()) == false) {
                logger.debug("current deployment name [{}] different from [{}]. skipping...", deployment.getName(), deploymentName);
                continue;
            }

            // We check current deployment status
            if (deployment.getStatus() != DeploymentStatus.Starting
                && deployment.getStatus() != DeploymentStatus.Deploying
                && deployment.getStatus() != DeploymentStatus.Running) {
                logger.debug("[{}] status is [{}]. skipping...", deployment.getName(), deployment.getStatus());
                continue;
            }

            // In other case, it should be the right deployment so we can add it to the list of instances

            for (RoleInstance instance : deployment.getRoleInstances()) {
                final String networkAddress = resolveInstanceAddress(hostType, instance);
                if (networkAddress == null) {
                    // We have a bad parameter here or not enough information from azure
                    logger.warn("no network address found. ignoring [{}]...", instance.getInstanceName());
                    continue;
                }

                try {
                    TransportAddress[] addresses = transportService.addressesFromString(networkAddress);
                    for (TransportAddress address : addresses) {
                        logger.trace("adding {}, transport_address {}", networkAddress, address);
                        dynamicHosts.add(address);
                    }
                } catch (Exception e) {
                    logger.warn("can not convert [{}] to transport address. skipping. [{}]", networkAddress, e.getMessage());
                }
            }
        }

        logger.debug("{} addresses added", dynamicHosts.size());

        return dynamicHosts;
    }

    protected String resolveInstanceAddress(final HostType hostTypeValue, final RoleInstance instance) {
        if (hostTypeValue == HostType.PRIVATE_IP) {
            final InetAddress privateIp = instance.getIPAddress();
            if (privateIp != null) {
                return InetAddresses.toUriString(privateIp);
            } else {
                logger.trace("no private ip provided. ignoring [{}]...", instance.getInstanceName());
            }
        } else if (hostTypeValue == HostType.PUBLIC_IP) {
            for (InstanceEndpoint endpoint : instance.getInstanceEndpoints()) {
                if (publicEndpointName.equals(endpoint.getName())) {
                    return NetworkAddress.format(new InetSocketAddress(endpoint.getVirtualIPAddress(), endpoint.getPort()));
                } else {
                    logger.trace("ignoring endpoint [{}] as different than [{}]", endpoint.getName(), publicEndpointName);
                }
            }
        }
        return null;
    }
}
