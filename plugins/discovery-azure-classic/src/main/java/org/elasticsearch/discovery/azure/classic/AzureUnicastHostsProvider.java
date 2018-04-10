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

package org.elasticsearch.discovery.azure.classic;

import com.microsoft.windowsazure.management.compute.models.DeploymentSlot;
import com.microsoft.windowsazure.management.compute.models.DeploymentStatus;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import com.microsoft.windowsazure.management.compute.models.InstanceEndpoint;
import com.microsoft.windowsazure.management.compute.models.RoleInstance;
import org.elasticsearch.Version;
import org.elasticsearch.cloud.azure.classic.AzureServiceDisableException;
import org.elasticsearch.cloud.azure.classic.AzureServiceRemoteException;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService.Discovery;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class AzureUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    public enum HostType {
        PRIVATE_IP("private_ip"),
        PUBLIC_IP("public_ip");

        private String type ;

        HostType(String type) {
            this.type = type ;
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

    private final AzureComputeService azureComputeService;
    private TransportService transportService;
    private NetworkService networkService;

    private final TimeValue refreshInterval;
    private long lastRefresh;
    private List<DiscoveryNode> cachedDiscoNodes;
    private final HostType hostType;
    private final String publicEndpointName;
    private final String deploymentName;
    private final DeploymentSlot deploymentSlot;

    public AzureUnicastHostsProvider(Settings settings, AzureComputeService azureComputeService,
                                     TransportService transportService, NetworkService networkService) {
        super(settings);
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
    public List<DiscoveryNode> buildDynamicNodes() {
        if (refreshInterval.millis() != 0) {
            if (cachedDiscoNodes != null &&
                    (refreshInterval.millis() < 0 || (System.currentTimeMillis() - lastRefresh) < refreshInterval.millis())) {
                logger.trace("using cache to retrieve node list");
                return cachedDiscoNodes;
            }
            lastRefresh = System.currentTimeMillis();
        }
        logger.debug("start building nodes list using Azure API");

        cachedDiscoNodes = new ArrayList<>();

        HostedServiceGetDetailedResponse detailed;
        try {
            detailed = azureComputeService.getServiceDetails();
        } catch (AzureServiceDisableException e) {
            logger.debug("Azure discovery service has been disabled. Returning empty list of nodes.");
            return cachedDiscoNodes;
        } catch (AzureServiceRemoteException e) {
            // We got a remote exception
            logger.warn("can not get list of azure nodes: [{}]. Returning empty list of nodes.", e.getMessage());
            logger.trace("AzureServiceRemoteException caught", e);
            return cachedDiscoNodes;
        }

        InetAddress ipAddress = null;
        try {
            ipAddress = networkService.resolvePublishHostAddresses(
                NetworkService.GLOBAL_NETWORK_PUBLISHHOST_SETTING.get(settings).toArray(Strings.EMPTY_ARRAY));
            logger.trace("ip of current node: [{}]", ipAddress);
        } catch (IOException e) {
            // We can't find the publish host address... Hmmm. Too bad :-(
            logger.trace("exception while finding ip", e);
        }

        for (HostedServiceGetDetailedResponse.Deployment deployment : detailed.getDeployments()) {
            // We check the deployment slot
            if (deployment.getDeploymentSlot() != deploymentSlot) {
                logger.debug("current deployment slot [{}] for [{}] is different from [{}]. skipping...",
                        deployment.getDeploymentSlot(), deployment.getName(), deploymentSlot);
                continue;
            }

            // If provided, we check the deployment name
            if (Strings.hasLength(deploymentName) && !deploymentName.equals(deployment.getName())) {
                logger.debug("current deployment name [{}] different from [{}]. skipping...",
                        deployment.getName(), deploymentName);
                continue;
            }

            // We check current deployment status
            if (deployment.getStatus() != DeploymentStatus.Starting &&
                    deployment.getStatus() != DeploymentStatus.Deploying &&
                    deployment.getStatus() != DeploymentStatus.Running) {
                logger.debug("[{}] status is [{}]. skipping...",
                        deployment.getName(), deployment.getStatus());
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
                    // we only limit to 1 port per address, makes no sense to ping 100 ports
                    TransportAddress[] addresses = transportService.addressesFromString(networkAddress, 1);
                    for (TransportAddress address : addresses) {
                        logger.trace("adding {}, transport_address {}", networkAddress, address);
                        cachedDiscoNodes.add(new DiscoveryNode("#cloud-" + instance.getInstanceName(), address, emptyMap(),
                                emptySet(), Version.CURRENT.minimumCompatibilityVersion()));
                    }
                } catch (Exception e) {
                    logger.warn("can not convert [{}] to transport address. skipping. [{}]", networkAddress, e.getMessage());
                }
            }
        }

        logger.debug("{} node(s) added", cachedDiscoNodes.size());

        return cachedDiscoNodes;
    }

    protected String resolveInstanceAddress(final HostType hostType, final RoleInstance instance) {
        if (hostType == HostType.PRIVATE_IP) {
            final InetAddress privateIp = instance.getIPAddress();
            if (privateIp != null) {
                return InetAddresses.toUriString(privateIp);
            } else {
                logger.trace("no private ip provided. ignoring [{}]...", instance.getInstanceName());
            }
        } else if (hostType == HostType.PUBLIC_IP) {
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
