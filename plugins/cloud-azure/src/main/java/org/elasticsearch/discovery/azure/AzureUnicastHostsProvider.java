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

package org.elasticsearch.discovery.azure;

import com.microsoft.windowsazure.management.compute.models.*;

import org.elasticsearch.Version;
import org.elasticsearch.cloud.azure.AzureServiceDisableException;
import org.elasticsearch.cloud.azure.AzureServiceRemoteException;
import org.elasticsearch.cloud.azure.management.AzureComputeService;
import org.elasticsearch.cloud.azure.management.AzureComputeService.Discovery;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Locale;
import java.util.List;

/**
 *
 */
public class AzureUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    public static enum HostType {
        PRIVATE_IP("private_ip"),
        PUBLIC_IP("public_ip");

        private String type ;

        private HostType(String type) {
            this.type = type ;
        }

        public static HostType fromString(String type) {
            for (HostType hostType : values()) {
                if (hostType.type.equalsIgnoreCase(type)) {
                    return hostType;
                }
            }
            return null;
        }
    }

    public static enum Deployment {
        PRODUCTION("production", DeploymentSlot.Production),
        STAGING("staging", DeploymentSlot.Staging);

        private String deployment;
        private DeploymentSlot slot;

        private Deployment(String deployment, DeploymentSlot slot) {
            this.deployment = deployment;
            this.slot = slot;
        }

        public static Deployment fromString(String string) {
            for (Deployment deployment : values()) {
                if (deployment.deployment.equalsIgnoreCase(string)) {
                    return deployment;
                }
            }
            return null;
        }
    }

    private final AzureComputeService azureComputeService;
    private TransportService transportService;
    private NetworkService networkService;
    private final Version version;

    private final TimeValue refreshInterval;
    private long lastRefresh;
    private List<DiscoveryNode> cachedDiscoNodes;
    private final HostType hostType;
    private final String publicEndpointName;
    private final String deploymentName;
    private final DeploymentSlot deploymentSlot;

    @Inject
    public AzureUnicastHostsProvider(Settings settings, AzureComputeService azureComputeService,
                                   TransportService transportService,
                                   NetworkService networkService,
                                   Version version) {
        super(settings);
        this.azureComputeService = azureComputeService;
        this.transportService = transportService;
        this.networkService = networkService;
        this.version = version;

        this.refreshInterval = settings.getAsTime(Discovery.REFRESH, TimeValue.timeValueSeconds(0));

        String strHostType = settings.get(Discovery.HOST_TYPE, HostType.PRIVATE_IP.name()).toUpperCase(Locale.ROOT);
        HostType tmpHostType = HostType.fromString(strHostType);
        if (tmpHostType == null) {
            logger.warn("wrong value for [{}]: [{}]. falling back to [{}]...", Discovery.HOST_TYPE,
                    strHostType, HostType.PRIVATE_IP.name().toLowerCase(Locale.ROOT));
            tmpHostType = HostType.PRIVATE_IP;
        }
        this.hostType = tmpHostType;
        this.publicEndpointName = settings.get(Discovery.ENDPOINT_NAME, "elasticsearch");

        // Deployment name could be set with discovery.azure.deployment.name
        // Default to cloud.azure.management.cloud.service.name
        this.deploymentName = settings.get(Discovery.DEPLOYMENT_NAME);

        // Reading deployment_slot
        String strDeployment = settings.get(Discovery.DEPLOYMENT_SLOT, Deployment.PRODUCTION.deployment);
        Deployment tmpDeployment = Deployment.fromString(strDeployment);
        if (tmpDeployment == null) {
            logger.warn("wrong value for [{}]: [{}]. falling back to [{}]...", Discovery.DEPLOYMENT_SLOT, strDeployment,
                    Deployment.PRODUCTION.deployment);
            tmpDeployment = Deployment.PRODUCTION;
        }
        this.deploymentSlot = tmpDeployment.slot;
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
            ipAddress = networkService.resolvePublishHostAddress(null);
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
            if (deploymentName != null && !deploymentName.equals(deployment.getName())) {
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
                String networkAddress = null;
                // Let's detect if we want to use public or private IP
                switch (hostType) {
                    case PRIVATE_IP:
                        InetAddress privateIp = instance.getIPAddress();

                        if (privateIp != null) {
                            if (privateIp.equals(ipAddress)) {
                                logger.trace("adding ourselves {}", NetworkAddress.format(ipAddress));
                            }
                            networkAddress = NetworkAddress.formatAddress(privateIp);
                        } else {
                            logger.trace("no private ip provided. ignoring [{}]...", instance.getInstanceName());
                        }
                        break;
                    case PUBLIC_IP:
                        for (InstanceEndpoint endpoint : instance.getInstanceEndpoints()) {
                            if (!publicEndpointName.equals(endpoint.getName())) {
                                logger.trace("ignoring endpoint [{}] as different than [{}]",
                                        endpoint.getName(), publicEndpointName);
                                continue;
                            }

                            networkAddress = NetworkAddress.formatAddress(new InetSocketAddress(endpoint.getVirtualIPAddress(), endpoint.getPort()));
                        }

                        if (networkAddress == null) {
                            logger.trace("no public ip provided. ignoring [{}]...", instance.getInstanceName());
                        }
                        break;
                    default:
                        // This could never happen!
                        logger.warn("undefined host_type [{}]. Please check your settings.", hostType);
                        return cachedDiscoNodes;
                }

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
                        cachedDiscoNodes.add(new DiscoveryNode("#cloud-" + instance.getInstanceName(), address,
                            version.minimumCompatibilityVersion()));
                    }
                } catch (Exception e) {
                    logger.warn("can not convert [{}] to transport address. skipping. [{}]", networkAddress, e.getMessage());
                }
            }
        }

        logger.debug("{} node(s) added", cachedDiscoNodes.size());

        return cachedDiscoNodes;
    }
}
