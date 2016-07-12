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

import com.microsoft.azure.management.network.*;
import com.microsoft.azure.management.network.models.*;

import com.microsoft.windowsazure.exception.ServiceException;
import org.elasticsearch.Version;
import org.elasticsearch.cloud.azure.management.AzureComputeService;
import org.elasticsearch.cloud.azure.management.AzureComputeService.Discovery;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.List;

/**
 *
 */
public class AzureUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    public static enum HostType {
        PRIVATE_IP("private_ip"),
        PUBLIC_IP("public_ip");

        private String type;

        private HostType(String type) {
            this.type = type;
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

    private final AzureComputeService azureComputeService;
    private TransportService transportService;
    private NetworkService networkService;
    private final Version version;

    private final TimeValue refreshInterval;
    private long lastRefresh;
    private List<DiscoveryNode> cachedDiscoNodes;
    private final HostType hostType;
    private final String discoveryMethod;

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
        this.discoveryMethod = settings.get(Discovery.DISCOVERY_METHOD, AzureDiscovery.VNET);
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
                    (refreshInterval.millis() < 0 ||
                            (System.currentTimeMillis() - lastRefresh) < refreshInterval.millis())) {
                logger.trace("using cache to retrieve node list");
                return cachedDiscoNodes;
            }
            lastRefresh = System.currentTimeMillis();
        }
        logger.debug("start building nodes list using Azure API");

        cachedDiscoNodes = new ArrayList<>();

        InetAddress ipAddress = null;
        try {
            ipAddress = networkService.resolvePublishHostAddresses(null);
            logger.trace("ip of current node: [{}]", ipAddress);
        } catch (IOException e) {
            // We can't find the publish host address... Hmmm. Too bad :-(
            logger.trace("exception while finding ip", e);
        }

        // In other case, it should be the right deployment so we can add it to the list of instances
        String rgName = settings.get(AzureComputeService.Management.RESOURCE_GROUP_NAME);
        NetworkResourceProviderClient networkResourceProviderClient =
                NetworkResourceProviderService.create(azureComputeService.getConfiguration());

        try {
            final HashMap<String, String> networkNameOfCurrentHost = retrieveNetInfo(rgName, NetworkAddress.format(ipAddress), networkResourceProviderClient);

            if(networkNameOfCurrentHost.size() == 0 ){
                logger.error("Could not find vnet or subnet of current host");
                return cachedDiscoNodes;
            }

            List<String> ipAddresses = listIPAddresses(networkResourceProviderClient, rgName, networkNameOfCurrentHost.get(AzureDiscovery.VNET),
                    networkNameOfCurrentHost.get(AzureDiscovery.SUBNET), discoveryMethod, hostType, logger);
            for (String networkAddress : ipAddresses) {
                try {
                    // we only limit to 1 port per address, makes no sense to ping 100 ports
                    TransportAddress[] addresses = transportService.addressesFromString(networkAddress, 1);
                    for (TransportAddress address : addresses) {
                        logger.trace("adding {}, transport_address {}", networkAddress, address);
                        cachedDiscoNodes.add(new DiscoveryNode("#cloud-" + networkAddress, address,
                                version.minimumCompatibilityVersion()));
                    }
                } catch (Exception e) {
                    logger.warn("can not convert [{}] to transport address. skipping. [{}]", networkAddress, e.getMessage());
                }
            }
        } catch (UnknownHostException e) {
            logger.error("Error occurred in getting hostname");
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        logger.debug("{} node(s) added", cachedDiscoNodes.size());

        return cachedDiscoNodes;
    }

    public static HashMap<String, String> retrieveNetInfo(String rgName, String ipAddress, NetworkResourceProviderClient networkResourceProviderClient) throws Exception {

        ArrayList<VirtualNetwork> virtualNetworks = networkResourceProviderClient.getVirtualNetworksOperations().list(rgName).getVirtualNetworks();
        HashMap<String, String> networkNames = new HashMap<>();

        for (VirtualNetwork vn : virtualNetworks) {
            ArrayList<Subnet> subnets = vn.getSubnets();
            for (Subnet subnet : subnets) {
                for (ResourceId resourceId : subnet.getIpConfigurations()) {
                    String[] nicURI = resourceId.getId().split("/");
                    NetworkInterface nic = networkResourceProviderClient.getNetworkInterfacesOperations().get(rgName, nicURI[
                            nicURI.length - 3]).getNetworkInterface();
                    ArrayList<NetworkInterfaceIpConfiguration> ips = nic.getIpConfigurations();

                    // find public ip address
                    for (NetworkInterfaceIpConfiguration ipConfiguration : ips) {
                        if (ipAddress.equals(ipConfiguration.getPrivateIpAddress())) {
                            networkNames.put(AzureDiscovery.VNET, vn.getName());
                            networkNames.put(AzureDiscovery.SUBNET, subnet.getName());
                            break;
                        }

                    }
                }
            }
        }

        return networkNames;
    }

    public static List<String> listIPAddresses(NetworkResourceProviderClient networkResourceProviderClient, String rgName, String vnetName, String subnetName,
                                               String discoveryMethod, HostType hostType, ESLogger logger) {

        List<String> ipList = new ArrayList<>();
        ArrayList<ResourceId> ipConfigurations = new ArrayList<>();

        try {
            List<Subnet> subnets = networkResourceProviderClient.getVirtualNetworksOperations().get(rgName, vnetName).getVirtualNetwork().getSubnets();
            if (discoveryMethod.equalsIgnoreCase(AzureDiscovery.VNET)) {
                for (Subnet subnet : subnets) {
                    ipConfigurations.addAll(subnet.getIpConfigurations());
                }
            } else {
                for (Subnet subnet : subnets) {
                    if (subnet.getName().equalsIgnoreCase(subnetName)) {
                        ipConfigurations.addAll(subnet.getIpConfigurations());
                    }
                }

            }

            for (ResourceId resourceId : ipConfigurations) {
                String[] nicURI = resourceId.getId().split("/");
                NetworkInterface nic = networkResourceProviderClient.getNetworkInterfacesOperations().get(rgName, nicURI[
                        nicURI.length - 3]).getNetworkInterface();
                ArrayList<NetworkInterfaceIpConfiguration> ips = nic.getIpConfigurations();

                // find public ip address
                for (NetworkInterfaceIpConfiguration ipConfiguration : ips) {

                    String networkAddress = null;
                    // Let's detect if we want to use public or private IP
                    switch (hostType) {
                        case PRIVATE_IP:
                            InetAddress privateIp = InetAddress.getByName(ipConfiguration.getPrivateIpAddress());

                            if (privateIp != null) {
                                networkAddress = NetworkAddress.formatAddress(privateIp);
                            } else {
                                logger.trace("no private ip provided. ignoring [{}]...", nic.getName());
                            }
                            break;
                        case PUBLIC_IP:
                            String[] pipID = ipConfiguration.getPublicIpAddress().getId().split("/");
                            PublicIpAddress pip = networkResourceProviderClient.getPublicIpAddressesOperations()
                                    .get(rgName, pipID[pipID.length - 1]).getPublicIpAddress();

                            networkAddress = NetworkAddress.formatAddress(InetAddress.getByName(pip.getIpAddress()));

                            if (networkAddress == null) {
                                logger.trace("no public ip provided. ignoring [{}]...", nic.getName());
                            }
                            break;
                        default:
                            // This could never happen!
                            logger.warn("undefined host_type [{}]. Please check your settings.", hostType);
                            return ipList;
                    }

                    if (networkAddress == null) {
                        // We have a bad parameter here or not enough information from azure
                        logger.warn("no network address found. ignoring [{}]...", nic.getName());
                        continue;
                    } else {
                        ipList.add(networkAddress);
                    }

                }
            }
        } catch (Exception e) {
           logger.error(e.getMessage());
        }
        return ipList;
    }
}
