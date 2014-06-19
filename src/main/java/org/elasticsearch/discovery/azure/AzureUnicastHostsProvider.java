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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.cloud.azure.AzureComputeService;
import org.elasticsearch.cloud.azure.Instance;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class AzureUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    public static enum HostType {
        PRIVATE_IP,
        PUBLIC_IP
    }


    private final AzureComputeService azureComputeService;
    private TransportService transportService;
    private NetworkService networkService;

    private final TimeValue refreshInterval;
    private long lastRefresh;
    private List<DiscoveryNode> cachedDiscoNodes;
    private final HostType host_type;

    @Inject
    public AzureUnicastHostsProvider(Settings settings, AzureComputeService azureComputeService,
                                   TransportService transportService,
                                   NetworkService networkService) {
        super(settings);
        this.azureComputeService = azureComputeService;
        this.transportService = transportService;
        this.networkService = networkService;

        this.refreshInterval = componentSettings.getAsTime(AzureComputeService.Fields.REFRESH,
                settings.getAsTime("cloud.azure." + AzureComputeService.Fields.REFRESH, TimeValue.timeValueSeconds(0)));
        this.host_type = HostType.valueOf(componentSettings.get(AzureComputeService.Fields.HOST_TYPE,
                settings.get("cloud.azure." + AzureComputeService.Fields.HOST_TYPE, HostType.PRIVATE_IP.name())).toUpperCase());

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
                if (logger.isTraceEnabled()) logger.trace("using cache to retrieve node list");
                return cachedDiscoNodes;
            }
            lastRefresh = System.currentTimeMillis();
        }
        logger.debug("start building nodes list using Azure API");

        cachedDiscoNodes = Lists.newArrayList();

        Set<Instance> response = azureComputeService.instances();

        String ipAddress = null;
        try {
            InetAddress inetAddress = networkService.resolvePublishHostAddress(null);
            if (inetAddress != null) {
                ipAddress = inetAddress.getHostAddress();
            }
            if (logger.isTraceEnabled()) logger.trace("ipAddress found: [{}]", ipAddress);
        } catch (IOException e) {
            // We can't find the publish host address... Hmmm. Too bad :-(
            if (logger.isTraceEnabled()) logger.trace("exception while finding ipAddress", e);
        }

        try {
            for (Instance instance : response) {
                String networkAddress = null;
                // Let's detect if we want to use public or private IP
                if (host_type == HostType.PRIVATE_IP) {
                    if (instance.getPrivateIp() != null) {
                        if (logger.isTraceEnabled() && instance.getPrivateIp().equals(ipAddress)) {
                            logger.trace("adding ourselves {}", ipAddress);
                        }

                        networkAddress = instance.getPrivateIp();
                    } else {
                        logger.trace("no private ip provided ignoring {}", instance.getName());
                    }
                }
                if (host_type == HostType.PUBLIC_IP) {
                    if (instance.getPublicIp() != null && instance.getPublicPort() != null) {
                        networkAddress = instance.getPublicIp() + ":" +instance.getPublicPort();
                    } else {
                        logger.trace("no public ip provided ignoring {}", instance.getName());
                    }
                }

                if (networkAddress == null) {
                    // We have a bad parameter here or not enough information from azure
                    throw new ElasticsearchIllegalArgumentException("can't find any " + host_type.name() + " address");
                } else {
                    TransportAddress[] addresses = transportService.addressesFromString(networkAddress);
                    // we only limit to 1 addresses, makes no sense to ping 100 ports
                    logger.trace("adding {}, transport_address {}", networkAddress, addresses[0]);
                    cachedDiscoNodes.add(new DiscoveryNode("#cloud-" + instance.getName(), addresses[0], Version.CURRENT));
                }

            }
        } catch (Throwable e) {
            logger.warn("Exception caught during discovery {} : {}", e.getClass().getName(), e.getMessage());
            logger.trace("Exception caught during discovery", e);
        }

        logger.debug("{} node(s) added", cachedDiscoNodes.size());
        logger.debug("using dynamic discovery nodes {}", cachedDiscoNodes);

        return cachedDiscoNodes;
    }
}
