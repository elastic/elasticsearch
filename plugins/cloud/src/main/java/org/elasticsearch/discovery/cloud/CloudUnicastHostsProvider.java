/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.discovery.cloud;

import org.elasticsearch.cloud.compute.CloudComputeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.transport.InetSocketTransportAddress;
import org.elasticsearch.util.transport.PortsRange;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeState;
import org.jclouds.domain.Location;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.util.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public class CloudUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    private final ComputeService computeService;

    private final String ports;

    private final String tag;

    private final String location;

    public CloudUnicastHostsProvider(Settings settings, CloudComputeService computeService) {
        super(settings);
        this.computeService = computeService.context().getComputeService();
        this.tag = componentSettings.get("tag");
        this.location = componentSettings.get("location");
        this.ports = componentSettings.get("ports", "9300-9302");
        // parse the ports just to see that they are valid
        new PortsRange(ports).ports();
    }

    @Override public List<DiscoveryNode> buildDynamicNodes() {
        List<DiscoveryNode> discoNodes = newArrayList();
        Set<? extends ComputeMetadata> nodes = computeService.listNodes();
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("Processing Nodes:");
            for (ComputeMetadata node : nodes) {
                sb.append("\n   -> ").append(node);
            }
            logger.trace(sb.toString());
        }
        for (ComputeMetadata node : nodes) {
            NodeMetadata nodeMetadata;
            if (node instanceof NodeMetadata) {
                nodeMetadata = (NodeMetadata) node;
            } else {
                nodeMetadata = computeService.getNodeMetadata(node.getId());
            }
            if (tag != null && !nodeMetadata.getTag().equals(tag)) {
                logger.trace("Filtering node {} with unmatched tag {}", nodeMetadata.getName(), nodeMetadata.getTag());
                continue;
            }
            boolean filteredByLocation = true;
            if (location != null) {
                Location nodeLocation = nodeMetadata.getLocation();
                if (location.equals(nodeLocation.getId())) {
                    filteredByLocation = false;
                } else {
                    if (nodeLocation.getParent() != null) {
                        if (location.equals(nodeLocation.getParent().getId())) {
                            filteredByLocation = false;
                        }
                    }
                }
            } else {
                filteredByLocation = false;
            }
            if (filteredByLocation) {
                logger.trace("Filtering node {} with unmatched location {}", nodeMetadata.getName(), nodeMetadata.getLocation());
                continue;
            }
            if (nodeMetadata.getState() == NodeState.PENDING || nodeMetadata.getState() == NodeState.RUNNING) {
                logger.debug("Adding {}, addresses {}", nodeMetadata.getName(), nodeMetadata.getPrivateAddresses());
                for (String inetAddress : nodeMetadata.getPrivateAddresses()) {
                    for (int port : new PortsRange(ports).ports()) {
                        discoNodes.add(new DiscoveryNode("#cloud-" + inetAddress + "-" + port, new InetSocketTransportAddress(inetAddress, port)));
                    }
                }
            }
        }
        return discoNodes;
    }
}
