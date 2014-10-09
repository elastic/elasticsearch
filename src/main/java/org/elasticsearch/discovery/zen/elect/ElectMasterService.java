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

package org.elasticsearch.discovery.zen.elect;

import com.carrotsearch.hppc.ObjectContainer;
import com.google.common.collect.Lists;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.*;

/**
 *
 */
public class ElectMasterService extends AbstractComponent {

    public static final String DISCOVERY_ZEN_MINIMUM_MASTER_NODES = "discovery.zen.minimum_master_nodes";

    private final NodeComparator nodeComparator = new NodeComparator();

    private volatile int minimumMasterNodes;

    @Inject
    public ElectMasterService(Settings settings) {
        super(settings);
        this.minimumMasterNodes = settings.getAsInt(DISCOVERY_ZEN_MINIMUM_MASTER_NODES, -1);
        logger.debug("using minimum_master_nodes [{}]", minimumMasterNodes);
    }

    public void minimumMasterNodes(int minimumMasterNodes) {
        this.minimumMasterNodes = minimumMasterNodes;
    }

    public int minimumMasterNodes() {
        return minimumMasterNodes;
    }

    public boolean hasEnoughMasterNodes(Iterable<DiscoveryNode> nodes) {
        if (minimumMasterNodes < 1) {
            return true;
        }
        int count = 0;
        for (DiscoveryNode node : nodes) {
            if (node.masterNode()) {
                count++;
            }
        }
        return count >= minimumMasterNodes;
    }

    /**
     * Returns the given nodes sorted by likelyhood of being elected as master, most likely first.
     * Non-master nodes are not removed but are rather put in the end
     * @param nodes
     * @return
     */
    public List<DiscoveryNode> sortByMasterLikelihood(Iterable<DiscoveryNode> nodes) {
        ArrayList<DiscoveryNode> sortedNodes = Lists.newArrayList(nodes);
        CollectionUtil.introSort(sortedNodes, nodeComparator);
        return sortedNodes;
    }

    /**
     * Returns a list of the next possible masters.
     */
    public DiscoveryNode[] nextPossibleMasters(ObjectContainer<DiscoveryNode> nodes, int numberOfPossibleMasters) {
        List<DiscoveryNode> sortedNodes = sortedMasterNodes(Arrays.asList(nodes.toArray(DiscoveryNode.class)));
        if (sortedNodes == null) {
            return new DiscoveryNode[0];
        }
        List<DiscoveryNode> nextPossibleMasters = Lists.newArrayListWithCapacity(numberOfPossibleMasters);
        int counter = 0;
        for (DiscoveryNode nextPossibleMaster : sortedNodes) {
            if (++counter >= numberOfPossibleMasters) {
                break;
            }
            nextPossibleMasters.add(nextPossibleMaster);
        }
        return nextPossibleMasters.toArray(new DiscoveryNode[nextPossibleMasters.size()]);
    }

    /**
     * Elects a new master out of the possible nodes, returning it. Returns <tt>null</tt>
     * if no master has been elected.
     */
    public DiscoveryNode electMaster(Iterable<DiscoveryNode> nodes) {
        List<DiscoveryNode> sortedNodes = sortedMasterNodes(nodes);
        if (sortedNodes == null || sortedNodes.isEmpty()) {
            return null;
        }
        return sortedNodes.get(0);
    }

    private List<DiscoveryNode> sortedMasterNodes(Iterable<DiscoveryNode> nodes) {
        List<DiscoveryNode> possibleNodes = Lists.newArrayList(nodes);
        if (possibleNodes.isEmpty()) {
            return null;
        }
        // clean non master nodes
        for (Iterator<DiscoveryNode> it = possibleNodes.iterator(); it.hasNext(); ) {
            DiscoveryNode node = it.next();
            if (!node.masterNode()) {
                it.remove();
            }
        }
        CollectionUtil.introSort(possibleNodes, nodeComparator);
        return possibleNodes;
    }

    private static class NodeComparator implements Comparator<DiscoveryNode> {

        @Override
        public int compare(DiscoveryNode o1, DiscoveryNode o2) {
            if (o1.masterNode() && !o2.masterNode()) {
                return -1;
            }
            if (!o1.masterNode() && o2.masterNode()) {
                return 1;
            }
            return o1.id().compareTo(o2.id());
        }
    }
}
