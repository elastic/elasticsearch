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

package org.elasticsearch.discovery.zen.elect;

import com.google.common.collect.Lists;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public class ElectMasterService extends AbstractComponent {

    private final NodeComparator nodeComparator = new NodeComparator();

    public ElectMasterService(Settings settings) {
        super(settings);
    }

    /**
     * Returns a list of the next possible masters.
     */
    public DiscoveryNode[] nextPossibleMasters(Iterable<DiscoveryNode> nodes, int numberOfPossibleMasters) {
        List<DiscoveryNode> sortedNodes = sortedNodes(nodes);
        if (sortedNodes == null) {
            return new DiscoveryNode[0];
        }
        List<DiscoveryNode> nextPossibleMasters = newArrayListWithExpectedSize(numberOfPossibleMasters);
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
        List<DiscoveryNode> sortedNodes = sortedNodes(nodes);
        if (sortedNodes == null) {
            return null;
        }
        return sortedNodes.get(0);
    }

    private List<DiscoveryNode> sortedNodes(Iterable<DiscoveryNode> nodes) {
        List<DiscoveryNode> possibleNodes = Lists.newArrayList(nodes);
        if (possibleNodes.isEmpty()) {
            return null;
        }
        Collections.sort(possibleNodes, nodeComparator);
        return possibleNodes;
    }

    private static class NodeComparator implements Comparator<DiscoveryNode> {

        @Override public int compare(DiscoveryNode o1, DiscoveryNode o2) {
            return o1.id().compareTo(o2.id());
        }
    }
}
