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

package org.elasticsearch.client.cluster;

import java.util.List;
import java.util.Objects;

public class SniffModeInfo implements RemoteConnectionInfo.ModeInfo {
    public static final String NAME = "sniff";
    static final String SEEDS = "seeds";
    static final String NUM_NODES_CONNECTED = "num_nodes_connected";
    static final String MAX_CONNECTIONS_PER_CLUSTER = "max_connections_per_cluster";
    final List<String> seedNodes;
    final int maxConnectionsPerCluster;
    final int numNodesConnected;

    SniffModeInfo(List<String> seedNodes, int maxConnectionsPerCluster, int numNodesConnected) {
        this.seedNodes = seedNodes;
        this.maxConnectionsPerCluster = maxConnectionsPerCluster;
        this.numNodesConnected = numNodesConnected;
    }

    @Override
    public boolean isConnected() {
        return numNodesConnected > 0;
    }

    @Override
    public String modeName() {
        return NAME;
    }

    public List<String> getSeedNodes() {
        return seedNodes;
    }

    public int getMaxConnectionsPerCluster() {
        return maxConnectionsPerCluster;
    }

    public int getNumNodesConnected() {
        return numNodesConnected;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SniffModeInfo sniff = (SniffModeInfo) o;
        return maxConnectionsPerCluster == sniff.maxConnectionsPerCluster &&
                numNodesConnected == sniff.numNodesConnected &&
                Objects.equals(seedNodes, sniff.seedNodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seedNodes, maxConnectionsPerCluster, numNodesConnected);
    }
}
