/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Objects;

public final class NodeAndClusterAlias {
    private final DiscoveryNode node;
    private final String clusterAlias;

    public NodeAndClusterAlias(DiscoveryNode node, String clusterAlias) {
        this.node = Objects.requireNonNull(node);
        this.clusterAlias = clusterAlias;
    }

    public DiscoveryNode getNode() {
        return node;
    }

    public String getClusterAlias() {
        return clusterAlias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeAndClusterAlias that = (NodeAndClusterAlias) o;
        return node.equals(that.node) && Objects.equals(clusterAlias, that.clusterAlias);
    }

    @Override
    public int hashCode() {
        return node.hashCode() * 31 + (clusterAlias == null ? 0 : clusterAlias.hashCode());
    }

    @Override
    public String toString() {
        return "cluster[" + clusterAlias + "] node [" + node + "]";
    }
}
