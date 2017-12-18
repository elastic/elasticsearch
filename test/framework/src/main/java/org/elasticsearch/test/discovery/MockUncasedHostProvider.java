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
package org.elasticsearch.test.discovery;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link UnicastHostsProvider} implementation which returns results based on a static in-memory map. This allows running
 * with nodes that only determine their transport address at runtime, which is the default behavior of
 * {@link org.elasticsearch.test.InternalTestCluster}
 */
public final class MockUncasedHostProvider implements UnicastHostsProvider, Closeable {

    static final Map<ClusterName, Set<MockUncasedHostProvider>> activeNodesPerCluster = new HashMap<>();


    private final Supplier<DiscoveryNode> localNodeSupplier;
    private final ClusterName clusterName;

    public MockUncasedHostProvider(Supplier<DiscoveryNode> localNodeSupplier, ClusterName clusterName) {
        this.localNodeSupplier = localNodeSupplier;
        this.clusterName = clusterName;
        synchronized (activeNodesPerCluster) {
            getActiveNodesForCurrentCluster().add(this);
        }
    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        synchronized (activeNodesPerCluster) {
            Set<MockUncasedHostProvider> activeNodes = getActiveNodesForCurrentCluster();
            return activeNodes.stream()
                .map(MockUncasedHostProvider::getNode)
                .filter(n -> !localNodeSupplier.get().equals(n))
                .collect(Collectors.toList());
        }
    }

    private DiscoveryNode getNode() {
        return localNodeSupplier.get();
    }

    private Set<MockUncasedHostProvider> getActiveNodesForCurrentCluster() {
        assert Thread.holdsLock(activeNodesPerCluster);
        return activeNodesPerCluster.computeIfAbsent(clusterName,
            clusterName -> ConcurrentCollections.newConcurrentSet());
    }

    @Override
    public void close() {
        synchronized (activeNodesPerCluster) {
            boolean found = getActiveNodesForCurrentCluster().remove(this);
            assert found;
        }
    }
}
