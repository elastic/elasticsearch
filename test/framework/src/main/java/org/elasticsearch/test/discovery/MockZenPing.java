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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.discovery.zen.PingContextProvider;
import org.elasticsearch.discovery.zen.ZenPing;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A {@link ZenPing} implementation which returns results based on a static in-memory map. This allows pinging
 * to be immediate and can be used to speed up tests.
 */
public final class MockZenPing extends AbstractComponent implements ZenPing {

    static final Map<ClusterName, Set<MockZenPing>> activeNodesPerCluster = new HashMap<>();

    /** a set of the last discovered pings. used to throttle busy spinning where MockZenPing will keep returning the same results */
    private Set<MockZenPing> lastDiscoveredPings = null;

    private final PingContextProvider contextProvider;

    public MockZenPing(Settings settings, PingContextProvider contextProvider) {
        super(settings);
        this.contextProvider = contextProvider;
    }

    @Override
    public void start() {
        synchronized (activeNodesPerCluster) {
            boolean added = getActiveNodesForCurrentCluster().add(this);
            assert added;
            activeNodesPerCluster.notifyAll();
        }
    }

    @Override
    public void ping(Consumer<PingCollection> resultsConsumer, TimeValue timeout) {
        logger.info("pinging using mock zen ping");
        synchronized (activeNodesPerCluster) {
            Set<MockZenPing> activeNodes = getActiveNodesForCurrentCluster();
            if (activeNodes.equals(lastDiscoveredPings)) {
                try {
                    logger.trace("nothing has changed since the last ping. waiting for a change");
                    activeNodesPerCluster.wait(timeout.millis());
                } catch (InterruptedException e) {

                }
                activeNodes = getActiveNodesForCurrentCluster();
            }
            lastDiscoveredPings = activeNodes;
            PingCollection pingCollection = new PingCollection();
            activeNodes.stream()
                .filter(p -> p != this) // remove this as pings are not expected to return the local node
                .map(MockZenPing::getPingResponse)
                .forEach(pingCollection::addPing);
            resultsConsumer.accept(pingCollection);
        }
    }

    private ClusterName getClusterName() {
        return contextProvider.clusterState().getClusterName();
    }

    private PingResponse getPingResponse() {
        final ClusterState clusterState = contextProvider.clusterState();
        return new PingResponse(clusterState.nodes().getLocalNode(), clusterState.nodes().getMasterNode(), clusterState);
    }

    private Set<MockZenPing> getActiveNodesForCurrentCluster() {
        assert Thread.holdsLock(activeNodesPerCluster);
        return activeNodesPerCluster.computeIfAbsent(getClusterName(),
            clusterName -> ConcurrentCollections.newConcurrentSet());
    }

    @Override
    public void close() {
        synchronized (activeNodesPerCluster) {
            boolean found = getActiveNodesForCurrentCluster().remove(this);
            assert found;
            activeNodesPerCluster.notifyAll();
        }
    }
}
