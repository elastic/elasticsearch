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
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * A publication can succeed and complete before all nodes have applied the published state and acknowledged it; however we need every node
 * eventually either to apply the published state (or a later state) or be removed from the cluster. This component achieves this by
 * removing any lagging nodes from the cluster after a timeout.
 */
public class LagDetector {

    private static final Logger logger = LogManager.getLogger(LagDetector.class);

    // the timeout for each node to apply a cluster state update after the leader has applied it, before being removed from the cluster
    public static final Setting<TimeValue> CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.follower_lag.timeout",
            TimeValue.timeValueMillis(90000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final TimeValue clusterStateApplicationTimeout;
    private final Consumer<DiscoveryNode> onLagDetected;
    private final Supplier<DiscoveryNode> localNodeSupplier;
    private final ThreadPool threadPool;
    private final Map<DiscoveryNode, NodeAppliedStateTracker> appliedStateTrackersByNode = newConcurrentMap();

    public LagDetector(final Settings settings, final ThreadPool threadPool, final Consumer<DiscoveryNode> onLagDetected,
                       final Supplier<DiscoveryNode> localNodeSupplier) {
        this.threadPool = threadPool;
        this.clusterStateApplicationTimeout = CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.get(settings);
        this.onLagDetected = onLagDetected;
        this.localNodeSupplier = localNodeSupplier;
    }

    public void setTrackedNodes(final Iterable<DiscoveryNode> discoveryNodes) {
        final Set<DiscoveryNode> discoveryNodeSet = new HashSet<>();
        discoveryNodes.forEach(discoveryNodeSet::add);
        discoveryNodeSet.remove(localNodeSupplier.get());
        appliedStateTrackersByNode.keySet().retainAll(discoveryNodeSet);
        discoveryNodeSet.forEach(node -> appliedStateTrackersByNode.putIfAbsent(node, new NodeAppliedStateTracker(node)));
    }

    public void clearTrackedNodes() {
        appliedStateTrackersByNode.clear();
    }

    public void setAppliedVersion(final DiscoveryNode discoveryNode, final long appliedVersion) {
        final NodeAppliedStateTracker nodeAppliedStateTracker = appliedStateTrackersByNode.get(discoveryNode);
        if (nodeAppliedStateTracker == null) {
            // Received an ack from a node that a later publication has removed (or we are no longer master). No big deal.
            logger.trace("node {} applied version {} but this node's version is not being tracked", discoveryNode, appliedVersion);
        } else {
            nodeAppliedStateTracker.increaseAppliedVersion(appliedVersion);
        }
    }

    public void startLagDetector(final long version) {
        final List<NodeAppliedStateTracker> laggingTrackers
            = appliedStateTrackersByNode.values().stream().filter(t -> t.appliedVersionLessThan(version)).collect(Collectors.toList());

        if (laggingTrackers.isEmpty()) {
            logger.trace("lag detection for version {} is unnecessary: {}", version, appliedStateTrackersByNode.values());
        } else {
            logger.debug("starting lag detector for version {}: {}", version, laggingTrackers);

            threadPool.scheduleUnlessShuttingDown(clusterStateApplicationTimeout, Names.GENERIC, new Runnable() {
                @Override
                public void run() {
                    laggingTrackers.forEach(t -> t.checkForLag(version));
                }

                @Override
                public String toString() {
                    return "lag detector for version " + version + " on " + laggingTrackers;
                }
            });
        }
    }

    @Override
    public String toString() {
        return "LagDetector{" +
            "clusterStateApplicationTimeout=" + clusterStateApplicationTimeout +
            ", appliedStateTrackersByNode=" + appliedStateTrackersByNode.values() +
            '}';
    }

    // for assertions
    Set<DiscoveryNode> getTrackedNodes() {
        return Collections.unmodifiableSet(appliedStateTrackersByNode.keySet());
    }

    private class NodeAppliedStateTracker {
        private final DiscoveryNode discoveryNode;
        private final AtomicLong appliedVersion = new AtomicLong();

        NodeAppliedStateTracker(final DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        void increaseAppliedVersion(long appliedVersion) {
            long maxAppliedVersion = this.appliedVersion.updateAndGet(v -> Math.max(v, appliedVersion));
            logger.trace("{} applied version {}, max now {}", this, appliedVersion, maxAppliedVersion);
        }

        boolean appliedVersionLessThan(final long version) {
            return appliedVersion.get() < version;
        }

        @Override
        public String toString() {
            return "NodeAppliedStateTracker{" +
                "discoveryNode=" + discoveryNode +
                ", appliedVersion=" + appliedVersion +
                '}';
        }

        void checkForLag(final long version) {
            if (appliedStateTrackersByNode.get(discoveryNode) != this) {
                logger.trace("{} no longer active when checking version {}", this, version);
                return;
            }

            long appliedVersion = this.appliedVersion.get();
            if (version <= appliedVersion) {
                logger.trace("{} satisfied when checking version {}, node applied version {}", this, version, appliedVersion);
                return;
            }

            logger.warn(
                "node [{}] is lagging at cluster state version [{}], although publication of cluster state version [{}] completed [{}] ago",
                discoveryNode, appliedVersion, version, clusterStateApplicationTimeout);
            onLagDetected.accept(discoveryNode);
        }
    }
}
