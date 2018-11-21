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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * A publication can succeed and complete before all nodes have applied the published state and acknowledged it; however we need every node
 * eventually either to apply the published state (or a later state) or be removed from the cluster. This component achieves this by
 * removing any lagging nodes from the cluster after a timeout.
 */
public class LagDetector {

    private static final Logger logger = LogManager.getLogger(LagDetector.class);

    // the timeout for each node to apply a value after the end of publication, before being removed from the cluster
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
            logger.trace("node {} applied version {} but this node's version is not being tracked", discoveryNode, appliedVersion);
        } else {
            nodeAppliedStateTracker.increaseAppliedVersion(appliedVersion);
        }
    }

    public void startLagDetector(final long version) {
        appliedStateTrackersByNode.values().forEach(nodeAppliedStateTracker -> nodeAppliedStateTracker.startLagDetector(version));
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

        @Override
        public String toString() {
            return "NodeAppliedStateTracker{" +
                "discoveryNode=" + discoveryNode +
                ", appliedVersion=" + appliedVersion +
                '}';
        }

        void startLagDetector(final long version) {
            final long appliedVersionWhenStarted = appliedVersion.get();
            if (version <= appliedVersionWhenStarted) {
                logger.trace("lag detection for {} for version {} unnecessary, node has already applied version {}",
                    discoveryNode, version, appliedVersionWhenStarted);
                return;
            }

            threadPool.scheduleUnlessShuttingDown(clusterStateApplicationTimeout, Names.GENERIC, new Runnable() {
                @Override
                public void run() {
                    if (appliedStateTrackersByNode.get(discoveryNode) != NodeAppliedStateTracker.this) {
                        logger.trace("{}, no longer active", this);
                        return;
                    }

                    long appliedVersion = NodeAppliedStateTracker.this.appliedVersion.get();
                    if (version <= appliedVersion) {
                        logger.trace("{}, satisfied, node applied version {}", this, appliedVersion);
                        return;
                    }

                    logger.debug("{}, detected lag, node has only applied version {}", this, appliedVersion);
                    onLagDetected.accept(discoveryNode);
                }

                @Override
                public String toString() {
                    return "lag detection for " + discoveryNode + " started at version " + appliedVersionWhenStarted
                        + ", expected version " + version;
                }
            });
        }
    }
}
