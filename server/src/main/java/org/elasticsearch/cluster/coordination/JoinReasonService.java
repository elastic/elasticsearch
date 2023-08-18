/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.coordination.Coordinator.Mode.CANDIDATE;

/**
 * Tracks nodes that were recently in the cluster, and uses this information to give extra details if these nodes rejoin the cluster.
 */
public class JoinReasonService {

    private final LongSupplier relativeTimeInMillisSupplier;

    // only used on cluster applier thread to detect changes, no need for synchronization, and deliberately an Object to remove temptation
    // to use this field for other reasons
    private Object discoveryNodes;

    // keyed by persistent node ID to track nodes across restarts
    private final Map<String, TrackedNode> trackedNodes = ConcurrentCollections.newConcurrentMap();

    public JoinReasonService(LongSupplier relativeTimeInMillisSupplier) {
        this.relativeTimeInMillisSupplier = relativeTimeInMillisSupplier;
    }

    /**
     * Called when a new cluster state was applied by a master-eligible node, possibly adding or removing some nodes.
     */
    public void onClusterStateApplied(DiscoveryNodes discoveryNodes) {
        assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
        assert discoveryNodes.getLocalNode().isMasterNode();

        if (this.discoveryNodes != discoveryNodes) {
            this.discoveryNodes = discoveryNodes;

            final Set<String> absentNodeIds = new HashSet<>(trackedNodes.keySet());
            for (final DiscoveryNode discoveryNode : discoveryNodes) {
                trackedNodes.compute(
                    discoveryNode.getId(),
                    (ignored, trackedNode) -> (trackedNode == null ? UNKNOWN_NODE : trackedNode).present(discoveryNode.getEphemeralId())
                );
                absentNodeIds.remove(discoveryNode.getId());
            }

            final long currentTimeMillis = relativeTimeInMillisSupplier.getAsLong();

            for (final String absentNodeId : absentNodeIds) {
                trackedNodes.computeIfPresent(
                    absentNodeId,
                    (ignored, trackedNode) -> trackedNode.absent(currentTimeMillis, discoveryNodes.getMasterNode())
                );
            }

            if (absentNodeIds.size() > 2 * discoveryNodes.getSize()) {
                final List<Tuple<Long, String>> absentNodes = new ArrayList<>(absentNodeIds.size());
                for (Map.Entry<String, TrackedNode> trackedNodesEntry : trackedNodes.entrySet()) {
                    final long removalAgeMillis = trackedNodesEntry.getValue().getRemovalAgeMillis(currentTimeMillis);
                    if (removalAgeMillis != NOT_REMOVED) {
                        absentNodes.add(Tuple.tuple(removalAgeMillis, trackedNodesEntry.getKey()));
                    }
                }
                absentNodes.sort(Comparator.comparing(Tuple::v1));
                for (int i = discoveryNodes.getSize(); i < absentNodes.size(); i++) {
                    trackedNodes.remove(absentNodes.get(i).v2());
                }
            }

            assert trackedNodes.size() <= discoveryNodes.getSize() * 3;
        }
    }

    /**
     * Called on the master when a {@code node-left} task completes successfully and after the resulting cluster state is applied. If the
     * absent node is still tracked then this adds the removal reason ({@code disconnected}, {@code lagging}, etc.) to the tracker.
     */
    public void onNodeRemoved(DiscoveryNode discoveryNode, String reason) {
        assert MasterService.assertMasterUpdateOrTestThread();
        trackedNodes.computeIfPresent(discoveryNode.getId(), (ignored, trackedNode) -> trackedNode.withRemovalReason(reason));
    }

    /**
     * @param discoveryNode The joining node.
     * @param currentMode   The current mode of the master that the node is joining.
     * @return              A description of the reason for the join, possibly including some details of its earlier removal.
     */
    public JoinReason getJoinReason(DiscoveryNode discoveryNode, Coordinator.Mode currentMode) {
        return trackedNodes.getOrDefault(discoveryNode.getId(), UNKNOWN_NODE)
            .getJoinReason(relativeTimeInMillisSupplier.getAsLong(), discoveryNode.getEphemeralId(), currentMode);
    }

    /**
     * Get the name of the node and do something reasonable if the node is missing or has no name.
     */
    private static String getNodeNameSafe(@Nullable DiscoveryNode discoveryNode) {
        if (discoveryNode == null) {
            return "_unknown_";
        }
        final String name = discoveryNode.getName();
        if (Strings.hasText(name)) {
            return name;
        }
        return discoveryNode.getId();
    }

    private interface TrackedNode {

        TrackedNode present(String ephemeralId);

        TrackedNode absent(long currentTimeMillis, @Nullable DiscoveryNode masterNode);

        TrackedNode withRemovalReason(String removalReason);

        JoinReason getJoinReason(long currentTimeMillis, String joiningNodeEphemeralId, Coordinator.Mode currentMode);

        long getRemovalAgeMillis(long currentTimeMillis);
    }

    private static final long NOT_REMOVED = -1L;

    private static final TrackedNode UNKNOWN_NODE = new TrackedNode() {

        @Override
        public TrackedNode present(String ephemeralId) {
            return new PresentNode(ephemeralId, 0);
        }

        @Override
        public TrackedNode absent(long currentTimeMillis, @Nullable DiscoveryNode masterNode) {
            assert false;
            return this;
        }

        @Override
        public TrackedNode withRemovalReason(String removalReason) {
            assert false;
            return this;
        }

        @Override
        public JoinReason getJoinReason(long currentTimeMillis, String joiningNodeEphemeralId, Coordinator.Mode currentMode) {
            if (currentMode == CANDIDATE) {
                return COMPLETING_ELECTION;
            } else {
                return NEW_NODE_JOINING;
            }
        }

        @Override
        public long getRemovalAgeMillis(long currentTimeMillis) {
            return NOT_REMOVED;
        }
    };

    private record PresentNode(String ephemeralId, int removalCount) implements TrackedNode {

        @Override
        public TrackedNode present(String ephemeralId) {
            return ephemeralId.equals(this.ephemeralId) ? this : new PresentNode(ephemeralId, 0);
        }

        @Override
        public TrackedNode absent(long currentTimeMillis, DiscoveryNode masterNode) {
            return new AbsentNode(ephemeralId, removalCount + 1, currentTimeMillis, getNodeNameSafe(masterNode), null);
        }

        @Override
        public TrackedNode withRemovalReason(String removalReason) {
            return this;
        }

        @Override
        public JoinReason getJoinReason(long currentTimeMillis, String joiningNodeEphemeralId, Coordinator.Mode currentMode) {
            if (currentMode == CANDIDATE) {
                return COMPLETING_ELECTION;
            } else {
                return KNOWN_NODE_REJOINING;
            }
        }

        @Override
        public long getRemovalAgeMillis(long currentTimeMillis) {
            return NOT_REMOVED;
        }
    }

    private record AbsentNode(
        String ephemeralId,
        int removalCount,
        long removalTimeMillis,
        String removingNodeName,
        @Nullable String removalReason // if removal reason not known, e.g. if removed by a different master
    ) implements TrackedNode {

        @Override
        public TrackedNode present(String ephemeralId) {
            return new PresentNode(ephemeralId, ephemeralId.equals(this.ephemeralId) ? removalCount : 0);
        }

        @Override
        public TrackedNode absent(long currentTimeMillis, DiscoveryNode masterNode) {
            return this;
        }

        @Override
        public TrackedNode withRemovalReason(String removalReason) {
            return new AbsentNode(ephemeralId, removalCount, removalTimeMillis, removingNodeName, removalReason);
        }

        @Override
        public JoinReason getJoinReason(long currentTimeMillis, String joiningNodeEphemeralId, Coordinator.Mode currentMode) {
            final StringBuilder description = new StringBuilder();
            if (currentMode == CANDIDATE) {
                description.append("completing election");
            } else {
                description.append("joining");
            }

            final boolean isRestarted = joiningNodeEphemeralId.equals(ephemeralId) == false;
            if (isRestarted) {
                description.append(" after restart");
            }

            description.append(", removed [");
            final long removalAgeMillis = getRemovalAgeMillis(currentTimeMillis);
            if (removalAgeMillis >= 1000) {
                description.append(TimeValue.timeValueMillis(removalAgeMillis)).append("/");
            }
            description.append(removalAgeMillis).append("ms] ago ");

            if (removalReason == null) {
                description.append("by [").append(removingNodeName).append("]");
            } else {
                description.append("with reason [").append(removalReason).append("]");
            }

            if (removalCount > 1 && isRestarted == false) {
                description.append(", [").append(removalCount).append("] total removals");
            }

            return new JoinReason(description.toString(), isRestarted ? null : ReferenceDocs.UNSTABLE_CLUSTER_TROUBLESHOOTING);
        }

        @Override
        public long getRemovalAgeMillis(long currentTimeMillis) {
            return currentTimeMillis - removalTimeMillis;
        }
    }

    private static final JoinReason COMPLETING_ELECTION = new JoinReason("completing election", null);
    private static final JoinReason NEW_NODE_JOINING = new JoinReason("joining", null);
    private static final JoinReason KNOWN_NODE_REJOINING = new JoinReason("rejoining", null);
}
