/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity.memory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AutoscalingMemoryInfoService {
    public static final Setting<TimeValue> FETCH_TIMEOUT = Setting.timeSetting(
        "xpack.autoscaling.memory.monitor.timeout",
        TimeValue.timeValueSeconds(15),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final Long FETCHING_SENTINEL = Long.MIN_VALUE;

    private static final Logger logger = LogManager.getLogger(AutoscalingMemoryInfoService.class);

    private volatile ImmutableOpenMap<String, Long> nodeToMemory = ImmutableOpenMap.<String, Long>builder().build();
    private volatile TimeValue fetchTimeout;

    private final Client client;
    private final Object mutex = new Object();

    @Inject
    public AutoscalingMemoryInfoService(ClusterService clusterService, Client client) {

        this.client = client;
        this.fetchTimeout = FETCH_TIMEOUT.get(clusterService.getSettings());
        if (DiscoveryNode.isMasterNode(clusterService.getSettings())) {
            clusterService.addListener(this::onClusterChanged);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(FETCH_TIMEOUT, this::setFetchTimeout);
        }
    }

    private void setFetchTimeout(TimeValue fetchTimeout) {
        this.fetchTimeout = fetchTimeout;
    }

    void onClusterChanged(ClusterChangedEvent event) {
        boolean master = event.localNodeMaster();
        final ClusterState state = event.state();
        final Set<DiscoveryNode> currentNodes = master ? relevantNodes(state) : Set.of();
        Set<DiscoveryNode> missingNodes = null;
        synchronized (mutex) {
            retainAliveNodes(currentNodes);
            if (master) {
                missingNodes = addMissingNodes(currentNodes);
            }
        }
        if (missingNodes != null) {
            sendToMissingNodes(state.nodes()::get, missingNodes);
        }
    }

    Set<DiscoveryNode> relevantNodes(ClusterState state) {
        final Set<Set<DiscoveryNodeRole>> roleSets = calculateAutoscalingRoleSets(state);
        return StreamSupport.stream(state.nodes().spliterator(), false)
            .filter(n -> roleSets.contains(n.getRoles()))
            .collect(Collectors.toSet());
    }

    private Set<DiscoveryNode> addMissingNodes(Set<DiscoveryNode> nodes) {
        // we retain only current nodes first, so we can use size check for equality.
        if (nodes.size() != nodeToMemory.size()) {
            Set<DiscoveryNode> missingNodes = nodes.stream()
                .filter(dn -> nodeToMemory.containsKey(dn.getEphemeralId()) == false)
                .collect(Collectors.toSet());
            if (missingNodes.size() > 0) {
                ImmutableOpenMap.Builder<String, Long> builder = ImmutableOpenMap.<String, Long>builder(nodeToMemory);
                missingNodes.stream().map(DiscoveryNode::getEphemeralId).forEach(id -> builder.put(id, FETCHING_SENTINEL));
                nodeToMemory = builder.build();

                return missingNodes;
            }
        }

        return null;
    }

    private void sendToMissingNodes(Function<String, DiscoveryNode> nodeLookup, Set<DiscoveryNode> missingNodes) {
        client.admin()
            .cluster()
            .nodesStats(
                new NodesStatsRequest(missingNodes.stream().map(DiscoveryNode::getId).toArray(String[]::new)).clear()
                    .addMetric(NodesStatsRequest.Metric.OS.metricName())
                    .timeout(fetchTimeout),
                new ActionListener<NodesStatsResponse>() {
                    @Override
                    public void onResponse(NodesStatsResponse nodesStatsResponse) {
                        synchronized (mutex) {
                            ImmutableOpenMap.Builder<String, Long> builder = ImmutableOpenMap.<String, Long>builder(nodeToMemory);
                            nodesStatsResponse.failures()
                                .stream()
                                .map(FailedNodeException::nodeId)
                                .map(nodeLookup)
                                .map(DiscoveryNode::getEphemeralId)
                                .forEach(builder::remove);

                            nodesStatsResponse.getNodes().forEach(nodeStats -> addNodeStats(builder, nodeStats));
                            nodeToMemory = builder.build();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        synchronized (mutex) {
                            ImmutableOpenMap.Builder<String, Long> builder = ImmutableOpenMap.<String, Long>builder(nodeToMemory);
                            missingNodes.stream().map(DiscoveryNode::getEphemeralId).forEach(builder::remove);
                            nodeToMemory = builder.build();
                        }

                        logger.warn("Unable to obtain memory info from [{}]", missingNodes);
                    }
                }
            );
    }

    private Set<Set<DiscoveryNodeRole>> calculateAutoscalingRoleSets(ClusterState state) {
        AutoscalingMetadata autoscalingMetadata = state.metadata().custom(AutoscalingMetadata.NAME);
        if (autoscalingMetadata != null) {
            return autoscalingMetadata.policies()
                .values()
                .stream()
                .map(AutoscalingPolicyMetadata::policy)
                .map(AutoscalingPolicy::roles)
                .map(this::toRoles)
                .collect(Collectors.toSet());
        }
        return Set.of();
    }

    private Set<DiscoveryNodeRole> toRoles(SortedSet<String> roleNames) {
        return roleNames.stream().map(DiscoveryNodeRole::getRoleFromRoleName).collect(Collectors.toSet());
    }

    private void retainAliveNodes(Set<DiscoveryNode> currentNodes) {
        assert Thread.holdsLock(mutex);
        Set<String> ephemeralIds = currentNodes.stream().map(DiscoveryNode::getEphemeralId).collect(Collectors.toSet());
        Set<String> toRemove = StreamSupport.stream(nodeToMemory.keys().spliterator(), false)
            .map(c -> c.value)
            .filter(Predicate.not(ephemeralIds::contains))
            .collect(Collectors.toSet());
        if (toRemove.isEmpty() == false) {
            ImmutableOpenMap.Builder<String, Long> builder = ImmutableOpenMap.<String, Long>builder(nodeToMemory);
            builder.removeAll(toRemove::contains);
            nodeToMemory = builder.build();
        }
    }

    private void addNodeStats(ImmutableOpenMap.Builder<String, Long> builder, NodeStats nodeStats) {
        // we might add nodes that already died here, but those will be removed on next cluster state update anyway and is only a small
        // waste.
        builder.put(nodeStats.getNode().getEphemeralId(), nodeStats.getOs().getMem().getTotal().getBytes());
    }

    public AutoscalingMemoryInfo snapshot() {
        final ImmutableOpenMap<String, Long> nodeToMemory = this.nodeToMemory;
        return node -> {
            Long result = nodeToMemory.get(node.getEphemeralId());
            // noinspection NumberEquality
            if (result == FETCHING_SENTINEL) {
                return null;
            } else {
                return result;
            }
        };
    }
}
