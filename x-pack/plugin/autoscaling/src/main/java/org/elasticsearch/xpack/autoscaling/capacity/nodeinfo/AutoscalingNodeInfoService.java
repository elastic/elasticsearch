/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity.nodeinfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoMetrics;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableMap;

public class AutoscalingNodeInfoService {
    public static final Setting<TimeValue> FETCH_TIMEOUT = Setting.timeSetting(
        "xpack.autoscaling.memory.monitor.timeout",
        TimeValue.timeValueSeconds(15),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final AutoscalingNodeInfo FETCHING_SENTINEL = new AutoscalingNodeInfo(Long.MIN_VALUE, Processors.MAX_PROCESSORS);

    private static final Logger logger = LogManager.getLogger(AutoscalingNodeInfoService.class);

    private volatile Map<String, AutoscalingNodeInfo> nodeToMemory = Map.of();
    private volatile TimeValue fetchTimeout;

    private final Client client;
    private final Object mutex = new Object();

    @Inject
    public AutoscalingNodeInfoService(ClusterService clusterService, Client client) {
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
        return state.nodes().stream().filter(n -> roleSets.contains(n.getRoles())).collect(Collectors.toSet());
    }

    private Set<DiscoveryNode> addMissingNodes(Set<DiscoveryNode> nodes) {
        // we retain only current nodes first, so we can use size check for equality.
        if (nodes.size() != nodeToMemory.size()) {
            Set<DiscoveryNode> missingNodes = nodes.stream()
                .filter(dn -> nodeToMemory.containsKey(dn.getEphemeralId()) == false)
                .collect(Collectors.toSet());
            if (missingNodes.size() > 0) {
                var builder = new HashMap<>(nodeToMemory);
                missingNodes.stream().map(DiscoveryNode::getEphemeralId).forEach(id -> builder.put(id, FETCHING_SENTINEL));
                nodeToMemory = Collections.unmodifiableMap(builder);
                return missingNodes;
            }
        }

        return null;
    }

    private void sendToMissingNodes(Function<String, DiscoveryNode> nodeLookup, Set<DiscoveryNode> missingNodes) {
        final Runnable onError = () -> {
            synchronized (mutex) {
                var builder = new HashMap<>(nodeToMemory);
                missingNodes.stream().map(DiscoveryNode::getEphemeralId).forEach(builder::remove);
                nodeToMemory = Collections.unmodifiableMap(builder);
            }
        };
        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(
            missingNodes.stream().map(DiscoveryNode::getId).toArray(String[]::new)
        ).clear().addMetric(NodesStatsRequestParameters.Metric.OS).timeout(fetchTimeout);
        nodesStatsRequest.setIncludeShardsStats(false);
        client.admin()
            .cluster()
            .nodesStats(
                nodesStatsRequest,
                ActionListener.wrap(
                    nodesStatsResponse -> client.admin()
                        .cluster()
                        .nodesInfo(
                            // Only gather info for nodes that provided their stats
                            new NodesInfoRequest(
                                nodesStatsResponse.getNodes()
                                    .stream()
                                    .map(BaseNodeResponse::getNode)
                                    .map(DiscoveryNode::getId)
                                    .toArray(String[]::new)
                            ).clear().addMetric(NodesInfoMetrics.Metric.OS.metricName()).timeout(fetchTimeout),
                            ActionListener.wrap(nodesInfoResponse -> {
                                final Map<String, AutoscalingNodeInfo.Builder> builderBuilder = Maps.newHashMapWithExpectedSize(
                                    nodesStatsResponse.getNodes().size()
                                );
                                nodesStatsResponse.getNodes()
                                    .forEach(
                                        nodeStats -> builderBuilder.put(
                                            nodeStats.getNode().getEphemeralId(),
                                            AutoscalingNodeInfo.builder()
                                                .setMemory(nodeStats.getOs().getMem().getAdjustedTotal().getBytes())
                                        )
                                    );
                                nodesInfoResponse.getNodes().forEach(nodeInfo -> {
                                    assert builderBuilder.containsKey(nodeInfo.getNode().getEphemeralId())
                                        : "unexpected missing node when setting processors [" + nodeInfo.getNode().getEphemeralId() + "]";
                                    builderBuilder.computeIfPresent(
                                        nodeInfo.getNode().getEphemeralId(),
                                        (n, b) -> b.setProcessors(nodeInfo.getInfo(OsInfo.class).getFractionalAllocatedProcessors())
                                    );
                                });
                                synchronized (mutex) {
                                    Map<String, AutoscalingNodeInfo> builder = new HashMap<>(nodeToMemory);
                                    // Remove all from the builder that failed getting info and stats
                                    Stream.concat(nodesStatsResponse.failures().stream(), nodesInfoResponse.failures().stream())
                                        .map(FailedNodeException::nodeId)
                                        .map(nodeLookup)
                                        .map(DiscoveryNode::getEphemeralId)
                                        .forEach(builder::remove);

                                    // we might add nodes that already died here,
                                    // but those will be removed on next cluster state update anyway and is only a small waste.
                                    builderBuilder.forEach((nodeEphemeralId, memoryProcessorBuilder) -> {
                                        if (memoryProcessorBuilder.canBuild()) {
                                            builder.put(nodeEphemeralId, memoryProcessorBuilder.build());
                                        }
                                    });
                                    nodeToMemory = Collections.unmodifiableMap(builder);
                                }
                            }, e -> {
                                onError.run();
                                logger.warn(() -> String.format(Locale.ROOT, "Unable to obtain processor info from [%s]", missingNodes), e);
                            })
                        ),
                    e -> {
                        onError.run();
                        logger.warn(() -> String.format(Locale.ROOT, "Unable to obtain memory info from [%s]", missingNodes), e);
                    }
                )
            );
    }

    private static Set<Set<DiscoveryNodeRole>> calculateAutoscalingRoleSets(ClusterState state) {
        AutoscalingMetadata autoscalingMetadata = state.metadata().custom(AutoscalingMetadata.NAME);
        if (autoscalingMetadata != null) {
            return autoscalingMetadata.policies()
                .values()
                .stream()
                .map(AutoscalingPolicyMetadata::policy)
                .map(AutoscalingPolicy::roles)
                .map(AutoscalingNodeInfoService::toRoles)
                .collect(Collectors.toSet());
        }
        return Set.of();
    }

    private static Set<DiscoveryNodeRole> toRoles(SortedSet<String> roleNames) {
        return roleNames.stream().map(DiscoveryNodeRole::getRoleFromRoleName).collect(Collectors.toSet());
    }

    private void retainAliveNodes(Set<DiscoveryNode> currentNodes) {
        assert Thread.holdsLock(mutex);
        Set<String> ephemeralIds = currentNodes.stream().map(DiscoveryNode::getEphemeralId).collect(Collectors.toSet());
        Set<String> toRemove = nodeToMemory.keySet().stream().filter(Predicate.not(ephemeralIds::contains)).collect(Collectors.toSet());
        if (toRemove.isEmpty() == false) {
            nodeToMemory = nodeToMemory.entrySet()
                .stream()
                .filter(n -> toRemove.contains(n.getKey()) == false)
                .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    public AutoscalingNodesInfo snapshot() {
        final Map<String, AutoscalingNodeInfo> nodeToMemoryRef = this.nodeToMemory;
        return node -> {
            AutoscalingNodeInfo result = nodeToMemoryRef.get(node.getEphemeralId());
            if (result == FETCHING_SENTINEL) {
                return Optional.empty();
            } else {
                return Optional.ofNullable(result);
            }
        };
    }
}
