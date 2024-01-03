/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.autoscaling.Autoscaling;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.action.PolicyValidator;
import org.elasticsearch.xpack.autoscaling.capacity.nodeinfo.AutoscalingNodeInfo;
import org.elasticsearch.xpack.autoscaling.capacity.nodeinfo.AutoscalingNodesInfo;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.node.DiscoveryNode.DISCOVERY_NODE_COMPARATOR;

public class AutoscalingCalculateCapacityService implements PolicyValidator {
    private final Map<String, AutoscalingDeciderService> deciderByName;

    public AutoscalingCalculateCapacityService(Set<AutoscalingDeciderService> deciders) {
        assert deciders.size() >= 1; // always have fixed
        this.deciderByName = deciders.stream().collect(Collectors.toMap(AutoscalingDeciderService::name, Function.identity()));
    }

    public void validate(AutoscalingPolicy policy) {
        policy.deciders().forEach((name, configuration) -> validate(name, configuration, policy.roles()));
        SortedMap<String, Settings> deciders = addDefaultDeciders(policy);
        if (deciders.isEmpty()) {
            throw new IllegalArgumentException(
                "no default nor user configured deciders for policy [" + policy.name() + "] with roles [" + policy.roles() + "]"
            );
        }
    }

    private void validate(final String deciderName, final Settings configuration, SortedSet<String> roles) {
        AutoscalingDeciderService deciderService = deciderByName.get(deciderName);
        if (deciderService == null) {
            throw new IllegalArgumentException("unknown decider [" + deciderName + "]");
        }

        if (appliesToPolicy(deciderService, roles) == false) {
            throw new IllegalArgumentException("decider [" + deciderName + "] not applicable to policy with roles [ " + roles + "]");
        }

        Map<String, Setting<?>> deciderSettings = deciderService.deciderSettings()
            .stream()
            .collect(Collectors.toMap(Setting::getKey, Function.identity()));

        configuration.keySet().forEach(key -> validateSetting(key, configuration, deciderSettings, deciderName));
    }

    private static void validateSetting(String key, Settings configuration, Map<String, Setting<?>> deciderSettings, String decider) {
        Setting<?> setting = deciderSettings.get(key);
        if (setting == null) {
            throw new IllegalArgumentException("unknown setting [" + key + "] for decider [" + decider + "]");
        }

        // check the setting, notice that `get` throws when `configuration` contains an invalid value for `setting`
        setting.get(configuration);
    }

    public static class Holder {
        private final Autoscaling autoscaling;
        private final SetOnce<AutoscalingCalculateCapacityService> servicesSetOnce = new SetOnce<>();

        public Holder(Autoscaling autoscaling) {
            this.autoscaling = autoscaling;
        }

        public AutoscalingCalculateCapacityService get() {
            // defer constructing services until transport action creation time, so that other plugins
            // can create their deciders in their createComponents.
            AutoscalingCalculateCapacityService autoscalingCalculateCapacityService = servicesSetOnce.get();
            if (autoscalingCalculateCapacityService == null) {
                autoscalingCalculateCapacityService = new AutoscalingCalculateCapacityService(autoscaling.createDeciderServices());
                servicesSetOnce.set(autoscalingCalculateCapacityService);
            }
            return autoscalingCalculateCapacityService;
        }
    }

    public SortedMap<String, AutoscalingDeciderResults> calculate(
        ClusterState state,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        AutoscalingNodesInfo autoscalingNodesInfo,
        Runnable ensureNotCancelled
    ) {
        AutoscalingMetadata autoscalingMetadata = state.metadata().custom(AutoscalingMetadata.NAME);
        if (autoscalingMetadata != null) {
            return new TreeMap<>(
                autoscalingMetadata.policies()
                    .entrySet()
                    .stream()
                    .map(
                        e -> Tuple.tuple(
                            e.getKey(),
                            calculateForPolicy(
                                e.getValue().policy(),
                                state,
                                clusterInfo,
                                shardSizeInfo,
                                autoscalingNodesInfo,
                                ensureNotCancelled
                            )
                        )
                    )
                    .collect(Collectors.toMap(Tuple::v1, Tuple::v2))
            );
        } else {
            return new TreeMap<>();
        }
    }

    private AutoscalingDeciderResults calculateForPolicy(
        AutoscalingPolicy policy,
        ClusterState state,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        AutoscalingNodesInfo autoscalingNodesInfo,
        Runnable ensureNotCancelled
    ) {
        if (hasUnknownRoles(policy)) {
            return new AutoscalingDeciderResults(
                AutoscalingCapacity.ZERO,
                Collections.emptySortedSet(),
                new TreeMap<>(Map.of("_unknown_role", new AutoscalingDeciderResult(null, null)))
            );
        }
        SortedMap<String, Settings> deciders = addDefaultDeciders(policy);
        DefaultAutoscalingDeciderContext context = createContext(
            policy.roles(),
            state,
            clusterInfo,
            shardSizeInfo,
            autoscalingNodesInfo,
            ensureNotCancelled
        );
        SortedMap<String, AutoscalingDeciderResult> results = deciders.entrySet()
            .stream()
            .map(entry -> Tuple.tuple(entry.getKey(), calculateForDecider(entry.getKey(), entry.getValue(), context, ensureNotCancelled)))
            .collect(Collectors.toMap(Tuple::v1, Tuple::v2, (a, b) -> {
                throw new UnsupportedOperationException();
            }, TreeMap::new));
        return new AutoscalingDeciderResults(context.currentCapacity, context.currentNodes, results);
    }

    private SortedMap<String, Settings> addDefaultDeciders(AutoscalingPolicy policy) {
        SortedMap<String, Settings> deciders = new TreeMap<>(policy.deciders());
        deciderByName.entrySet()
            .stream()
            .filter(e -> defaultForPolicy(e.getValue(), policy.roles()))
            .forEach(e -> deciders.putIfAbsent(e.getKey(), Settings.EMPTY));
        return deciders;
    }

    private boolean defaultForPolicy(AutoscalingDeciderService deciderService, SortedSet<String> roles) {
        if (deciderService.defaultOn()) {
            return appliesToPolicy(deciderService, roles);
        } else {
            return false;
        }
    }

    private boolean appliesToPolicy(AutoscalingDeciderService deciderService, SortedSet<String> roles) {
        if (roles.isEmpty()) {
            return deciderService.appliesToEmptyRoles();
        } else {
            return deciderService.roles().stream().map(DiscoveryNodeRole::roleName).anyMatch(roles::contains);
        }
    }

    // visible for tests
    DefaultAutoscalingDeciderContext createContext(
        SortedSet<String> roles,
        ClusterState state,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo shardSizeInfo,
        AutoscalingNodesInfo autoscalingNodesInfo,
        Runnable ensureNotCancelled
    ) {
        return new DefaultAutoscalingDeciderContext(roles, state, clusterInfo, shardSizeInfo, autoscalingNodesInfo, ensureNotCancelled);
    }

    /**
     * Check if the policy has unknown roles. This can only happen in mixed clusters, where one master can accept a policy but if it fails
     * over to an older master before it is also upgraded, one of the roles might not be known.
     */
    private static boolean hasUnknownRoles(AutoscalingPolicy policy) {
        return DiscoveryNodeRole.roleNames().containsAll(policy.roles()) == false;
    }

    private AutoscalingDeciderResult calculateForDecider(
        String name,
        Settings configuration,
        AutoscalingDeciderContext context,
        Runnable ensureNotCancelled
    ) {
        assert deciderByName.containsKey(name);
        ensureNotCancelled.run();
        AutoscalingDeciderService service = deciderByName.get(name);
        return service.scale(configuration, context);
    }

    static class DefaultAutoscalingDeciderContext implements AutoscalingDeciderContext {

        private final SortedSet<DiscoveryNodeRole> roles;
        private final ClusterState state;
        private final ClusterInfo clusterInfo;
        private final SnapshotShardSizeInfo snapshotShardSizeInfo;
        private final AutoscalingNodesInfo autoscalingNodesInfo;
        private final SortedSet<DiscoveryNode> currentNodes;
        private final AutoscalingCapacity currentCapacity;
        private final boolean currentCapacityAccurate;
        private final Runnable ensureNotCancelled;

        DefaultAutoscalingDeciderContext(
            SortedSet<String> roles,
            ClusterState state,
            ClusterInfo clusterInfo,
            SnapshotShardSizeInfo snapshotShardSizeInfo,
            AutoscalingNodesInfo autoscalingNodesInfo,
            Runnable ensureNotCancelled
        ) {
            this.roles = roles.stream().map(DiscoveryNodeRole::getRoleFromRoleName).collect(Sets.toUnmodifiableSortedSet());
            Objects.requireNonNull(state);
            Objects.requireNonNull(clusterInfo);
            this.state = state;
            this.clusterInfo = clusterInfo;
            this.snapshotShardSizeInfo = snapshotShardSizeInfo;
            this.autoscalingNodesInfo = autoscalingNodesInfo;
            this.currentNodes = state.nodes()
                .stream()
                .filter(this::rolesFilter)
                .collect(Collectors.toCollection(() -> new TreeSet<>(DISCOVERY_NODE_COMPARATOR)));
            this.currentCapacity = calculateCurrentCapacity();
            this.currentCapacityAccurate = calculateCurrentCapacityAccurate();
            this.ensureNotCancelled = ensureNotCancelled;
        }

        @Override
        public ClusterState state() {
            return state;
        }

        @Override
        public AutoscalingCapacity currentCapacity() {
            if (currentCapacityAccurate) {
                assert currentCapacity.total().storage() != null;
                assert currentCapacity.node().storage() != null;
                return currentCapacity;
            } else {
                return null;
            }
        }

        @Override
        public Set<DiscoveryNode> nodes() {
            return currentNodes;
        }

        @Override
        public Set<DiscoveryNodeRole> roles() {
            return roles;
        }

        private boolean calculateCurrentCapacityAccurate() {
            return currentNodes.stream().allMatch(this::nodeHasAccurateCapacity);
        }

        private boolean nodeHasAccurateCapacity(DiscoveryNode node) {
            if (node.canContainData()) {
                // todo: multiple data path support.
                DiskUsage mostAvailable = clusterInfo.getNodeMostAvailableDiskUsages().get(node.getId());
                DiskUsage leastAvailable = clusterInfo.getNodeLeastAvailableDiskUsages().get(node.getId());
                if (mostAvailable == null
                    || mostAvailable.getPath().equals(leastAvailable.getPath()) == false
                    || totalStorage(clusterInfo.getNodeMostAvailableDiskUsages(), node) < 0) {
                    return false;
                }
            }

            return autoscalingNodesInfo.get(node).isPresent();
        }

        private AutoscalingCapacity calculateCurrentCapacity() {
            return currentNodes.stream()
                .map(this::resourcesFor)
                .map(c -> new AutoscalingCapacity(c, c))
                .reduce(
                    (c1, c2) -> new AutoscalingCapacity(
                        AutoscalingCapacity.AutoscalingResources.sum(c1.total(), c2.total()),
                        AutoscalingCapacity.AutoscalingResources.max(c1.node(), c2.node())
                    )
                )
                .orElse(AutoscalingCapacity.ZERO);
        }

        private AutoscalingCapacity.AutoscalingResources resourcesFor(DiscoveryNode node) {
            long storage = node.canContainData()
                ? Math.max(
                    totalStorage(clusterInfo.getNodeLeastAvailableDiskUsages(), node),
                    totalStorage(clusterInfo.getNodeMostAvailableDiskUsages(), node)
                )
                : 0L;

            Optional<AutoscalingNodeInfo> memoryAndProcessors = autoscalingNodesInfo.get(node);
            return new AutoscalingCapacity.AutoscalingResources(
                storage == -1 ? ByteSizeValue.ZERO : ByteSizeValue.ofBytes(storage),
                memoryAndProcessors.map(AutoscalingNodeInfo::memory).map(ByteSizeValue::ofBytes).orElse(ByteSizeValue.ZERO),
                memoryAndProcessors.map(AutoscalingNodeInfo::processors).orElse(null)
            );
        }

        private static long totalStorage(Map<String, DiskUsage> diskUsages, DiscoveryNode node) {
            DiskUsage diskUsage = diskUsages.get(node.getId());
            return diskUsage != null ? diskUsage.getTotalBytes() : -1;
        }

        private boolean rolesFilter(DiscoveryNode discoveryNode) {
            return discoveryNode.getRoles().equals(roles);
        }

        @Override
        public ClusterInfo info() {
            return clusterInfo;
        }

        @Override
        public SnapshotShardSizeInfo snapshotShardSizeInfo() {
            return snapshotShardSizeInfo;
        }

        public void ensureNotCancelled() {
            ensureNotCancelled.run();
        }
    }
}
