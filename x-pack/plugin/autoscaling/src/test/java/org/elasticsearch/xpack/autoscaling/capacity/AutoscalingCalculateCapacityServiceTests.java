/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.nodeinfo.AutoscalingNodeInfo;
import org.elasticsearch.xpack.autoscaling.capacity.nodeinfo.AutoscalingNodesInfo;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class AutoscalingCalculateCapacityServiceTests extends AutoscalingTestCase {
    public void testMultiplePoliciesFixedCapacity() {
        AutoscalingCalculateCapacityService service = new AutoscalingCalculateCapacityService(Set.of(new FixedAutoscalingDeciderService()));
        Set<String> policyNames = IntStream.range(0, randomIntBetween(1, 10))
            .mapToObj(i -> "test_ " + randomAlphaOfLength(10))
            .collect(Collectors.toSet());

        SortedMap<String, AutoscalingPolicyMetadata> policies = new TreeMap<>(
            policyNames.stream()
                .map(s -> Tuple.tuple(s, new AutoscalingPolicyMetadata(new AutoscalingPolicy(s, randomRoles(), randomFixedDeciders()))))
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2))
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().putCustom(AutoscalingMetadata.NAME, new AutoscalingMetadata(policies)))
            .build();
        SortedMap<String, AutoscalingDeciderResults> resultsMap = service.calculate(
            state,
            ClusterInfo.EMPTY,
            null,
            AutoscalingNodesInfo.EMPTY,
            () -> {}
        );
        assertThat(resultsMap.keySet(), equalTo(policyNames));
        for (Map.Entry<String, AutoscalingDeciderResults> entry : resultsMap.entrySet()) {
            AutoscalingDeciderResults results = entry.getValue();
            SortedMap<String, Settings> deciders = policies.get(entry.getKey()).policy().deciders();
            assertThat(deciders.size(), equalTo(1));
            Settings configuration = deciders.values().iterator().next();
            AutoscalingCapacity requiredCapacity = calculateFixedDeciderCapacity(configuration);
            assertThat(results.requiredCapacity(), equalTo(requiredCapacity));
            assertThat(results.results().size(), equalTo(1));
            AutoscalingDeciderResult deciderResult = results.results().get(deciders.firstKey());
            assertNotNull(deciderResult);
            assertThat(deciderResult.requiredCapacity(), equalTo(requiredCapacity));
            ByteSizeValue storage = configuration.getAsBytesSize(FixedAutoscalingDeciderService.STORAGE.getKey(), null);
            ByteSizeValue memory = configuration.getAsMemory(FixedAutoscalingDeciderService.MEMORY.getKey(), null);
            Float processors = configuration.getAsFloat(FixedAutoscalingDeciderService.PROCESSORS.getKey(), null);
            int nodes = FixedAutoscalingDeciderService.NODES.get(configuration);
            assertThat(deciderResult.reason(), equalTo(new FixedAutoscalingDeciderService.FixedReason(storage, memory, nodes, processors)));
            assertThat(
                deciderResult.reason().summary(),
                equalTo("fixed storage [" + storage + "] memory [" + memory + "] processors [" + processors + "] nodes [" + nodes + "]")
            );

            // there is no nodes in any tier.
            assertThat(results.currentCapacity(), equalTo(AutoscalingCapacity.ZERO));
            assertThat(results.currentNodes(), equalTo(Collections.emptySortedSet()));
        }
    }

    public void testDefaultDeciders() {
        FixedAutoscalingDeciderService defaultOn = new FixedAutoscalingDeciderService() {
            @Override
            public boolean defaultOn() {
                return true;
            }

            @Override
            public String name() {
                return "default_on";
            }
        };

        FixedAutoscalingDeciderService defaultOff = new FixedAutoscalingDeciderService();

        AutoscalingCalculateCapacityService service = new AutoscalingCalculateCapacityService(Set.of(defaultOn, defaultOff));
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .putCustom(
                        AutoscalingMetadata.NAME,
                        new AutoscalingMetadata(
                            new TreeMap<>(
                                Map.of("test", new AutoscalingPolicyMetadata(new AutoscalingPolicy("test", randomRoles(), new TreeMap<>())))
                            )
                        )
                    )
            )
            .build();

        assertThat(
            service.calculate(state, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY, AutoscalingNodesInfo.EMPTY, () -> {})
                .get("test")
                .results()
                .keySet(),
            equalTo(Set.of(defaultOn.name()))
        );
    }

    private SortedMap<String, Settings> randomFixedDeciders() {
        Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(FixedAutoscalingDeciderService.STORAGE.getKey(), randomByteSizeValue());
        }
        if (randomBoolean()) {
            settings.put(FixedAutoscalingDeciderService.MEMORY.getKey(), randomByteSizeValue());
        }
        if (randomBoolean()) {
            settings.put(FixedAutoscalingDeciderService.PROCESSORS.getKey(), randomInt(64));
        }
        settings.put(FixedAutoscalingDeciderService.NODES.getKey(), randomIntBetween(1, 10));
        return new TreeMap<>(Map.of(FixedAutoscalingDeciderService.NAME, settings.build()));
    }

    private AutoscalingCapacity calculateFixedDeciderCapacity(Settings configuration) {
        ByteSizeValue storage = configuration.getAsBytesSize(FixedAutoscalingDeciderService.STORAGE.getKey(), null);
        ByteSizeValue memory = configuration.getAsBytesSize(FixedAutoscalingDeciderService.MEMORY.getKey(), null);
        Float processors = configuration.getAsFloat(FixedAutoscalingDeciderService.PROCESSORS.getKey(), null);
        int nodes = FixedAutoscalingDeciderService.NODES.get(configuration);
        ByteSizeValue totalStorage = storage != null ? new ByteSizeValue(storage.getBytes() * nodes) : null;
        ByteSizeValue totalMemory = memory != null ? new ByteSizeValue(memory.getBytes() * nodes) : null;
        Float totalProcessors = processors != null ? processors * nodes : null;

        if (totalStorage == null && totalMemory == null && totalProcessors == null) {
            return null;
        } else {
            return new AutoscalingCapacity(
                new AutoscalingCapacity.AutoscalingResources(totalStorage, totalMemory, totalProcessors),
                new AutoscalingCapacity.AutoscalingResources(storage, memory, processors)
            );
        }
    }

    public void testContext() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        ClusterInfo info = ClusterInfo.EMPTY;
        SortedSet<String> roleNames = randomRoles();
        boolean hasDataRole = roleNames.stream().anyMatch(r -> r.equals("data") || r.startsWith("data_"));

        AutoscalingCalculateCapacityService service = new AutoscalingCalculateCapacityService(Set.of(new FixedAutoscalingDeciderService()));
        SnapshotShardSizeInfo snapshotShardSizeInfo = new SnapshotShardSizeInfo(Map.of());
        AutoscalingDeciderContext context = service.createContext(
            roleNames,
            state,
            info,
            snapshotShardSizeInfo,
            n -> Optional.of(new AutoscalingNodeInfo(randomNonNegativeLong(), randomInt(64))),
            () -> {}
        );

        assertSame(state, context.state());
        assertThat(context.nodes(), equalTo(Set.of()));
        assertThat(context.currentCapacity(), equalTo(AutoscalingCapacity.ZERO));
        assertThat(context.info(), sameInstance(info));
        assertThat(context.snapshotShardSizeInfo(), sameInstance(snapshotShardSizeInfo));

        Set<DiscoveryNodeRole> roles = roleNames.stream().map(DiscoveryNodeRole::getRoleFromRoleName).collect(Collectors.toSet());
        Set<DiscoveryNodeRole> otherRoles = mutateRoles(roleNames).stream()
            .map(DiscoveryNodeRole::getRoleFromRoleName)
            .collect(Collectors.toSet());
        final long memory = between(0, 1000);
        state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder().add(new DiscoveryNode("nodeId", buildNewFakeTransportAddress(), Map.of(), roles, Version.CURRENT))
            )
            .build();
        context = new AutoscalingCalculateCapacityService.DefaultAutoscalingDeciderContext(
            roleNames,
            state,
            info,
            null,
            n -> Optional.of(new AutoscalingNodeInfo(memory, randomInt(64))),
            () -> {}
        );

        assertThat(context.nodes().size(), equalTo(1));
        assertThat(context.nodes(), equalTo(new HashSet<>(state.nodes())));
        if (hasDataRole) {
            assertNull(context.currentCapacity());
        } else {
            assertThat(context.currentCapacity().node().memory(), equalTo(new ByteSizeValue(memory)));
            assertThat(context.currentCapacity().total().memory(), equalTo(new ByteSizeValue(memory)));
            assertThat(context.currentCapacity().node().storage(), equalTo(ByteSizeValue.ZERO));
            assertThat(context.currentCapacity().total().storage(), equalTo(ByteSizeValue.ZERO));
        }

        Map<String, DiskUsage> leastUsages = new HashMap<>();
        Map<String, DiskUsage> mostUsages = new HashMap<>();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        Set<DiscoveryNode> expectedNodes = new HashSet<>();
        long sumTotal = 0;
        long maxTotal = 0;
        for (int i = 0; i < randomIntBetween(1, 5); ++i) {
            String nodeId = "nodeId" + i;
            boolean useOtherRoles = randomBoolean();
            DiscoveryNode node = new DiscoveryNode(
                nodeId,
                buildNewFakeTransportAddress(),
                Map.of(),
                useOtherRoles ? otherRoles : roles,
                Version.CURRENT
            );
            nodes.add(node);

            if (useOtherRoles == false) {
                long total = randomLongBetween(1, 1L << 40);
                DiskUsage diskUsage = new DiskUsage(nodeId, null, randomAlphaOfLength(5), total, randomLongBetween(0, total));
                leastUsages.put(nodeId, diskUsage);
                if (randomBoolean()) {
                    diskUsage = new DiskUsage(nodeId, null, diskUsage.getPath(), total, diskUsage.getFreeBytes());
                }
                mostUsages.put(nodeId, diskUsage);
                sumTotal += total;
                maxTotal = Math.max(total, maxTotal);
                expectedNodes.add(node);
            } else {
                long total1 = randomLongBetween(0, 1L << 40);
                leastUsages.put(nodeId, new DiskUsage(nodeId, null, randomAlphaOfLength(5), total1, randomLongBetween(0, total1)));
                long total2 = randomLongBetween(0, 1L << 40);
                mostUsages.put(nodeId, new DiskUsage(nodeId, null, randomAlphaOfLength(5), total2, randomLongBetween(0, total2)));
            }
        }
        state = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
        info = new ClusterInfo(leastUsages, mostUsages, Map.of(), Map.of(), Map.of(), Map.of());
        context = new AutoscalingCalculateCapacityService.DefaultAutoscalingDeciderContext(
            roleNames,
            state,
            info,
            null,
            n -> Optional.of(new AutoscalingNodeInfo(memory, randomInt(64))),
            () -> {}
        );

        assertThat(context.nodes(), equalTo(expectedNodes));
        if (hasDataRole) {
            assertThat(context.currentCapacity().node().storage(), equalTo(new ByteSizeValue(maxTotal)));
            assertThat(context.currentCapacity().total().storage(), equalTo(new ByteSizeValue(sumTotal)));
        } else {
            assertThat(context.currentCapacity().node().storage(), equalTo(ByteSizeValue.ZERO));
            assertThat(context.currentCapacity().total().storage(), equalTo(ByteSizeValue.ZERO));
        }
        assertThat(context.currentCapacity().node().memory(), equalTo(new ByteSizeValue(memory * Integer.signum(expectedNodes.size()))));
        assertThat(context.currentCapacity().total().memory(), equalTo(new ByteSizeValue(memory * expectedNodes.size())));

        if (expectedNodes.isEmpty() == false) {
            context = new AutoscalingCalculateCapacityService.DefaultAutoscalingDeciderContext(
                roleNames,
                state,
                info,
                null,
                AutoscalingNodesInfo.EMPTY,
                () -> {}
            );
            assertThat(context.nodes(), equalTo(expectedNodes));
            assertThat(context.currentCapacity(), is(nullValue()));

            String multiPathNodeId = randomFrom(expectedNodes).getId();
            DiskUsage original = mostUsages.get(multiPathNodeId);
            mostUsages.put(
                multiPathNodeId,
                new DiskUsage(
                    multiPathNodeId,
                    null,
                    randomValueOtherThan(original.getPath(), () -> randomAlphaOfLength(5)),
                    original.getTotalBytes(),
                    original.getFreeBytes()
                )
            );

            info = new ClusterInfo(leastUsages, mostUsages, Map.of(), Map.of(), Map.of(), Map.of());
            context = new AutoscalingCalculateCapacityService.DefaultAutoscalingDeciderContext(
                roleNames,
                state,
                info,
                null,
                n -> Optional.of(new AutoscalingNodeInfo(memory, randomInt(64))),
                () -> {}
            );
            assertThat(context.nodes(), equalTo(expectedNodes));
            if (hasDataRole) {
                assertThat(context.currentCapacity(), is(nullValue()));
            } else {
                assertThat(context.currentCapacity().node().memory(), equalTo(new ByteSizeValue(memory)));
                assertThat(context.currentCapacity().total().memory(), equalTo(new ByteSizeValue(memory * expectedNodes.size())));
                assertThat(context.currentCapacity().node().storage(), equalTo(ByteSizeValue.ZERO));
                assertThat(context.currentCapacity().total().storage(), equalTo(ByteSizeValue.ZERO));
            }
        }
    }

    public void testValidateDeciderName() {
        AutoscalingCalculateCapacityService service = new AutoscalingCalculateCapacityService(Set.of(new FixedAutoscalingDeciderService()));
        String badDeciderName = randomValueOtherThan(FixedAutoscalingDeciderService.NAME, () -> randomAlphaOfLength(8));
        AutoscalingPolicy policy = new AutoscalingPolicy(
            randomAlphaOfLength(8),
            randomRoles(),
            new TreeMap<>(Map.of(badDeciderName, Settings.EMPTY))
        );
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> service.validate(policy));
        assertThat(exception.getMessage(), equalTo("unknown decider [" + badDeciderName + "]"));
    }

    public void testValidateDeciderRoles() {
        Set<String> roles = randomRoles();
        AutoscalingCalculateCapacityService service = new AutoscalingCalculateCapacityService(Set.of(new FixedAutoscalingDeciderService() {

            @Override
            public List<DiscoveryNodeRole> roles() {
                return roles.stream().map(DiscoveryNodeRole::getRoleFromRoleName).collect(Collectors.toList());
            }

            @Override
            public boolean appliesToEmptyRoles() {
                return false;
            }

        }));
        SortedSet<String> badRoles = new TreeSet<>(randomRoles());
        badRoles.removeAll(roles);
        AutoscalingPolicy policy = new AutoscalingPolicy(
            FixedAutoscalingDeciderService.NAME,
            badRoles,
            new TreeMap<>(Map.of(FixedAutoscalingDeciderService.NAME, Settings.EMPTY))
        );
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> service.validate(policy));
        assertThat(
            exception.getMessage(),
            equalTo("decider [" + FixedAutoscalingDeciderService.NAME + "] not applicable to policy with roles [ " + badRoles + "]")
        );
    }

    public void testValidateNotEmptyDeciders() {
        AutoscalingCalculateCapacityService service = new AutoscalingCalculateCapacityService(Set.of(new FixedAutoscalingDeciderService()));
        String policyName = randomAlphaOfLength(8);
        AutoscalingPolicy policy = new AutoscalingPolicy(
            policyName,
            new TreeSet<>(randomBoolean() ? Set.of() : Set.of("master")),
            new TreeMap<>()
        );
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> service.validate(policy));
        assertThat(
            exception.getMessage(),
            equalTo("no default nor user configured deciders for policy [" + policyName + "] with roles [" + policy.roles() + "]")
        );
    }

    public void testValidateSettingName() {
        AutoscalingCalculateCapacityService service = new AutoscalingCalculateCapacityService(Set.of(new FixedAutoscalingDeciderService()));
        Set<String> legalNames = Set.of(
            FixedAutoscalingDeciderService.STORAGE.getKey(),
            FixedAutoscalingDeciderService.MEMORY.getKey(),
            FixedAutoscalingDeciderService.NODES.getKey()
        );
        String badSettingName = randomValueOtherThanMany(legalNames::contains, () -> randomAlphaOfLength(8));
        AutoscalingPolicy policy = new AutoscalingPolicy(
            randomAlphaOfLength(8),
            randomRoles(),
            new TreeMap<>(
                Map.of(FixedAutoscalingDeciderService.NAME, Settings.builder().put(badSettingName, randomAlphaOfLength(1)).build())
            )
        );
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> service.validate(policy));
        assertThat(
            exception.getMessage(),
            equalTo("unknown setting [" + badSettingName + "] for decider [" + FixedAutoscalingDeciderService.NAME + "]")
        );
    }

    public void testValidateSettingValue() {
        AutoscalingCalculateCapacityService service = new AutoscalingCalculateCapacityService(Set.of(new FixedAutoscalingDeciderService()));
        String value = randomValueOtherThanMany(s -> Character.isDigit(s.charAt(0)), () -> randomAlphaOfLength(5));
        AutoscalingPolicy policy = new AutoscalingPolicy(
            randomAlphaOfLength(8),
            randomRoles(),
            new TreeMap<>(
                Map.of(
                    FixedAutoscalingDeciderService.NAME,
                    Settings.builder().put(FixedAutoscalingDeciderService.STORAGE.getKey(), value).build()
                )
            )
        );
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> service.validate(policy));
        assertThat(exception.getMessage(), containsString("[" + value + "]"));
        assertThat(exception.getMessage(), containsString("[" + FixedAutoscalingDeciderService.STORAGE.getKey() + "]"));
    }
}
