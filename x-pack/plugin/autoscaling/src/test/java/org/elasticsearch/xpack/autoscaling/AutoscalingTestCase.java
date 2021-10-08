/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResults;
import org.elasticsearch.xpack.autoscaling.capacity.FixedAutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AutoscalingTestCase extends ESTestCase {

    public static AutoscalingDeciderResult randomAutoscalingDeciderResult() {
        AutoscalingCapacity capacity = randomNullableAutoscalingCapacity();
        return randomAutoscalingDeciderResultWithCapacity(capacity);
    }

    protected static AutoscalingDeciderResult randomAutoscalingDeciderResultWithCapacity(AutoscalingCapacity capacity) {
        return new AutoscalingDeciderResult(
            capacity,
            new FixedAutoscalingDeciderService.FixedReason(randomNullableByteSizeValue(), randomNullableByteSizeValue(), randomInt(1000))
        );
    }

    public static AutoscalingDeciderResults randomAutoscalingDeciderResults() {
        final SortedMap<String, AutoscalingDeciderResult> results = IntStream.range(0, randomIntBetween(1, 10))
            .mapToObj(i -> Tuple.tuple(Integer.toString(i), randomAutoscalingDeciderResult()))
            .collect(Collectors.toMap(Tuple::v1, Tuple::v2, (a, b) -> { throw new IllegalStateException(); }, TreeMap::new));
        AutoscalingCapacity capacity = new AutoscalingCapacity(randomAutoscalingResources(), randomAutoscalingResources());
        return new AutoscalingDeciderResults(capacity, randomNodes(), results);
    }

    public static AutoscalingCapacity randomAutoscalingCapacity() {
        AutoscalingCapacity.AutoscalingResources total = randomNullValueAutoscalingResources();
        return new AutoscalingCapacity(
            total,
            randomBoolean() ? randomNullValueAutoscalingResources(total.storage() != null, total.memory() != null) : null
        );
    }

    protected static AutoscalingCapacity randomNullableAutoscalingCapacity() {
        return randomBoolean() ? randomAutoscalingCapacity() : null;
    }

    protected static AutoscalingCapacity.AutoscalingResources randomAutoscalingResources() {
        return new AutoscalingCapacity.AutoscalingResources(randomByteSizeValue(), randomByteSizeValue());
    }

    private static AutoscalingCapacity.AutoscalingResources randomNullValueAutoscalingResources() {
        return randomNullValueAutoscalingResources(true, true);
    }

    public static AutoscalingCapacity.AutoscalingResources randomNullValueAutoscalingResources(boolean allowStorage, boolean allowMemory) {
        assert allowMemory || allowStorage;
        boolean addStorage = (allowStorage && randomBoolean()) || allowMemory == false;
        boolean addMemory = (allowMemory && randomBoolean()) || addStorage == false;
        return new AutoscalingCapacity.AutoscalingResources(
            addStorage ? randomByteSizeValue() : null,
            addMemory ? randomByteSizeValue() : null
        );
    }

    public static SortedSet<DiscoveryNode> randomNodes() {
        String prefix = randomAlphaOfLength(5);
        return IntStream.range(0, randomIntBetween(1, 10))
            .mapToObj(
                i -> new DiscoveryNode(
                    prefix + i,
                    buildNewFakeTransportAddress(),
                    Map.of(),
                    randomRoles().stream().map(DiscoveryNodeRole::getRoleFromRoleName).collect(Collectors.toSet()),
                    Version.CURRENT
                )
            )
            .collect(Collectors.toCollection(() -> new TreeSet<>(AutoscalingDeciderResults.DISCOVERY_NODE_COMPARATOR)));
    }

    public static ByteSizeValue randomByteSizeValue() {
        // do not want to test any overflow.
        return new ByteSizeValue(randomLongBetween(0, Long.MAX_VALUE >> 16));
    }

    public static ByteSizeValue randomNullableByteSizeValue() {
        return randomBoolean() ? randomByteSizeValue() : null;
    }

    public static SortedMap<String, Settings> randomAutoscalingDeciders() {
        return new TreeMap<>(
            List.of(randomFixedDecider()).stream().collect(Collectors.toMap(d -> FixedAutoscalingDeciderService.NAME, Function.identity()))
        );
    }

    public static Settings randomFixedDecider() {
        Settings.Builder configurationBuilder = Settings.builder();
        if (randomBoolean()) {
            configurationBuilder.put(FixedAutoscalingDeciderService.STORAGE.getKey(), randomByteSizeValue());
        }
        if (randomBoolean()) {
            configurationBuilder.put(FixedAutoscalingDeciderService.MEMORY.getKey(), randomByteSizeValue());
        }
        if (randomBoolean()) {
            configurationBuilder.put(FixedAutoscalingDeciderService.NODES.getKey(), randomIntBetween(1, 10));
        }

        return configurationBuilder.build();
    }

    public static AutoscalingPolicy randomAutoscalingPolicy() {
        return randomAutoscalingPolicyOfName(randomAlphaOfLength(8));
    }

    public static AutoscalingPolicy randomAutoscalingPolicyOfName(final String name) {
        return new AutoscalingPolicy(name, randomRoles(), randomAutoscalingDeciders());
    }

    public static AutoscalingPolicy mutateAutoscalingPolicy(final AutoscalingPolicy instance) {
        String name = instance.name();
        SortedSet<String> roles = instance.roles();
        SortedMap<String, Settings> deciders = instance.deciders();
        BitSet choice = BitSet.valueOf(new long[] { randomIntBetween(1, 7) });
        if (choice.get(0)) {
            name = randomValueOtherThan(instance.name(), () -> randomAlphaOfLength(8));
        }
        if (choice.get(1)) {
            roles = mutateRoles(roles);
        }
        if (choice.get(2)) {
            deciders = mutateAutoscalingDeciders(deciders);
        }
        return new AutoscalingPolicy(name, roles, deciders);
    }

    protected static SortedSet<String> mutateRoles(SortedSet<String> roles) {
        return randomValueOtherThan(roles, AutoscalingTestCase::randomRoles);
    }

    public static SortedMap<String, Settings> mutateAutoscalingDeciders(final SortedMap<String, Settings> deciders) {
        if (deciders.size() == 0) {
            return randomAutoscalingDeciders();
        } else {
            // use a proper subset of the deciders
            return new TreeMap<>(
                randomSubsetOf(randomIntBetween(0, deciders.size() - 1), deciders.entrySet()).stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }
    }

    public static AutoscalingMetadata randomAutoscalingMetadata() {
        return randomAutoscalingMetadataOfPolicyCount(randomIntBetween(0, 8));
    }

    public static AutoscalingMetadata randomAutoscalingMetadataOfPolicyCount(final int numberOfPolicies) {
        final SortedMap<String, AutoscalingPolicyMetadata> policies = new TreeMap<>();
        for (int i = 0; i < numberOfPolicies; i++) {
            final AutoscalingPolicy policy = randomAutoscalingPolicy();
            final AutoscalingPolicyMetadata policyMetadata = new AutoscalingPolicyMetadata(policy);
            policies.put(policy.name(), policyMetadata);
        }
        return new AutoscalingMetadata(policies);
    }

    public static SortedSet<String> randomRoles() {
        return randomSubsetOf(DiscoveryNodeRole.roleNames()).stream().collect(Sets.toUnmodifiableSortedSet());
    }

    public static NamedWriteableRegistry getAutoscalingNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new Autoscaling().getNamedWriteables());
    }

    public static NamedXContentRegistry getAutoscalingXContentRegistry() {
        return new NamedXContentRegistry(new Autoscaling().getNamedXContent());
    }
}
