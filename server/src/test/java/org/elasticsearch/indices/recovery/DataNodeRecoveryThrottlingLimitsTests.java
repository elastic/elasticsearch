/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingLimits.IncomingRecoveryLimits;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingLimits.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingLimits.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingLimits.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_PER_HEAP_GB_SETTING;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingLimits.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingLimits.INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingLimits.appliesIncomingHeapBasedLimit;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingLimits.computeEffectiveMaxConcurrentRecoveries;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingLimits.computeIncomingRecoveryLimits;
import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.elasticsearch.test.NodeRoles.indexOnlyNode;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class DataNodeRecoveryThrottlingLimitsTests extends ESTestCase {

    public void testHeapBasedLimitRejectsZero() {
        expectThrows(
            IllegalArgumentException.class,
            () -> INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING.get(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING.getKey(), 0.0).build()
            )
        );
    }

    public void testNonCriticalRecoveriesMaxProportionRejectsZero() {
        for (final var zeroValue : List.of("0", "0%")) {
            expectThrows(
                IllegalArgumentException.class,
                () -> INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING.get(
                    Settings.builder()
                        .put(INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING.getKey(), zeroValue)
                        .build()
                )
            );
        }
    }

    public void testEffectiveMaxConcurrentRecoveries() {
        final int maxConcurrent = randomIntBetween(1, 100);
        // Unknown heap size, only the static limit applies.
        assertThat(
            computeEffectiveMaxConcurrentRecoveries(maxConcurrent, randomDoubleBetween(0.1, 10, true), ByteSizeValue.ZERO, true),
            equalTo(maxConcurrent)
        );

        assertThat(computeEffectiveMaxConcurrentRecoveries(100, 1.25, ByteSizeValue.ofGb(4), true), equalTo(5));
        assertThat(computeEffectiveMaxConcurrentRecoveries(3, 1.25, ByteSizeValue.ofGb(4), true), equalTo(3));

        // The heap-based limit rounds up, so the smallest allowed ratio still allows one recovery.
        assertThat(computeEffectiveMaxConcurrentRecoveries(100, Double.MIN_NORMAL, ByteSizeValue.ofGb(4), true), equalTo(1));
        assertThat(
            computeEffectiveMaxConcurrentRecoveries(maxConcurrent, Double.MAX_VALUE, ByteSizeValue.ofGb(randomIntBetween(1, 64)), true),
            equalTo(maxConcurrent)
        );
    }

    public void testEffectiveMaxConcurrentRecoveriesWithoutHeapBasedLimit() {
        final int maxConcurrent = randomIntBetween(1, 100);
        // Only the static limit applies, whatever the heap and ratio.
        assertThat(
            computeEffectiveMaxConcurrentRecoveries(
                maxConcurrent,
                randomFrom(Double.MIN_NORMAL, randomDoubleBetween(0.1, 10, true)),
                randomFrom(ByteSizeValue.ZERO, ByteSizeValue.ofGb(randomIntBetween(1, 64))),
                false
            ),
            equalTo(maxConcurrent)
        );
    }

    public void testIncomingRecoveryLimits() {
        assertThat(
            computeIncomingRecoveryLimits(100, 1.25, RatioValue.ofPercent(50), ByteSizeValue.ofGb(4), true),
            equalTo(new IncomingRecoveryLimits(5, 3))
        );
        // Without the heap-based limit, only the static limit applies, whatever the heap and ratio.
        assertThat(
            computeIncomingRecoveryLimits(
                32,
                randomFrom(Double.MIN_NORMAL, randomDoubleBetween(0.1, 10, true)),
                RatioValue.ofPercent(50),
                randomFrom(ByteSizeValue.ZERO, ByteSizeValue.ofGb(randomIntBetween(1, 64))),
                false
            ),
            equalTo(new IncomingRecoveryLimits(32, 16))
        );
        assertThat(
            computeIncomingRecoveryLimits(10, Double.MAX_VALUE, RatioValue.ofPercent(Double.MIN_NORMAL), ByteSizeValue.ZERO, true),
            equalTo(new IncomingRecoveryLimits(10, 1))
        );
        // Default settings are unbounded, whether the heap-based limit applies or not, and whether the heap size is known or not.
        for (final var maxHeap : List.of(ByteSizeValue.ZERO, ByteSizeValue.ofGb(randomIntBetween(1, 64)))) {
            for (final boolean applyHeapBasedLimit : new boolean[] { true, false }) {
                assertThat(
                    computeIncomingRecoveryLimits(
                        INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getDefault(Settings.EMPTY),
                        INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING.getDefault(Settings.EMPTY),
                        INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING.getDefault(Settings.EMPTY),
                        maxHeap,
                        applyHeapBasedLimit
                    ),
                    equalTo(IncomingRecoveryLimits.UNBOUNDED)
                );
            }
        }
    }

    public void testAppliesIncomingHeapBasedLimit() {
        assertTrue(appliesIncomingHeapBasedLimit(Settings.EMPTY));
        assertTrue(appliesIncomingHeapBasedLimit(indexOnlyNode(Settings.EMPTY)));
        assertFalse(appliesIncomingHeapBasedLimit(onlyRole(DiscoveryNodeRole.SEARCH_ROLE)));
        assertFalse(appliesIncomingHeapBasedLimit(addRoles(Set.of(DiscoveryNodeRole.SEARCH_ROLE))));
    }

    public void testWatchIncomingRecoveryLimits() {
        final var registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registeredSettings.addAll(DataNodeRecoveryThrottlingLimits.settings());
        final var clusterSettings = new ClusterSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 4).build(),
            registeredSettings
        );
        final List<IncomingRecoveryLimits> observed = new ArrayList<>();
        new DataNodeRecoveryThrottlingLimits(clusterSettings, Settings.EMPTY, ByteSizeValue.ZERO).addIncomingRecoveryLimitsConsumer(
            observed::add
        );
        assertThat(observed, contains(new IncomingRecoveryLimits(4, 4)));

        // Both settings change in a single update, which is applied atomically.
        clusterSettings.applySettings(
            Settings.builder()
                .put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 6)
                .put(INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING.getKey(), "50%")
                .build()
        );
        assertThat(observed, contains(new IncomingRecoveryLimits(4, 4), new IncomingRecoveryLimits(6, 3)));
    }

    public void testWatchIncomingRecoveryLimitsWhenSettingsAreNotRegistered() {
        final var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final List<IncomingRecoveryLimits> observed = new ArrayList<>();
        new DataNodeRecoveryThrottlingLimits(
            clusterSettings,
            randomFrom(Settings.EMPTY, indexOnlyNode(Settings.EMPTY), onlyRole(DiscoveryNodeRole.SEARCH_ROLE)),
            randomFrom(ByteSizeValue.ZERO, ByteSizeValue.ofGb(randomIntBetween(1, 64)))
        ).addIncomingRecoveryLimitsConsumer(observed::add);
        assertThat(observed, contains(IncomingRecoveryLimits.UNBOUNDED));
    }

    public void testWatchIncomingRecoveryAppliesHeapBasedLimitBasedOnNodeRole() {
        final var registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registeredSettings.addAll(DataNodeRecoveryThrottlingLimits.settings());
        final var throttleSettings = Settings.builder()
            .put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 32)
            .put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING.getKey(), 1.5)
            .put(INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING.getKey(), "50%")
            .build();

        final List<IncomingRecoveryLimits> indexNodeObserved = new ArrayList<>();
        new DataNodeRecoveryThrottlingLimits(
            new ClusterSettings(throttleSettings, registeredSettings),
            indexOnlyNode(Settings.EMPTY),
            ByteSizeValue.ofGb(4)
        ).addIncomingRecoveryLimitsConsumer(indexNodeObserved::add);
        assertThat(indexNodeObserved, contains(new IncomingRecoveryLimits(6, 3)));

        // The heap-based limit does not apply to search nodes, whatever the heap and ratio.
        final var clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(throttleSettings)
                .put(
                    INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING.getKey(),
                    randomFrom(Double.MIN_NORMAL, randomDoubleBetween(0.1, 10, true))
                )
                .build(),
            registeredSettings
        );
        final List<IncomingRecoveryLimits> searchNodeObserved = new ArrayList<>();
        new DataNodeRecoveryThrottlingLimits(
            clusterSettings,
            onlyRole(DiscoveryNodeRole.SEARCH_ROLE),
            randomFrom(ByteSizeValue.ZERO, ByteSizeValue.ofGb(randomIntBetween(1, 64)))
        ).addIncomingRecoveryLimitsConsumer(searchNodeObserved::add);
        assertThat(searchNodeObserved, contains(new IncomingRecoveryLimits(32, 16)));
        clusterSettings.applySettings(
            Settings.builder()
                .put(throttleSettings)
                .put(
                    INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING.getKey(),
                    randomFrom(Double.MIN_NORMAL, randomDoubleBetween(0.1, 10, true))
                )
                .put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 20)
                .build()
        );
        assertThat(searchNodeObserved, contains(new IncomingRecoveryLimits(32, 16), new IncomingRecoveryLimits(20, 10)));
    }

    public void testWatchOutgoingRecoveryLimit() {
        final var registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registeredSettings.addAll(DataNodeRecoveryThrottlingLimits.settings());
        final var clusterSettings = new ClusterSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4).build(),
            registeredSettings
        );
        final List<Integer> observed = new ArrayList<>();
        new DataNodeRecoveryThrottlingLimits(clusterSettings, Settings.EMPTY, ByteSizeValue.ofGb(2)).addOutgoingRecoveryLimitConsumer(
            observed::add
        );
        assertThat(observed, contains(4));
        clusterSettings.applySettings(
            Settings.builder()
                .put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 100)
                .put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_PER_HEAP_GB_SETTING.getKey(), 1.5)
                .build()
        );
        assertThat(observed, contains(4, 3));

        // Incoming settings do not affect the outgoing limit.
        clusterSettings.applySettings(
            Settings.builder()
                .put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 100)
                .put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_PER_HEAP_GB_SETTING.getKey(), 1.5)
                .put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
                .build()
        );
        assertThat(observed, contains(4, 3));
    }

    public void testWatchOutgoingRecoveryLimitWhenSettingsAreNotRegistered() {
        final var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final List<Integer> observed = new ArrayList<>();
        new DataNodeRecoveryThrottlingLimits(clusterSettings, Settings.EMPTY, ByteSizeValue.ofGb(4)).addOutgoingRecoveryLimitConsumer(
            observed::add
        );
        assertThat(observed, contains(Integer.MAX_VALUE));
    }

    public void testGettersReflectSettingsUpdates() {
        final var clusterSettings = clusterSettingsWithDataNodeSettingsRegistered(
            Settings.builder()
                .put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 4)
                .put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 3)
                .build()
        );
        final var limits = new DataNodeRecoveryThrottlingLimits(clusterSettings, Settings.EMPTY, ByteSizeValue.ofGb(2));
        assertThat(limits.incomingRecoveryLimits(), equalTo(new IncomingRecoveryLimits(4, 4)));
        assertThat(limits.outgoingRecoveryLimit(), equalTo(3));

        clusterSettings.applySettings(
            Settings.builder()
                .put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 6)
                .put(INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING.getKey(), "50%")
                .put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 5)
                .build()
        );
        assertThat(limits.incomingRecoveryLimits(), equalTo(new IncomingRecoveryLimits(6, 3)));
        assertThat(limits.outgoingRecoveryLimit(), equalTo(5));
    }

    /// Every listener registered with the watch methods is notified, and sees the getters already updated.
    public void testWatchersAreNotifiedAfterGettersAreUpdated() {
        final var clusterSettings = clusterSettingsWithDataNodeSettingsRegistered(Settings.EMPTY);
        final var limits = new DataNodeRecoveryThrottlingLimits(clusterSettings, Settings.EMPTY, ByteSizeValue.ZERO);
        final List<IncomingRecoveryLimits> incomingObserved = new ArrayList<>();
        final List<Integer> outgoingObserved = new ArrayList<>();
        final int watcherCount = between(1, 3);
        for (int i = 0; i < watcherCount; i++) {
            limits.addIncomingRecoveryLimitsConsumer(newLimits -> {
                assertThat(limits.incomingRecoveryLimits(), equalTo(newLimits));
                incomingObserved.add(newLimits);
            });
            limits.addOutgoingRecoveryLimitConsumer(newLimit -> {
                assertThat(limits.outgoingRecoveryLimit(), equalTo(newLimit));
                outgoingObserved.add(newLimit);
            });
        }
        assertThat(incomingObserved, equalTo(Collections.nCopies(watcherCount, IncomingRecoveryLimits.UNBOUNDED)));
        assertThat(outgoingObserved, equalTo(Collections.nCopies(watcherCount, Integer.MAX_VALUE)));

        incomingObserved.clear();
        outgoingObserved.clear();
        clusterSettings.applySettings(
            Settings.builder()
                .put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 2)
                .put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 7)
                .build()
        );
        assertThat(incomingObserved, equalTo(Collections.nCopies(watcherCount, new IncomingRecoveryLimits(2, 2))));
        assertThat(outgoingObserved, equalTo(Collections.nCopies(watcherCount, 7)));
    }

    public void testDataNodeRecoveryThrottlingEnabled() {
        final var clusterSettings = clusterSettingsWithDataNodeSettingsRegistered(Settings.EMPTY);
        final var limits = new DataNodeRecoveryThrottlingLimits(clusterSettings, Settings.EMPTY, ByteSizeValue.ZERO);
        assertFalse(limits.isDataNodeRecoveryThrottlingEnabled());

        final var dataNodeThrottledSettings = Settings.builder()
            .put(masterUnthrottledSettings())
            .put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 4)
            .put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4)
            .build();
        clusterSettings.applySettings(dataNodeThrottledSettings);
        assertTrue(limits.isDataNodeRecoveryThrottlingEnabled());

        // The data node does not bound its incoming recoveries.
        clusterSettings.applySettings(
            Settings.builder()
                .put(dataNodeThrottledSettings)
                .remove(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey())
                .build()
        );
        assertFalse(limits.isDataNodeRecoveryThrottlingEnabled());

        clusterSettings.applySettings(dataNodeThrottledSettings);
        assertTrue(limits.isDataNodeRecoveryThrottlingEnabled());

        // The data node does not bound its outgoing recoveries.
        clusterSettings.applySettings(
            Settings.builder()
                .put(dataNodeThrottledSettings)
                .remove(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey())
                .build()
        );
        assertFalse(limits.isDataNodeRecoveryThrottlingEnabled());

        clusterSettings.applySettings(dataNodeThrottledSettings);
        assertTrue(limits.isDataNodeRecoveryThrottlingEnabled());

        // The master throttles again through any one of its settings.
        final String throttlingMasterSetting = randomFrom(masterUnthrottledSettings().keySet());
        clusterSettings.applySettings(
            Settings.builder().put(dataNodeThrottledSettings).put(throttlingMasterSetting, between(0, 100)).build()
        );
        assertFalse(limits.isDataNodeRecoveryThrottlingEnabled());
    }

    public void testDataNodeRecoveryThrottlingDisabledWithDefaultMasterSettings() {
        final var clusterSettings = clusterSettingsWithDataNodeSettingsRegistered(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 4).build()
        );
        assertFalse(
            new DataNodeRecoveryThrottlingLimits(clusterSettings, Settings.EMPTY, ByteSizeValue.ZERO).isDataNodeRecoveryThrottlingEnabled()
        );
    }

    public void testDataNodeRecoveryThrottlingDisabledWhenDataNodeSettingsAreNotRegistered() {
        final var clusterSettings = new ClusterSettings(masterUnthrottledSettings(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var limits = new DataNodeRecoveryThrottlingLimits(clusterSettings, Settings.EMPTY, ByteSizeValue.ofGb(4));
        assertThat(limits.incomingRecoveryLimits(), equalTo(IncomingRecoveryLimits.UNBOUNDED));
        assertThat(limits.outgoingRecoveryLimit(), equalTo(Integer.MAX_VALUE));
        assertFalse(limits.isDataNodeRecoveryThrottlingEnabled());
    }

    private static ClusterSettings clusterSettingsWithDataNodeSettingsRegistered(Settings settings) {
        final var registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registeredSettings.addAll(DataNodeRecoveryThrottlingLimits.settings());
        return new ClusterSettings(settings, registeredSettings);
    }

    /// The master settings with which the master leaves recovery throttling to the data nodes.
    private static Settings masterUnthrottledSettings() {
        return Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE)
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), -1)
            .build();
    }
}
