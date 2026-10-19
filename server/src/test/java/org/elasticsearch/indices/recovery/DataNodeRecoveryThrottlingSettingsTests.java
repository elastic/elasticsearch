/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings.IncomingRecoveryLimits;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_INCOMING_RECOVERIES_MAX_RELOCATION_PROPORTION_SETTING;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_PER_HEAP_GB_SETTING;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings.effectiveMaxConcurrentRecoveries;
import static org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings.incomingRecoveryLimits;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class DataNodeRecoveryThrottlingSettingsTests extends ESTestCase {

    public void testEffectiveMaxConcurrentRecoveries() {
        final int maxConcurrent = randomIntBetween(1, 100);
        // Unknown heap size, only the static limit applies.
        assertThat(
            effectiveMaxConcurrentRecoveries(maxConcurrent, randomDoubleBetween(0.1, 10, true), ByteSizeValue.ZERO),
            equalTo(maxConcurrent)
        );

        assertThat(effectiveMaxConcurrentRecoveries(100, 1.25, ByteSizeValue.ofGb(4)), equalTo(5));
        assertThat(effectiveMaxConcurrentRecoveries(3, 1.25, ByteSizeValue.ofGb(4)), equalTo(3));

        // The heap-based limit rounds up, so the smallest allowed ratio still allows one recovery.
        assertThat(effectiveMaxConcurrentRecoveries(100, Double.MIN_NORMAL, ByteSizeValue.ofGb(4)), equalTo(1));
        assertThat(
            effectiveMaxConcurrentRecoveries(maxConcurrent, Double.MAX_VALUE, ByteSizeValue.ofGb(randomIntBetween(1, 64))),
            equalTo(maxConcurrent)
        );
    }

    public void testIncomingRecoveryLimits() {
        assertThat(
            incomingRecoveryLimits(100, 1.25, RatioValue.ofPercent(50), ByteSizeValue.ofGb(4)),
            equalTo(new IncomingRecoveryLimits(5, 3))
        );
        assertThat(
            incomingRecoveryLimits(10, Double.MAX_VALUE, RatioValue.ofPercent(Double.MIN_NORMAL), ByteSizeValue.ZERO),
            equalTo(new IncomingRecoveryLimits(10, 1))
        );
        // Default settings are unbounded.
        assertThat(
            incomingRecoveryLimits(
                INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getDefault(Settings.EMPTY),
                INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING.getDefault(Settings.EMPTY),
                INDICES_RECOVERY_INCOMING_RECOVERIES_MAX_RELOCATION_PROPORTION_SETTING.getDefault(Settings.EMPTY),
                randomBoolean() ? ByteSizeValue.ZERO : ByteSizeValue.ofGb(randomIntBetween(1, 64))
            ),
            equalTo(IncomingRecoveryLimits.UNBOUNDED)
        );
    }

    public void testWatchIncomingRecoveryLimits() {
        final var registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registeredSettings.addAll(DataNodeRecoveryThrottlingSettings.settings());
        final var clusterSettings = new ClusterSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 4).build(),
            registeredSettings
        );
        final List<IncomingRecoveryLimits> observed = new ArrayList<>();
        DataNodeRecoveryThrottlingSettings.watchIncomingRecoveryLimits(clusterSettings, ByteSizeValue.ZERO, observed::add);
        assertThat(observed, contains(new IncomingRecoveryLimits(4, 4)));

        // Both settings change in a single update, which is applied atomically.
        clusterSettings.applySettings(
            Settings.builder()
                .put(INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 6)
                .put(INDICES_RECOVERY_INCOMING_RECOVERIES_MAX_RELOCATION_PROPORTION_SETTING.getKey(), "50%")
                .build()
        );
        assertThat(observed, contains(new IncomingRecoveryLimits(4, 4), new IncomingRecoveryLimits(6, 3)));
    }

    public void testWatchIncomingRecoveryLimitsWhenSettingsAreNotRegistered() {
        final var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final List<IncomingRecoveryLimits> observed = new ArrayList<>();
        DataNodeRecoveryThrottlingSettings.watchIncomingRecoveryLimits(clusterSettings, ByteSizeValue.ofGb(4), observed::add);
        assertThat(observed, contains(IncomingRecoveryLimits.UNBOUNDED));
    }

    public void testWatchOutgoingRecoveryLimit() {
        final var registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registeredSettings.addAll(DataNodeRecoveryThrottlingSettings.settings());
        final var clusterSettings = new ClusterSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4).build(),
            registeredSettings
        );
        final List<Integer> observed = new ArrayList<>();
        DataNodeRecoveryThrottlingSettings.watchOutgoingRecoveryLimit(clusterSettings, ByteSizeValue.ofGb(2), observed::add);
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
        DataNodeRecoveryThrottlingSettings.watchOutgoingRecoveryLimit(clusterSettings, ByteSizeValue.ofGb(4), observed::add);
        assertThat(observed, contains(Integer.MAX_VALUE));
    }
}
