/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.RecoveryDirectCancellationService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.RecoveryGateMonitor;
import org.elasticsearch.indices.recovery.ThrottlingRecoveryService;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.recovery.StatelessPrimaryRelocationSourceService;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/// TODO: Remove this test suite once we have rolled DNRT everywhere and updated the defaults.
public class DNRTRolloutTests extends ESTestCase {

    public void testDNRTSettingsDisabledWithoutOverrides() {
        final ClusterSettings clusterSettings = statelessClusterSettings();
        assertThat(
            effectiveValue(clusterSettings, RecoveryDirectCancellationService.ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING),
            is(false)
        );
        assertThat(
            effectiveValue(clusterSettings, RecoveryDirectCancellationService.ENABLE_DIRECT_CANCELLATIONS_FOR_SNAPSHOTS_SETTING),
            is(false)
        );
        assertThat(effectiveValue(clusterSettings, RecoveryGateMonitor.ENABLE_RECOVERY_GATES_SETTING), is(false));
        assertThat(
            effectiveValue(clusterSettings, ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING),
            equalTo(Integer.MAX_VALUE)
        );
        assertThat(
            effectiveValue(
                clusterSettings,
                ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING
            ),
            equalTo(Double.MAX_VALUE)
        );
        assertThat(
            effectiveValue(
                clusterSettings,
                ThrottlingRecoveryService.INDICES_RECOVERY_INCOMING_RECOVERIES_MAX_RELOCATION_PROPORTION_SETTING
            ).getAsPercent(),
            equalTo(100.0)
        );
        assertThat(
            effectiveValue(clusterSettings, PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING),
            equalTo(Integer.MAX_VALUE)
        );
        assertThat(
            effectiveValue(
                clusterSettings,
                StatelessPrimaryRelocationSourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_PER_HEAP_GB_SETTING
            ),
            equalTo(Double.MAX_VALUE)
        );
    }

    /// Reads the value the way the consuming services do.
    private static <T> T effectiveValue(ClusterSettings clusterSettings, Setting<T> setting) {
        final AtomicReference<T> value = new AtomicReference<>();
        clusterSettings.initializeAndWatch(setting, value::set);
        return value.get();
    }

    private static ClusterSettings statelessClusterSettings() {
        final Settings nodeSettings = Settings.builder()
            .put(StatelessPlugin.STATELESS_ENABLED.getKey(), true)
            .put(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                randomFrom(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE).roleName()
            )
            .build();
        final StatelessPlugin plugin = new TestUtils.StatelessPluginWithTrialLicense(nodeSettings);
        final Set<Setting<?>> registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        plugin.getSettings().stream().filter(Setting::hasNodeScope).forEach(registeredSettings::add);
        return new ClusterSettings(Settings.builder().put(nodeSettings).put(plugin.additionalSettings()).build(), registeredSettings);
    }
}
