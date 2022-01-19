/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.recovery.RecoverySettings.ALL_MAX_BYTES_PER_SEC_EXTERNAL_SETTINGS;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_DISK_AVAILABLE_BANDWIDTH_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_NETWORK_AVAILABLE_BANDWIDTH_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_RECOVERY_MAX_BYTES_PER_SEC_FACTOR_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RecoverySettingsTests extends ESTestCase {
    public void testSnapshotDownloadPermitsAreNotGrantedWhenSnapshotsUseFlagIsFalse() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder()
                .put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 5)
                .put(INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.getKey(), false)
                .build(),
            clusterSettings
        );

        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(nullValue()));
    }

    public void testGrantsSnapshotDownloadPermitsUpToMaxPermits() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 5).build(),
            clusterSettings
        );

        Releasable permit = recoverySettings.tryAcquireSnapshotDownloadPermits();
        assertThat(permit, is(notNullValue()));

        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(nullValue()));

        permit.close();
        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(notNullValue()));
    }

    public void testSnapshotDownloadPermitCanBeDynamicallyUpdated() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 5).build(),
            clusterSettings
        );

        Releasable permit = recoverySettings.tryAcquireSnapshotDownloadPermits();
        assertThat(permit, is(notNullValue()));

        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(nullValue()));
        clusterSettings.applySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 10).build()
        );

        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(notNullValue()));
        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(nullValue()));
        permit.close();
    }

    public void testMaxConcurrentSnapshotFileDownloadsPerNodeIsValidated() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), 10)
            .put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 5)
            .build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RecoverySettings(settings, clusterSettings)
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "[indices.recovery.max_concurrent_snapshot_file_downloads_per_node]=5 "
                    + "is less than [indices.recovery.max_concurrent_snapshot_file_downloads]=10"
            )
        );
    }

    public void testIndicesRecoveryMaxBytesPerSecSetting() {
        final ByteSizeValue indicesRecoveryMaxBytesPerSec = randomByteSizeValue();

        final Settings.Builder settings = Settings.builder();
        settings.put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), indicesRecoveryMaxBytesPerSec);
        if (randomBoolean()) {
            ALL_MAX_BYTES_PER_SEC_EXTERNAL_SETTINGS.forEach(setting -> settings.put(setting.getKey(), ByteSizeValue.ZERO));
        }

        assertThat(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.get(settings.build()), equalTo(indicesRecoveryMaxBytesPerSec));
    }

    public void testExternalSettingsAreAllConfigured() {
        final Setting<?> randomBandwidthSetting = randomFrom(ALL_MAX_BYTES_PER_SEC_EXTERNAL_SETTINGS);

        final Settings.Builder settings = Settings.builder();
        settings.put(randomBandwidthSetting.getKey(), randomNonZeroByteSizeValue());
        if (randomBoolean()) {
            settings.put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), randomByteSizeValue());
        }

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.get(settings.build())
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "Settings [node.disk.allocated_bandwidth, node.network.allocated_bandwidth] must all be defined or all be undefined; "
                    + "but only settings ["
                    + randomBandwidthSetting.getKey()
                    + "] are configured."
            )
        );
    }

    public void testIndicesRecoveryMaxBytesPerSecSettingTakesPrecedenceOverDefaults() {
        final ByteSizeValue indicesRecoveryMaxBytesPerSec = randomByteSizeValue();

        final Settings.Builder settings = Settings.builder();
        settings.put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), indicesRecoveryMaxBytesPerSec);
        ALL_MAX_BYTES_PER_SEC_EXTERNAL_SETTINGS.forEach(s -> settings.put(s.getKey(), randomNonZeroByteSizeValue()));
        if (randomBoolean()) {
            List<DiscoveryNodeRole> roles = randomSubsetOf(DiscoveryNodeRole.roles());
            settings.putList(NODE_ROLES_SETTING.getKey(), roles.stream().map(DiscoveryNodeRole::roleName).collect(Collectors.toList()));
        }

        assertThat(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.get(settings.build()), equalTo(indicesRecoveryMaxBytesPerSec));

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RecoverySettings recoverySettings = new RecoverySettings(settings.build(), clusterSettings);
        assertThat(recoverySettings.getMaxBytesPerSec(), equalTo(indicesRecoveryMaxBytesPerSec));
    }

    public void testIndicesRecoveryMaxBytesPerSecDefaultOnNonDataNode() {
        Set<String> nonDataRoles = DiscoveryNodeRole.roles()
            .stream()
            .filter(role -> role != DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE)
            .filter(role -> role.canContainData() == false)
            .map(DiscoveryNodeRole::roleName)
            .collect(Collectors.toSet());

        final Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.putList(NODE_ROLES_SETTING.getKey(), randomSubsetOf(randomIntBetween(1, nonDataRoles.size()), nonDataRoles));
        if (randomBoolean()) {
            settingsBuilder.put(NODE_DISK_AVAILABLE_BANDWIDTH_SETTING.getKey(), randomNonZeroByteSizeValue())
                .put(NODE_NETWORK_AVAILABLE_BANDWIDTH_SETTING.getKey(), randomNonZeroByteSizeValue());
        }

        final Settings settings = settingsBuilder.build();
        assertThat(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.get(settings), equalTo(RecoverySettings.DEFAULT_MAX_BYTES_PER_SEC));

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        assertThat(recoverySettings.getMaxBytesPerSec(), equalTo(RecoverySettings.DEFAULT_MAX_BYTES_PER_SEC));
    }

    public void testIndicesRecoveryMaxBytesPerSecFromExternalSettings() {
        final Settings.Builder settingsBuilder = Settings.builder();

        final ByteSizeValue diskBandwidth = ByteSizeValue.ofMb(randomIntBetween(1, 300));
        settingsBuilder.put(NODE_DISK_AVAILABLE_BANDWIDTH_SETTING.getKey(), diskBandwidth);

        final ByteSizeValue networkBandwidth = ByteSizeValue.ofMb(randomIntBetween(1, 12500));
        settingsBuilder.put(NODE_NETWORK_AVAILABLE_BANDWIDTH_SETTING.getKey(), networkBandwidth);

        final double factor;
        if (randomBoolean()) {
            factor = randomDoubleBetween(0.0, 1.0, true);
            settingsBuilder.put(NODE_RECOVERY_MAX_BYTES_PER_SEC_FACTOR_SETTING.getKey(), factor);
        } else {
            factor = NODE_RECOVERY_MAX_BYTES_PER_SEC_FACTOR_SETTING.get(Settings.EMPTY);
        }

        final long maxBytesPerSec = RecoverySettings.recoveryMaxBytesPerSec(factor, diskBandwidth.getBytes(), networkBandwidth.getBytes());

        final Settings settings = settingsBuilder.build();
        assertThat(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.get(settings), equalTo(new ByteSizeValue(maxBytesPerSec)));

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        assertThat(recoverySettings.getMaxBytesPerSec(), equalTo(new ByteSizeValue(maxBytesPerSec)));
    }

    public static ByteSizeValue randomByteSizeValue() {
        return new ByteSizeValue(randomLongBetween(0L, Long.MAX_VALUE >> 16));
    }

    public static ByteSizeValue randomNonZeroByteSizeValue() {
        return new ByteSizeValue(randomLongBetween(1L, Long.MAX_VALUE >> 16));
    }
}
