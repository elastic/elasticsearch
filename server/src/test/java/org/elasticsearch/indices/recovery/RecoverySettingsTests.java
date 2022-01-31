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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.recovery.RecoverySettings.ALL_MAX_BYTES_PER_SEC_EXTERNAL_SETTINGS;
import static org.elasticsearch.indices.recovery.RecoverySettings.DEFAULT_MAX_BYTES_PER_SEC;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.JAVA_VERSION_OVERRIDING_TEST_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_DISK_AVAILABLE_BANDWIDTH_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_NETWORK_AVAILABLE_BANDWIDTH_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_RECOVERY_MAX_BYTES_PER_SEC_FACTOR_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.TOTAL_PHYSICAL_MEMORY_OVERRIDING_TEST_SETTING;
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

    public void testDefaultMaxBytesPerSecOnNonDataNode() {
        assertThat(
            "Non-data nodes have a default 40mb rate limit",
            nodeRecoverySettings().withRole(randomFrom("master", "ingest", "ml")).withRandomMemory().build().getMaxBytesPerSec(),
            equalTo(DEFAULT_MAX_BYTES_PER_SEC)
        );
    }

    public void testMaxBytesPerSecOnNonDataNodeWithIndicesRecoveryMaxBytesPerSec() {
        final ByteSizeValue random = randomByteSizeValue();
        assertThat(
            "Non-data nodes should use the defined rate limit when set",
            nodeRecoverySettings().withRole(randomFrom("master", "ingest", "ml"))
                .withIndicesRecoveryMaxBytesPerSec(random)
                .withRandomMemory()
                .build()
                .getMaxBytesPerSec(),
            equalTo(random)
        );
    }

    public void testDefaultMaxBytesPerSecOnDataNode() {
        assertThat(
            "Data nodes that are not dedicated to cold/frozen have a default 40mb rate limit",
            nodeRecoverySettings().withRole(randomFrom("data", "data_hot", "data_warm", "data_content"))
                .withRandomMemory()
                .build()
                .getMaxBytesPerSec(),
            equalTo(DEFAULT_MAX_BYTES_PER_SEC)
        );
    }

    public void testMaxBytesPerSecOnDataNodeWithIndicesRecoveryMaxBytesPerSec() {
        final Set<String> roles = new HashSet<>(randomSubsetOf(randomIntBetween(1, 4), "data", "data_hot", "data_warm", "data_content"));
        roles.addAll(randomSubsetOf(Set.of("data_cold", "data_frozen")));
        final ByteSizeValue random = randomByteSizeValue();
        assertThat(
            "Data nodes that are not dedicated to cold/frozen should use the defined rate limit when set",
            nodeRecoverySettings().withRoles(roles)
                .withIndicesRecoveryMaxBytesPerSec(random)
                .withRandomMemory()
                .build()
                .getMaxBytesPerSec(),
            equalTo(random)
        );
    }

    public void testDefaultMaxBytesPerSecOnColdOrFrozenNodeWithOldJvm() {
        assertThat(
            "Data nodes with only cold/frozen data roles have a default 40mb rate limit on Java version prior to 14",
            nodeRecoverySettings().withRoles(randomFrom(Set.of("data_cold"), Set.of("data_frozen"), Set.of("data_cold", "data_frozen")))
                .withJavaVersion(randomFrom("8", "9", "11"))
                .withRandomMemory()
                .build()
                .getMaxBytesPerSec(),
            equalTo(DEFAULT_MAX_BYTES_PER_SEC)
        );
    }

    public void testDefaultMaxBytesPerSecOnColdOrFrozenNode() {
        final Set<String> dataRoles = randomFrom(Set.of("data_cold"), Set.of("data_frozen"), Set.of("data_cold", "data_frozen"));
        final String recentVersion = JavaVersion.current().compareTo(JavaVersion.parse("14")) < 0 ? "14" : null;
        {
            assertThat(
                "Dedicated cold/frozen data nodes with <= 4GB of RAM have a default 40mb rate limit",
                nodeRecoverySettings().withRoles(dataRoles)
                    .withMemory(ByteSizeValue.ofBytes(randomLongBetween(1L, ByteSizeUnit.GB.toBytes(4L))))
                    .withJavaVersion(recentVersion)
                    .build()
                    .getMaxBytesPerSec(),
                equalTo(new ByteSizeValue(40, ByteSizeUnit.MB))
            );
        }
        {
            assertThat(
                "Dedicated cold/frozen data nodes with 4GB < RAM <= 8GB have a default 60mb rate limit",
                nodeRecoverySettings().withRoles(dataRoles)
                    .withMemory(ByteSizeValue.ofBytes(randomLongBetween(ByteSizeUnit.GB.toBytes(4L) + 1L, ByteSizeUnit.GB.toBytes(8L))))
                    .withJavaVersion(recentVersion)
                    .build()
                    .getMaxBytesPerSec(),
                equalTo(new ByteSizeValue(60, ByteSizeUnit.MB))
            );
        }
        {
            assertThat(
                "Dedicated cold/frozen data nodes with 8GB < RAM <= 16GB have a default 90mb rate limit",
                nodeRecoverySettings().withRoles(dataRoles)
                    .withMemory(ByteSizeValue.ofBytes(randomLongBetween(ByteSizeUnit.GB.toBytes(8L) + 1L, ByteSizeUnit.GB.toBytes(16L))))
                    .withJavaVersion(recentVersion)
                    .build()
                    .getMaxBytesPerSec(),
                equalTo(new ByteSizeValue(90, ByteSizeUnit.MB))
            );
        }
        {
            assertThat(
                "Dedicated cold/frozen data nodes with 16GB < RAM <= 32GB have a default 90mb rate limit",
                nodeRecoverySettings().withRoles(dataRoles)
                    .withMemory(ByteSizeValue.ofBytes(randomLongBetween(ByteSizeUnit.GB.toBytes(16L) + 1L, ByteSizeUnit.GB.toBytes(32L))))
                    .withJavaVersion(recentVersion)
                    .build()
                    .getMaxBytesPerSec(),
                equalTo(new ByteSizeValue(125, ByteSizeUnit.MB))
            );
        }
        {
            assertThat(
                "Dedicated cold/frozen data nodes with RAM > 32GB have a default 250mb rate limit",
                nodeRecoverySettings().withRoles(dataRoles)
                    .withMemory(ByteSizeValue.ofBytes(randomLongBetween(ByteSizeUnit.GB.toBytes(32L) + 1L, ByteSizeUnit.TB.toBytes(4L))))
                    .withJavaVersion(recentVersion)
                    .build()
                    .getMaxBytesPerSec(),
                equalTo(new ByteSizeValue(250, ByteSizeUnit.MB))
            );
        }
    }

    public void testMaxBytesPerSecOnColdOrFrozenNodeWithIndicesRecoveryMaxBytesPerSec() {
        final ByteSizeValue random = randomByteSizeValue();
        assertThat(
            "Dedicated cold/frozen data nodes should use the defined rate limit when set",
            nodeRecoverySettings().withRoles(randomFrom(Set.of("data_cold"), Set.of("data_frozen"), Set.of("data_cold", "data_frozen")))
                .withJavaVersion(JavaVersion.current().compareTo(JavaVersion.parse("14")) < 0 ? "14" : null)
                .withMemory(ByteSizeValue.ofBytes(randomLongBetween(1L, ByteSizeUnit.TB.toBytes(4L))))
                .withIndicesRecoveryMaxBytesPerSec(random)
                .build()
                .getMaxBytesPerSec(),
            equalTo(random)
        );
    }

    public static ByteSizeValue randomByteSizeValue() {
        return new ByteSizeValue(randomLongBetween(0L, Long.MAX_VALUE >> 16));
    }

    public static ByteSizeValue randomNonZeroByteSizeValue() {
        return new ByteSizeValue(randomLongBetween(1L, Long.MAX_VALUE >> 16));
    }

    static NodeRecoverySettings nodeRecoverySettings() {
        return new NodeRecoverySettings();
    }

    private static class NodeRecoverySettings {

        private Set<String> roles;
        private ByteSizeValue physicalMemory;
        private @Nullable String javaVersion;
        private @Nullable ByteSizeValue indicesRecoveryMaxBytesPerSec;

        NodeRecoverySettings withRole(String role) {
            this.roles = Set.of(Objects.requireNonNull(role));
            return this;
        }

        NodeRecoverySettings withRoles(Set<String> roles) {
            this.roles = Objects.requireNonNull(roles);
            return this;
        }

        NodeRecoverySettings withMemory(ByteSizeValue physicalMemory) {
            this.physicalMemory = Objects.requireNonNull(physicalMemory);
            return this;
        }

        NodeRecoverySettings withRandomMemory() {
            return withMemory(ByteSizeValue.ofBytes(randomLongBetween(ByteSizeUnit.GB.toBytes(1L), ByteSizeUnit.TB.toBytes(4L))));
        }

        NodeRecoverySettings withJavaVersion(String javaVersion) {
            this.javaVersion = javaVersion;
            return this;
        }

        NodeRecoverySettings withIndicesRecoveryMaxBytesPerSec(ByteSizeValue indicesRecoveryMaxBytesPerSec) {
            this.indicesRecoveryMaxBytesPerSec = Objects.requireNonNull(indicesRecoveryMaxBytesPerSec);
            return this;
        }

        RecoverySettings build() {
            final Settings.Builder settings = Settings.builder();
            settings.put(TOTAL_PHYSICAL_MEMORY_OVERRIDING_TEST_SETTING.getKey(), Objects.requireNonNull(physicalMemory));
            if (roles.isEmpty() == false) {
                settings.putList(NODE_ROLES_SETTING.getKey(), new ArrayList<>(roles));
            }
            if (javaVersion != null) {
                settings.put(JAVA_VERSION_OVERRIDING_TEST_SETTING.getKey(), javaVersion);
            }
            if (indicesRecoveryMaxBytesPerSec != null) {
                settings.put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), indicesRecoveryMaxBytesPerSec);
            }
            return new RecoverySettings(settings.build(), new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        }
    }
}
