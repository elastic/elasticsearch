/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.recovery.RecoverySettings.DEFAULT_FACTOR_VALUE;
import static org.elasticsearch.indices.recovery.RecoverySettings.DEFAULT_MAX_BYTES_PER_SEC;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_MAX_OVERCOMMIT_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_SETTINGS;
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

    public void testAvailableBandwidthsSettingsAreAllConfigured() {
        final NodeRecoverySettings recoverySettings = nodeRecoverySettings();
        recoverySettings.withRandomIndicesRecoveryMaxBytesPerSec();
        recoverySettings.withRoles(randomDataNodeRoles());
        recoverySettings.withRandomMemory();

        final List<Setting<?>> randomSettings = randomSubsetOf(
            randomIntBetween(1, NODE_BANDWIDTH_RECOVERY_SETTINGS.size() - 1),
            NODE_BANDWIDTH_RECOVERY_SETTINGS
        );
        for (Setting<?> setting : randomSettings) {
            if (setting.getKey().equals(NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING.getKey())) {
                recoverySettings.withNetworkBandwidth(randomNonZeroByteSizeValue());
            } else if (setting.getKey().equals(NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING.getKey())) {
                recoverySettings.withDiskReadBandwidth(randomNonZeroByteSizeValue());
            } else if (setting.getKey().equals(NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING.getKey())) {
                recoverySettings.withDiskWriteBandwidth(randomNonZeroByteSizeValue());
            } else {
                throw new AssertionError();
            }
        }

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, recoverySettings::build);
        assertThat(
            exception.getMessage(),
            containsString(
                "Settings "
                    + NODE_BANDWIDTH_RECOVERY_SETTINGS.stream().map(Setting::getKey).toList()
                    + " must all be defined or all be undefined; but only settings "
                    + NODE_BANDWIDTH_RECOVERY_SETTINGS.stream().filter(randomSettings::contains).map(Setting::getKey).toList()
                    + " are configured."
            )
        );
    }

    public void testDefaultMaxBytesPerSecOnNonDataNode() {
        assertThat(
            "Non-data nodes have a default 40mb rate limit",
            nodeRecoverySettings().withRole(randomFrom("master", "ingest", "ml"))
                .withRandomBandwidths()
                .withRandomMemory()
                .build()
                .getMaxBytesPerSec(),
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
        final ByteSizeValue random = randomByteSizeValue();
        assertThat(
            "Data nodes that are not dedicated to cold/frozen should use the defined rate limit when set",
            nodeRecoverySettings().withIndicesRecoveryMaxBytesPerSec(random)
                .withRoles(randomDataNodeRoles())
                .withRandomMemory()
                .build()
                .getMaxBytesPerSec(),
            equalTo(random)
        );
    }

    public void testMaxBytesPerSecOnDataNodeWithIndicesRecoveryMaxBytesPerSecAndOvercommit() {
        final Double maxOvercommitFactor = randomBoolean() ? randomDoubleBetween(1.0d, 100.0d, true) : null;
        final ByteSizeValue indicesRecoveryMaxBytesPerSec = switch (randomInt(2)) {
            case 0 -> ByteSizeValue.MINUS_ONE;
            case 1 -> ByteSizeValue.ZERO;
            case 2 -> ByteSizeValue.ofGb(between(100, 1000));
            default -> throw new AssertionError();
        };
        assertThat(
            "Data nodes should not exceed the max. allowed overcommit when 'indices.recovery.max_bytes_per_sec' is too large",
            nodeRecoverySettings().withIndicesRecoveryMaxBytesPerSec(indicesRecoveryMaxBytesPerSec)
                .withNetworkBandwidth(ByteSizeValue.ofGb(1))
                .withDiskReadBandwidth(ByteSizeValue.ofMb(500))
                .withDiskWriteBandwidth(ByteSizeValue.ofMb(250))
                .withMaxOvercommitFactor(maxOvercommitFactor)
                .withRoles(randomDataNodeRoles())
                .withRandomMemory()
                .build()
                .getMaxBytesPerSec(),
            equalTo(
                ByteSizeValue.ofBytes(
                    Math.round(Objects.requireNonNullElse(maxOvercommitFactor, 100.d) * ByteSizeValue.ofMb(250).getBytes())
                )
            )
        );
    }

    public void testMaxBytesPerSecOnDataNodeWithAvailableBandwidths() {
        assertThat(
            "Data node should use pre 8.1.0 default because available bandwidths are lower",
            nodeRecoverySettings().withRoles(randomDataNodeRoles())
                .withRandomMemory()
                .withNetworkBandwidth(ByteSizeValue.ofGb(between(1, 10)))
                .withDiskReadBandwidth(ByteSizeValue.ofMb(between(10, 50)))
                .withDiskWriteBandwidth(ByteSizeValue.ofMb(between(10, 50)))
                .build()
                .getMaxBytesPerSec(),
            equalTo(DEFAULT_MAX_BYTES_PER_SEC)
        );

        final ByteSizeValue indicesRecoveryMaxBytesPerSec = ByteSizeValue.ofMb(randomFrom(100, 250));
        assertThat(
            "Data node should use 'indices.recovery.max_bytes_per_sec' setting because available bandwidths are lower",
            nodeRecoverySettings().withRoles(randomDataNodeRoles())
                .withRandomMemory()
                .withNetworkBandwidth(ByteSizeValue.ofGb(between(1, 10)))
                .withDiskReadBandwidth(ByteSizeValue.ofMb(between(10, 50)))
                .withDiskWriteBandwidth(ByteSizeValue.ofMb(between(10, 50)))
                .withIndicesRecoveryMaxBytesPerSec(indicesRecoveryMaxBytesPerSec)
                .build()
                .getMaxBytesPerSec(),
            equalTo(indicesRecoveryMaxBytesPerSec)
        );

        final Double factor = randomBoolean() ? randomDoubleBetween(0.5d, 1.0d, true) : null;

        final ByteSizeValue networkBandwidth = ByteSizeValue.ofMb(randomFrom(100, 250));
        assertThat(
            "Data node should use available disk read bandwidth",
            nodeRecoverySettings().withRoles(randomDataNodeRoles())
                .withRandomMemory()
                .withNetworkBandwidth(networkBandwidth)
                .withDiskReadBandwidth(ByteSizeValue.ofMb(between(250, 500)))
                .withDiskWriteBandwidth(ByteSizeValue.ofMb(between(250, 500)))
                .withOperatorDefaultFactor(factor)
                .build()
                .getMaxBytesPerSec(),
            equalTo(
                ByteSizeValue.ofBytes(Math.round(Objects.requireNonNullElse(factor, DEFAULT_FACTOR_VALUE) * networkBandwidth.getBytes()))
            )
        );

        final ByteSizeValue diskReadBandwidth = ByteSizeValue.ofMb(randomFrom(100, 250));
        assertThat(
            "Data node should use available disk read bandwidth",
            nodeRecoverySettings().withRoles(randomDataNodeRoles())
                .withRandomMemory()
                .withNetworkBandwidth(ByteSizeValue.ofGb(between(1, 10)))
                .withDiskReadBandwidth(diskReadBandwidth)
                .withDiskWriteBandwidth(ByteSizeValue.ofMb(between(250, 500)))
                .withOperatorDefaultFactor(factor)
                .build()
                .getMaxBytesPerSec(),
            equalTo(
                ByteSizeValue.ofBytes(Math.round(Objects.requireNonNullElse(factor, DEFAULT_FACTOR_VALUE) * diskReadBandwidth.getBytes()))
            )
        );

        final ByteSizeValue diskWriteBandwidth = ByteSizeValue.ofMb(randomFrom(100, 250));
        assertThat(
            "Data node should use available disk write bandwidth",
            nodeRecoverySettings().withRoles(randomDataNodeRoles())
                .withRandomMemory()
                .withNetworkBandwidth(ByteSizeValue.ofGb(between(1, 10)))
                .withDiskReadBandwidth(ByteSizeValue.ofMb(between(250, 500)))
                .withDiskWriteBandwidth(diskWriteBandwidth)
                .withOperatorDefaultFactor(factor)
                .build()
                .getMaxBytesPerSec(),
            equalTo(
                ByteSizeValue.ofBytes(Math.round(Objects.requireNonNullElse(factor, DEFAULT_FACTOR_VALUE) * diskWriteBandwidth.getBytes()))
            )
        );
    }

    public void testDefaultMaxBytesPerSecOnColdOrFrozenNode() {
        final Set<String> dataRoles = randomFrom(Set.of("data_cold"), Set.of("data_frozen"), Set.of("data_cold", "data_frozen"));
        {
            assertThat(
                "Dedicated cold/frozen data nodes with <= 4GB of RAM have a default 40mb rate limit",
                nodeRecoverySettings().withRoles(dataRoles)
                    .withMemory(ByteSizeValue.ofBytes(randomLongBetween(1L, ByteSizeUnit.GB.toBytes(4L))))
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
                .withMemory(ByteSizeValue.ofBytes(randomLongBetween(1L, ByteSizeUnit.TB.toBytes(4L))))
                .withIndicesRecoveryMaxBytesPerSec(random)
                .build()
                .getMaxBytesPerSec(),
            equalTo(random)
        );
    }

    public void testRecoverFromSnapshotPermitsAreNotLeakedWhenRecoverFromSnapshotIsDisabled() throws Exception {
        final Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.getKey(), false)
            .put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), 1)
            .put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 1)
            .build();

        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation("no warnings", RecoverySettings.class.getCanonicalName(), Level.WARN, "*")
        );
        mockAppender.start();
        final Logger logger = LogManager.getLogger(RecoverySettings.class);
        Loggers.addAppender(logger, mockAppender);

        try {
            assertThat(recoverySettings.getUseSnapshotsDuringRecovery(), is(false));

            for (int i = 0; i < 4; i++) {
                assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(nullValue()));
            }

            clusterSettings.applySettings(Settings.builder().put(INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.getKey(), true).build());

            final var releasable = recoverySettings.tryAcquireSnapshotDownloadPermits();
            assertThat(releasable, is(notNullValue()));
            releasable.close();

            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(logger, mockAppender);
            mockAppender.stop();
        }
    }

    private static ByteSizeValue randomByteSizeValue() {
        return ByteSizeValue.ofBytes(randomLongBetween(0L, Long.MAX_VALUE >> 16));
    }

    private static ByteSizeValue randomNonZeroByteSizeValue() {
        return ByteSizeValue.ofBytes(randomLongBetween(1L, Long.MAX_VALUE >> 16));
    }

    private static Set<String> randomDataNodeRoles() {
        final Set<String> roles = new HashSet<>(randomSubsetOf(randomIntBetween(1, 4), "data", "data_hot", "data_warm", "data_content"));
        roles.addAll(randomSubsetOf(Set.of("data_cold", "data_frozen")));
        if (randomBoolean()) {
            roles.addAll(
                randomSubsetOf(
                    DiscoveryNodeRole.roles()
                        .stream()
                        .filter(role -> role != DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE)
                        .filter(role -> role.canContainData() == false)
                        .map(DiscoveryNodeRole::roleName)
                        .collect(Collectors.toSet())
                )
            );
        }
        return roles;
    }

    private static NodeRecoverySettings nodeRecoverySettings() {
        return new NodeRecoverySettings();
    }

    private static class NodeRecoverySettings {

        private Set<String> roles;
        private ByteSizeValue physicalMemory;
        private @Nullable ByteSizeValue networkBandwidth;
        private @Nullable ByteSizeValue diskReadBandwidth;
        private @Nullable ByteSizeValue diskWriteBandwidth;
        private @Nullable ByteSizeValue indicesRecoveryMaxBytesPerSec;
        private @Nullable Double operatorDefaultFactor;
        private @Nullable Double maxOvercommitFactor;

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

        NodeRecoverySettings withIndicesRecoveryMaxBytesPerSec(ByteSizeValue indicesRecoveryMaxBytesPerSec) {
            this.indicesRecoveryMaxBytesPerSec = Objects.requireNonNull(indicesRecoveryMaxBytesPerSec);
            return this;
        }

        NodeRecoverySettings withRandomIndicesRecoveryMaxBytesPerSec() {
            if (randomBoolean()) {
                withIndicesRecoveryMaxBytesPerSec(randomByteSizeValue());
            }
            return this;
        }

        NodeRecoverySettings withNetworkBandwidth(ByteSizeValue networkBandwidth) {
            this.networkBandwidth = networkBandwidth;
            return this;
        }

        NodeRecoverySettings withDiskReadBandwidth(ByteSizeValue diskReadBandwidth) {
            this.diskReadBandwidth = diskReadBandwidth;
            return this;
        }

        NodeRecoverySettings withDiskWriteBandwidth(ByteSizeValue diskWriteBandwidth) {
            this.diskWriteBandwidth = diskWriteBandwidth;
            return this;
        }

        NodeRecoverySettings withRandomBandwidths() {
            if (randomBoolean()) {
                withNetworkBandwidth(randomNonZeroByteSizeValue());
                withDiskReadBandwidth(randomNonZeroByteSizeValue());
                withDiskWriteBandwidth(randomNonZeroByteSizeValue());
            }
            return this;
        }

        NodeRecoverySettings withOperatorDefaultFactor(Double factor) {
            this.operatorDefaultFactor = factor;
            return this;
        }

        NodeRecoverySettings withMaxOvercommitFactor(Double factor) {
            this.maxOvercommitFactor = factor;
            return this;
        }

        RecoverySettings build() {
            final Settings.Builder settings = Settings.builder();
            settings.put(TOTAL_PHYSICAL_MEMORY_OVERRIDING_TEST_SETTING.getKey(), Objects.requireNonNull(physicalMemory));
            if (roles.isEmpty() == false) {
                settings.putList(NODE_ROLES_SETTING.getKey(), new ArrayList<>(roles));
            }
            if (indicesRecoveryMaxBytesPerSec != null) {
                settings.put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), indicesRecoveryMaxBytesPerSec);
            }
            if (networkBandwidth != null) {
                settings.put(NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING.getKey(), networkBandwidth);
            }
            if (diskReadBandwidth != null) {
                settings.put(NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING.getKey(), diskReadBandwidth);
            }
            if (diskWriteBandwidth != null) {
                settings.put(NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING.getKey(), diskWriteBandwidth);
            }
            if (operatorDefaultFactor != null) {
                settings.put(NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_SETTING.getKey(), operatorDefaultFactor);
            }
            if (maxOvercommitFactor != null) {
                settings.put(NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_MAX_OVERCOMMIT_SETTING.getKey(), maxOvercommitFactor);
            }
            return new RecoverySettings(settings.build(), new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        }
    }
}
