/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;

import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING_NAME;

public class StatelessSnapshotSettings {

    public enum StatelessSnapshotEnabledStatus {
        /**
         * The stateless snapshot functionality is disabled
         */
        DISABLED(false),

        /**
         * The stateless snapshot reads indices data from the object store. But otherwise no changes.
         * The commit information is obtained directly from the local primary shard.
         */
        READ_FROM_OBJECT_STORE(false),

        /**
         * The stateless snapshot reads indices data from the object store.
         * The commit information is obtained from the primary shard via a transport request,
         * which allows the snapshot to be taken on a node that does not host the primary shard.
         */
        ENABLED(true);

        /**
         * Whether the enabled status supports relocation during snapshots.
         */
        private final boolean supportsRelocationDuringSnapshot;

        StatelessSnapshotEnabledStatus(boolean supportsRelocationDuringSnapshot) {
            this.supportsRelocationDuringSnapshot = supportsRelocationDuringSnapshot;
        }

        public boolean supportsRelocationDuringSnapshot() {
            return supportsRelocationDuringSnapshot;
        }

        public String description() {
            return this + " [supportsRelocationDuringSnapshot=" + supportsRelocationDuringSnapshot() + "]";
        }
    }

    public static final Setting<StatelessSnapshotEnabledStatus> STATELESS_SNAPSHOT_ENABLED_SETTING = Setting.enumSetting(
        StatelessSnapshotEnabledStatus.class,
        "stateless.snapshot.enabled",
        StatelessSnapshotEnabledStatus.DISABLED,
        new Setting.Validator<>() {
            @Override
            public void validate(StatelessSnapshotEnabledStatus value) {}

            @Override
            public void validate(StatelessSnapshotEnabledStatus value, Map<Setting<?>, Object> settings) {
                validateSettingsConsistency(value, (boolean) settings.get(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING));
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return Iterators.single(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING);
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING = Setting.boolSetting(
        RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING_NAME,
        false,
        new Setting.Validator<>() {
            @Override
            public void validate(Boolean value) {}

            @Override
            public void validate(Boolean value, Map<Setting<?>, Object> settings) {
                validateSettingsConsistency((StatelessSnapshotEnabledStatus) settings.get(STATELESS_SNAPSHOT_ENABLED_SETTING), value);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return Iterators.single(STATELESS_SNAPSHOT_ENABLED_SETTING);
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> STATELESS_SNAPSHOT_WAIT_FOR_ACTIVE_PRIMARY_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "stateless.snapshot.wait_for_active_primary_timeout",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static void validateSettingsConsistency(
        StatelessSnapshotSettings.StatelessSnapshotEnabledStatus statelessSnapshotEnabledStatus,
        boolean relocationDuringSnapshotEnabled
    ) {
        if (statelessSnapshotEnabledStatus != StatelessSnapshotSettings.StatelessSnapshotEnabledStatus.ENABLED
            && relocationDuringSnapshotEnabled) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Setting [%s] cannot be [true] unless setting [%s] is [%s]",
                    StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(),
                    StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(),
                    StatelessSnapshotSettings.StatelessSnapshotEnabledStatus.ENABLED
                )
            );
        }
    }
}
