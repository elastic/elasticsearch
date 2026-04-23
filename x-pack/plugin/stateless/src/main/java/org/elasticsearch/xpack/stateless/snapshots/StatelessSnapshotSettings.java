/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;

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
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> STATELESS_SNAPSHOT_WAIT_FOR_ACTIVE_PRIMARY_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "stateless.snapshot.wait_for_active_primary_timeout",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
}
