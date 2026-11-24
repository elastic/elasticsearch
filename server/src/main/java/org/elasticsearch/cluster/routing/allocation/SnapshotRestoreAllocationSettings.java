/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

/**
 * Settings for snapshot-restore disk capacity checks during allocation.
 * Temporary until restore no longer needs a full local copy on disk.
 * Operator-only and registered in StatelessPlugin to allow removal without deprecation.
 */
public final class SnapshotRestoreAllocationSettings {

    private SnapshotRestoreAllocationSettings() {}

    /**
     * Enables snapshot-restore disk capacity checks. When true:
     * <ul>
     * <li>{@link org.elasticsearch.cluster.InternalClusterInfoService} collects disk usage into
     * {@link org.elasticsearch.cluster.ClusterInfo} even if the disk threshold decider is disabled</li>
     * <li>gates the stateless restore allocation decider, storage monitor, and ESA wait for snapshot sizes</li>
     * <li>gates restore-capacity autoscaling contributions (serverless)</li>
     * </ul>
     */
    public static final Setting<Boolean> SNAPSHOT_RESTORE_ALLOCATION_ENABLED = Setting.boolSetting(
        "cluster.routing.allocation.snapshot_restore.enabled",
        false,
        Property.OperatorDynamic,
        Property.NodeScope
    );
}
