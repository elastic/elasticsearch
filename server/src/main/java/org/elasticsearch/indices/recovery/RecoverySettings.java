/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.store.RateLimiter.SimpleRateLimiter;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.node.NodeRoleSettings;

import java.util.List;
import java.util.stream.Collectors;

public class RecoverySettings {
    public static final Version SNAPSHOT_RECOVERIES_SUPPORTED_VERSION = Version.V_7_15_0;

    private static final Logger logger = LogManager.getLogger(RecoverySettings.class);

    public static final Setting<ByteSizeValue> INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING =
        Setting.byteSizeSetting(
            "indices.recovery.max_bytes_per_sec",
            s -> {
                final ByteSizeValue defaultMaxBytesPerSec = new ByteSizeValue(40, ByteSizeUnit.MB);
                final List<DiscoveryNodeRole> roles = NodeRoleSettings.NODE_ROLES_SETTING.get(s);
                final List<DiscoveryNodeRole> dataRoles =
                    roles.stream().filter(DiscoveryNodeRole::canContainData).collect(Collectors.toUnmodifiableList());
                if (dataRoles.isEmpty()) {
                    // if the node is not a data node, this value doesn't matter, use the default
                    return defaultMaxBytesPerSec.getStringRep();
                }
                if (dataRoles.stream().allMatch(dn -> dn.equals(DiscoveryNodeRole.DATA_COLD_NODE_ROLE)
                    || dn.equals(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)) == false) {
                    // the node is not a dedicated cold and/or frozen node, use the default
                    return defaultMaxBytesPerSec.getStringRep();
                }
                /*
                 * Now we are looking at a node that has a single data role, that data role is the cold data role, and the node does not
                 * have the master role. In this case, we are going to set the recovery size as a function of the memory size. We are making
                 * an assumption here that the size of the instance is correlated with I/O resources. That is we are assuming that the
                 * larger the instance, the more disk and networking capacity it has available.
                 */
                if (JavaVersion.current().compareTo(JavaVersion.parse("14")) < 0) {
                    // prior to JDK 14, the JDK did not take into consideration container memory limits when reporting total system memory
                    return defaultMaxBytesPerSec.getStringRep();
                }
                final ByteSizeValue totalPhysicalMemory = new ByteSizeValue(OsProbe.getInstance().getTotalPhysicalMemorySize());
                final ByteSizeValue maxBytesPerSec;
                if (totalPhysicalMemory.compareTo(new ByteSizeValue(4, ByteSizeUnit.GB)) <= 0) {
                    maxBytesPerSec = new ByteSizeValue(40, ByteSizeUnit.MB);
                } else if (totalPhysicalMemory.compareTo(new ByteSizeValue(8, ByteSizeUnit.GB)) <= 0) {
                    maxBytesPerSec = new ByteSizeValue(60, ByteSizeUnit.MB);
                } else if (totalPhysicalMemory.compareTo(new ByteSizeValue(16, ByteSizeUnit.GB)) <= 0) {
                    maxBytesPerSec = new ByteSizeValue(90, ByteSizeUnit.MB);
                } else if (totalPhysicalMemory.compareTo(new ByteSizeValue(32, ByteSizeUnit.GB)) <= 0) {
                    maxBytesPerSec = new ByteSizeValue(125, ByteSizeUnit.MB);
                } else {
                    maxBytesPerSec = new ByteSizeValue(250, ByteSizeUnit.MB);
                }
                return maxBytesPerSec.getStringRep();
            },
            Property.Dynamic,
            Property.NodeScope);

    /**
     * Controls the maximum number of file chunk requests that can be sent concurrently from the source node to the target node.
     */
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING =
        Setting.intSetting("indices.recovery.max_concurrent_file_chunks", 2, 1, 8, Property.Dynamic, Property.NodeScope);

    /**
     * Controls the maximum number of operation chunk requests that can be sent concurrently from the source node to the target node.
     */
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING =
        Setting.intSetting("indices.recovery.max_concurrent_operations", 1, 1, 4, Property.Dynamic, Property.NodeScope);

    /**
     * how long to wait before retrying after issues cause by cluster state syncing between nodes
     * i.e., local node is not yet known on remote node, remote shard not yet started etc.
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING =
        Setting.positiveTimeSetting("indices.recovery.retry_delay_state_sync", TimeValue.timeValueMillis(500),
            Property.Dynamic, Property.NodeScope);

    /** how long to wait before retrying after network related issues */
    public static final Setting<TimeValue> INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING =
        Setting.positiveTimeSetting("indices.recovery.retry_delay_network", TimeValue.timeValueSeconds(5),
            Property.Dynamic, Property.NodeScope);

    /** timeout value to use for requests made as part of the recovery process */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("indices.recovery.internal_action_timeout", TimeValue.timeValueMinutes(15),
            Property.Dynamic, Property.NodeScope);

    /** timeout value to use for the retrying of requests made as part of the recovery process */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("indices.recovery.internal_action_retry_timeout", TimeValue.timeValueMinutes(1),
            Property.Dynamic, Property.NodeScope);

    /**
     * timeout value to use for requests made as part of the recovery process that are expected to take long time.
     * defaults to twice `indices.recovery.internal_action_timeout`.
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING =
        Setting.timeSetting("indices.recovery.internal_action_long_timeout",
            (s) -> TimeValue.timeValueMillis(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.get(s).millis() * 2),
            TimeValue.timeValueSeconds(0), Property.Dynamic,  Property.NodeScope);

    /**
     * recoveries that don't show any activity for more then this interval will be failed.
     * defaults to `indices.recovery.internal_action_long_timeout`
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING =
        Setting.timeSetting("indices.recovery.recovery_activity_timeout",
            INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING::get, TimeValue.timeValueSeconds(0),
            Property.Dynamic, Property.NodeScope);

    /**
     * recoveries would try to use files from available snapshots instead of sending them from the source node.
     * defaults to `false`
     */
    public static final Setting<Boolean> INDICES_RECOVERY_USE_SNAPSHOTS_SETTING =
        Setting.boolSetting("indices.recovery.use_snapshots", true, Property.Dynamic, Property.NodeScope);

    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS =
        Setting.intSetting("indices.recovery.max_concurrent_snapshot_file_downloads",
            5,
            1,
            20,
            Property.Dynamic,
            Property.NodeScope
        );

    public static final ByteSizeValue DEFAULT_CHUNK_SIZE = new ByteSizeValue(512, ByteSizeUnit.KB);

    private volatile ByteSizeValue maxBytesPerSec;
    private volatile int maxConcurrentFileChunks;
    private volatile int maxConcurrentOperations;
    private volatile SimpleRateLimiter rateLimiter;
    private volatile TimeValue retryDelayStateSync;
    private volatile TimeValue retryDelayNetwork;
    private volatile TimeValue activityTimeout;
    private volatile TimeValue internalActionTimeout;
    private volatile TimeValue internalActionRetryTimeout;
    private volatile TimeValue internalActionLongTimeout;
    private volatile boolean useSnapshotsDuringRecovery;
    private volatile int maxConcurrentSnapshotFileDownloads;

    private volatile ByteSizeValue chunkSize = DEFAULT_CHUNK_SIZE;

    public RecoverySettings(Settings settings, ClusterSettings clusterSettings) {
        this.retryDelayStateSync = INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.get(settings);
        this.maxConcurrentFileChunks = INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING.get(settings);
        this.maxConcurrentOperations = INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING.get(settings);
        // doesn't have to be fast as nodes are reconnected every 10s by default (see InternalClusterService.ReconnectToNodes)
        // and we want to give the master time to remove a faulty node
        this.retryDelayNetwork = INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.get(settings);

        this.internalActionTimeout = INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.get(settings);
        this.internalActionRetryTimeout = INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING.get(settings);
        this.internalActionLongTimeout = INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING.get(settings);

        this.activityTimeout = INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.get(settings);
        this.maxBytesPerSec = INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.get(settings);
        if (maxBytesPerSec.getBytes() <= 0) {
            rateLimiter = null;
        } else {
            rateLimiter = new SimpleRateLimiter(maxBytesPerSec.getMbFrac());
        }
        this.useSnapshotsDuringRecovery = INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.get(settings);
        this.maxConcurrentSnapshotFileDownloads = INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.get(settings);

        logger.debug("using max_bytes_per_sec[{}]", maxBytesPerSec);

        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING, this::setMaxBytesPerSec);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING, this::setMaxConcurrentFileChunks);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING,
            this::setMaxConcurrentOperations);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING, this::setRetryDelayStateSync);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING, this::setRetryDelayNetwork);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING, this::setInternalActionTimeout);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING,
            this::setInternalActionLongTimeout);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING, this::setActivityTimeout);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_USE_SNAPSHOTS_SETTING, this::setUseSnapshotsDuringRecovery);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS,
            this::setMaxConcurrentSnapshotFileDownloads);
    }

    public RateLimiter rateLimiter() {
        return rateLimiter;
    }

    public TimeValue retryDelayNetwork() {
        return retryDelayNetwork;
    }

    public TimeValue retryDelayStateSync() {
        return retryDelayStateSync;
    }

    public TimeValue activityTimeout() {
        return activityTimeout;
    }

    public TimeValue internalActionTimeout() {
        return internalActionTimeout;
    }

    public TimeValue internalActionRetryTimeout() {
        return internalActionRetryTimeout;
    }

    public TimeValue internalActionLongTimeout() {
        return internalActionLongTimeout;
    }

    public ByteSizeValue getChunkSize() { return chunkSize; }

    public void setChunkSize(ByteSizeValue chunkSize) { // only settable for tests
        if (chunkSize.bytesAsInt() <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        this.chunkSize = chunkSize;
    }

    public void setRetryDelayStateSync(TimeValue retryDelayStateSync) {
        this.retryDelayStateSync = retryDelayStateSync;
    }

    public void setRetryDelayNetwork(TimeValue retryDelayNetwork) {
        this.retryDelayNetwork = retryDelayNetwork;
    }

    public void setActivityTimeout(TimeValue activityTimeout) {
        this.activityTimeout = activityTimeout;
    }

    public void setInternalActionTimeout(TimeValue internalActionTimeout) {
        this.internalActionTimeout = internalActionTimeout;
    }

    public void setInternalActionLongTimeout(TimeValue internalActionLongTimeout) {
        this.internalActionLongTimeout = internalActionLongTimeout;
    }

    private void setMaxBytesPerSec(ByteSizeValue maxBytesPerSec) {
        this.maxBytesPerSec = maxBytesPerSec;
        if (maxBytesPerSec.getBytes() <= 0) {
            rateLimiter = null;
        } else if (rateLimiter != null) {
            rateLimiter.setMBPerSec(maxBytesPerSec.getMbFrac());
        } else {
            rateLimiter = new SimpleRateLimiter(maxBytesPerSec.getMbFrac());
        }
    }

    public int getMaxConcurrentFileChunks() {
        return maxConcurrentFileChunks;
    }

    private void setMaxConcurrentFileChunks(int maxConcurrentFileChunks) {
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
    }

    public int getMaxConcurrentOperations() {
        return maxConcurrentOperations;
    }

    private void setMaxConcurrentOperations(int maxConcurrentOperations) {
        this.maxConcurrentOperations = maxConcurrentOperations;
    }

    public boolean getUseSnapshotsDuringRecovery() {
        return useSnapshotsDuringRecovery;
    }

    private void setUseSnapshotsDuringRecovery(boolean useSnapshotsDuringRecovery) {
        this.useSnapshotsDuringRecovery = useSnapshotsDuringRecovery;
    }

    public int getMaxConcurrentSnapshotFileDownloads() {
        return maxConcurrentSnapshotFileDownloads;
    }

    public void setMaxConcurrentSnapshotFileDownloads(int maxConcurrentSnapshotFileDownloads) {
        this.maxConcurrentSnapshotFileDownloads = maxConcurrentSnapshotFileDownloads;
    }
}
