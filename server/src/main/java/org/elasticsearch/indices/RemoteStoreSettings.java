/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.indices;


import org.elasticsearch.common.annotation.PublicApi;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.remote.RemoteStoreEnums;

/**
 * Settings for remote store
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public class RemoteStoreSettings {
    private static final int MIN_CLUSTER_REMOTE_MAX_TRANSLOG_READERS = 100;

    /**
     * Used to specify the default translog buffer interval for remote store backed indexes.
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.remote_store.translog.buffer_interval",
        IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        IndexSettings.MINIMUM_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Controls minimum number of metadata files to keep in remote segment store.
     * {@code value < 1} will disable deletion of stale segment metadata files.
     */
    public static final Setting<Integer> CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING = Setting.intSetting(
        "cluster.remote_store.index.segment_metadata.retention.max_count",
        10,
        -1,
        v -> {
            if (v == 0) {
                throw new IllegalArgumentException(
                    "Value 0 is not allowed for this setting as it would delete all the data from remote segment store"
                );
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Controls timeout value while uploading translog and checkpoint files to remote translog
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.translog.transfer_timeout",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This setting is used to set the remote store blob store path type strategy. This setting is effective only for
     * remote store enabled cluster.
     */
    public static final Setting<RemoteStoreEnums.PathType> CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING = new Setting<>(
        "cluster.remote_store.index.path.type",
        RemoteStoreEnums.PathType.FIXED.toString(),
        RemoteStoreEnums.PathType::parseString,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This setting is used to disable uploading translog.ckp file as metadata to translog.tlog. This setting is effective only for
     * repositories that supports metadata read and write with metadata and is applicable for only remote store enabled clusters.
     */
    @ExperimentalApi
    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_TRANSLOG_METADATA = Setting.boolSetting(
        "cluster.remote_store.index.translog.translog_metadata",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This setting is used to set the remote store blob store path hash algorithm strategy. This setting is effective only for
     * remote store enabled cluster. This setting will come to effect if the {@link #CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING}
     * is either {@code HASHED_PREFIX} or {@code HASHED_INFIX}.
     */

    public static final Setting<RemoteStoreEnums.PathHashAlgorithm> CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING = new Setting<>(
        "cluster.remote_store.index.path.hash_algorithm",
        RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString(),
        RemoteStoreEnums.PathHashAlgorithm::parseString,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Controls the maximum referenced remote translog files. If breached the shard will be flushed.
     */
    public static final Setting<Integer> CLUSTER_REMOTE_MAX_TRANSLOG_READERS = Setting.intSetting(
        "cluster.remote_store.translog.max_readers",
        1000,
        -1,
        v -> {
            if (v != -1 && v < MIN_CLUSTER_REMOTE_MAX_TRANSLOG_READERS) {
                throw new IllegalArgumentException("Cannot set value lower than " + MIN_CLUSTER_REMOTE_MAX_TRANSLOG_READERS);
            }
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Controls timeout value while uploading segment files to remote segment store
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.segment.transfer_timeout",
        TimeValue.timeValueMinutes(30),
        TimeValue.timeValueMinutes(10),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile TimeValue clusterRemoteTranslogBufferInterval;
    private volatile int minRemoteSegmentMetadataFiles;
    private volatile TimeValue clusterRemoteTranslogTransferTimeout;
    private volatile TimeValue clusterRemoteSegmentTransferTimeout;
    private volatile RemoteStoreEnums.PathType pathType;
    private volatile RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm;
    private volatile int maxRemoteTranslogReaders;
    private volatile boolean isTranslogMetadataEnabled;

    public RemoteStoreSettings(Settings settings, ClusterSettings clusterSettings) {
        clusterRemoteTranslogBufferInterval = CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING,
            this::setClusterRemoteTranslogBufferInterval
        );

        minRemoteSegmentMetadataFiles = CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING,
            this::setMinRemoteSegmentMetadataFiles
        );

        clusterRemoteTranslogTransferTimeout = CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING,
            this::setClusterRemoteTranslogTransferTimeout
        );

        pathType = clusterSettings.get(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING, this::setPathType);

        isTranslogMetadataEnabled = clusterSettings.get(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA, this::setTranslogMetadataEnabled);

        pathHashAlgorithm = clusterSettings.get(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING, this::setPathHashAlgorithm);

        maxRemoteTranslogReaders = CLUSTER_REMOTE_MAX_TRANSLOG_READERS.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_MAX_TRANSLOG_READERS, this::setMaxRemoteTranslogReaders);

        clusterRemoteSegmentTransferTimeout = CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING,
            this::setClusterRemoteSegmentTransferTimeout
        );
    }

    public TimeValue getClusterRemoteTranslogBufferInterval() {
        return clusterRemoteTranslogBufferInterval;
    }

    private void setClusterRemoteTranslogBufferInterval(TimeValue clusterRemoteTranslogBufferInterval) {
        this.clusterRemoteTranslogBufferInterval = clusterRemoteTranslogBufferInterval;
    }

    private void setMinRemoteSegmentMetadataFiles(int minRemoteSegmentMetadataFiles) {
        this.minRemoteSegmentMetadataFiles = minRemoteSegmentMetadataFiles;
    }

    public int getMinRemoteSegmentMetadataFiles() {
        return this.minRemoteSegmentMetadataFiles;
    }

    public TimeValue getClusterRemoteTranslogTransferTimeout() {
        return clusterRemoteTranslogTransferTimeout;
    }

    public TimeValue getClusterRemoteSegmentTransferTimeout() {
        return clusterRemoteSegmentTransferTimeout;
    }

    private void setClusterRemoteTranslogTransferTimeout(TimeValue clusterRemoteTranslogTransferTimeout) {
        this.clusterRemoteTranslogTransferTimeout = clusterRemoteTranslogTransferTimeout;
    }

    private void setClusterRemoteSegmentTransferTimeout(TimeValue clusterRemoteSegmentTransferTimeout) {
        this.clusterRemoteSegmentTransferTimeout = clusterRemoteSegmentTransferTimeout;
    }

    public RemoteStoreEnums.PathType getPathType() {
        return pathType;
    }

    public RemoteStoreEnums.PathHashAlgorithm getPathHashAlgorithm() {
        return pathHashAlgorithm;
    }

    private void setPathType(RemoteStoreEnums.PathType pathType) {
        this.pathType = pathType;
    }

    private void setTranslogMetadataEnabled(boolean isTranslogMetadataEnabled) {
        this.isTranslogMetadataEnabled = isTranslogMetadataEnabled;
    }

    public boolean isTranslogMetadataEnabled() {
        return isTranslogMetadataEnabled;
    }

    private void setPathHashAlgorithm(RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm) {
        this.pathHashAlgorithm = pathHashAlgorithm;
    }

    public int getMaxRemoteTranslogReaders() {
        return maxRemoteTranslogReaders;
    }

    private void setMaxRemoteTranslogReaders(int maxRemoteTranslogReaders) {
        this.maxRemoteTranslogReaders = maxRemoteTranslogReaders;
    }
}
