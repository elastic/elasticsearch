/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.CombinedRateLimiter;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Arrays;
import java.util.List;

/**
 * Container class for CCR settings.
 */
public final class CcrSettings {

    /**
     * Index setting for a following index.
     */
    public static final Setting<Boolean> CCR_FOLLOWING_INDEX_SETTING =
            Setting.boolSetting("index.xpack.ccr.following_index", false, Property.IndexScope, Property.InternalIndex);

    /**
     * Dynamic node setting for specifying the wait_for_timeout that the auto follow coordinator and shard follow task should be using.
     */
    public static final Setting<TimeValue> CCR_WAIT_FOR_METADATA_TIMEOUT = Setting.timeSetting(
        "ccr.wait_for_metadata_timeout", TimeValue.timeValueSeconds(60), Property.NodeScope, Property.Dynamic);

    /**
     * Dynamic node setting for specifying the wait_for_timeout that the auto follow coordinator should be using.
     * TODO: Deprecate and remove this setting
     */
    private static final Setting<TimeValue> CCR_AUTO_FOLLOW_WAIT_FOR_METADATA_TIMEOUT = Setting.timeSetting(
        "ccr.auto_follow.wait_for_metadata_timeout", CCR_WAIT_FOR_METADATA_TIMEOUT, Property.NodeScope, Property.Dynamic);

    /**
     * Max bytes a node can recover per second.
     */
    public static final Setting<ByteSizeValue> RECOVERY_MAX_BYTES_PER_SECOND =
        Setting.byteSizeSetting("ccr.indices.recovery.max_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB),
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    /**
     * File chunk size to send during recovery
     */
    public static final Setting<ByteSizeValue> RECOVERY_CHUNK_SIZE =
        Setting.byteSizeSetting("ccr.indices.recovery.chunk_size", new ByteSizeValue(1, ByteSizeUnit.MB),
            new ByteSizeValue(1, ByteSizeUnit.KB), new ByteSizeValue(1, ByteSizeUnit.GB), Setting.Property.Dynamic,
            Setting.Property.NodeScope);

    /**
     * Controls the maximum number of file chunk requests that are sent concurrently per recovery to the leader.
     */
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING =
        Setting.intSetting("ccr.indices.recovery.max_concurrent_file_chunks", 5, 1, 10, Property.Dynamic, Property.NodeScope);

    /**
     * The leader must open resources for a ccr recovery. If there is no activity for this interval of time,
     * the leader will close the restore session.
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING =
        Setting.timeSetting("ccr.indices.recovery.recovery_activity_timeout", TimeValue.timeValueSeconds(60),
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    /**
     * The timeout value to use for requests made as part of ccr recovery process.
     * */
    public static final Setting<TimeValue> INDICES_RECOVERY_ACTION_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("ccr.indices.recovery.internal_action_timeout", TimeValue.timeValueSeconds(60),
            Property.Dynamic, Property.NodeScope);

    /**
     * The settings defined by CCR.
     *
     * @return the settings
     */
    public static List<Setting<?>> getSettings() {
        return Arrays.asList(
                XPackSettings.CCR_ENABLED_SETTING,
                CCR_FOLLOWING_INDEX_SETTING,
                RECOVERY_MAX_BYTES_PER_SECOND,
                INDICES_RECOVERY_ACTION_TIMEOUT_SETTING,
                INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING,
                CCR_AUTO_FOLLOW_WAIT_FOR_METADATA_TIMEOUT,
                RECOVERY_CHUNK_SIZE,
                INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING,
                CCR_WAIT_FOR_METADATA_TIMEOUT);
    }

    private final CombinedRateLimiter ccrRateLimiter;
    private volatile TimeValue recoveryActivityTimeout;
    private volatile TimeValue recoveryActionTimeout;
    private volatile ByteSizeValue chunkSize;
    private volatile int maxConcurrentFileChunks;

    public CcrSettings(Settings settings, ClusterSettings clusterSettings) {
        this.recoveryActivityTimeout = INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.get(settings);
        this.recoveryActionTimeout = INDICES_RECOVERY_ACTION_TIMEOUT_SETTING.get(settings);
        this.ccrRateLimiter = new CombinedRateLimiter(RECOVERY_MAX_BYTES_PER_SECOND.get(settings));
        this.chunkSize = RECOVERY_CHUNK_SIZE.get(settings);
        this.maxConcurrentFileChunks = INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(RECOVERY_MAX_BYTES_PER_SECOND, this::setMaxBytesPerSec);
        clusterSettings.addSettingsUpdateConsumer(RECOVERY_CHUNK_SIZE, this::setChunkSize);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING, this::setMaxConcurrentFileChunks);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING, this::setRecoveryActivityTimeout);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_ACTION_TIMEOUT_SETTING, this::setRecoveryActionTimeout);
    }

    private void setChunkSize(ByteSizeValue chunkSize) {
        this.chunkSize = chunkSize;
    }

    private void setMaxConcurrentFileChunks(int maxConcurrentFileChunks) {
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
    }

    private void setMaxBytesPerSec(ByteSizeValue maxBytesPerSec) {
        ccrRateLimiter.setMBPerSec(maxBytesPerSec);
    }

    private void setRecoveryActivityTimeout(TimeValue recoveryActivityTimeout) {
        this.recoveryActivityTimeout = recoveryActivityTimeout;
    }

    private void setRecoveryActionTimeout(TimeValue recoveryActionTimeout) {
        this.recoveryActionTimeout = recoveryActionTimeout;
    }

    public ByteSizeValue getChunkSize() {
        return chunkSize;
    }

    public int getMaxConcurrentFileChunks() {
        return maxConcurrentFileChunks;
    }

    public CombinedRateLimiter getRateLimiter() {
        return ccrRateLimiter;
    }

    public TimeValue getRecoveryActivityTimeout() {
        return recoveryActivityTimeout;
    }

    public TimeValue getRecoveryActionTimeout() {
        return recoveryActionTimeout;
    }
}
