/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.store.RateLimiter.SimpleRateLimiter;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.settings.ClusterSettingsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 */
public class RecoverySettings extends AbstractComponent implements Closeable {

    public static final Setting<ByteSizeValue> INDICES_RECOVERY_FILE_CHUNK_SIZE_SETTING = Setting.byteSizeSetting("indices.recovery.file_chunk_size", new ByteSizeValue(512, ByteSizeUnit.KB), true, Setting.Scope.Cluster);
    public static final Setting<Integer> INDICES_RECOVERY_TRANSLOG_OPS_SETTING = Setting.intSetting("indices.recovery.translog_ops", 1000, true, Setting.Scope.Cluster);
    public static final Setting<ByteSizeValue> INDICES_RECOVERY_TRANSLOG_SIZE_SETTING = Setting.byteSizeSetting("indices.recovery.translog_size", new ByteSizeValue(512, ByteSizeUnit.KB), true, Setting.Scope.Cluster);
    public static final Setting<Boolean> INDICES_RECOVERY_COMPRESS_SETTING = Setting.boolSetting("indices.recovery.compress", true, true, Setting.Scope.Cluster);
    public static final Setting<Integer> INDICES_RECOVERY_CONCURRENT_STREAMS_SETTING = Setting.intSetting("indices.recovery.concurrent_streams", 3, true, Setting.Scope.Cluster);
    public static final Setting<Integer> INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS_SETTING = Setting.intSetting("indices.recovery.concurrent_small_file_streams", 2, true, Setting.Scope.Cluster);
    public static final Setting<ByteSizeValue> INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING = Setting.byteSizeSetting("indices.recovery.max_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB), true, Setting.Scope.Cluster);

    /**
     * how long to wait before retrying after issues cause by cluster state syncing between nodes
     * i.e., local node is not yet known on remote node, remote shard not yet started etc.
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING = Setting.positiveTimeSetting("indices.recovery.retry_delay_state_sync", TimeValue.timeValueMillis(500), true, Setting.Scope.Cluster);

    /** how long to wait before retrying after network related issues */
    public static final Setting<TimeValue> INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING = Setting.positiveTimeSetting("indices.recovery.retry_delay_network", TimeValue.timeValueSeconds(5), true, Setting.Scope.Cluster);

    /** timeout value to use for requests made as part of the recovery process */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING = Setting.positiveTimeSetting("indices.recovery.internal_action_timeout", TimeValue.timeValueMinutes(15), true, Setting.Scope.Cluster);

    /**
     * timeout value to use for requests made as part of the recovery process that are expected to take long time.
     * defaults to twice `indices.recovery.internal_action_timeout`.
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING = Setting.timeSetting("indices.recovery.internal_action_long_timeout", (s) -> TimeValue.timeValueMillis(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.get(s).millis() * 2).toString(), TimeValue.timeValueSeconds(0), true,  Setting.Scope.Cluster);

    /**
     * recoveries that don't show any activity for more then this interval will be failed.
     * defaults to `indices.recovery.internal_action_long_timeout`
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING = Setting.timeSetting("indices.recovery.recovery_activity_timeout", (s) -> INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING.getRaw(s) , TimeValue.timeValueSeconds(0), true,  Setting.Scope.Cluster);

    public static final long SMALL_FILE_CUTOFF_BYTES = ByteSizeValue.parseBytesSizeValue("5mb", "SMALL_FILE_CUTOFF_BYTES").bytes();

    private volatile ByteSizeValue fileChunkSize;

    private volatile boolean compress;
    private volatile int translogOps;
    private volatile ByteSizeValue translogSize;

    private volatile int concurrentStreams;
    private volatile int concurrentSmallFileStreams;
    private final ThreadPoolExecutor concurrentStreamPool;
    private final ThreadPoolExecutor concurrentSmallFileStreamPool;

    private volatile ByteSizeValue maxBytesPerSec;
    private volatile SimpleRateLimiter rateLimiter;
    private volatile TimeValue retryDelayStateSync;
    private volatile TimeValue retryDelayNetwork;
    private volatile TimeValue activityTimeout;
    private volatile TimeValue internalActionTimeout;
    private volatile TimeValue internalActionLongTimeout;


    @Inject
    public RecoverySettings(Settings settings, ClusterSettingsService clusterSettingsService) {
        super(settings);
        this.fileChunkSize = INDICES_RECOVERY_FILE_CHUNK_SIZE_SETTING.get(settings);
        this.translogOps = INDICES_RECOVERY_TRANSLOG_OPS_SETTING.get(settings);
        this.translogSize = INDICES_RECOVERY_TRANSLOG_SIZE_SETTING.get(settings);
        this.compress = INDICES_RECOVERY_COMPRESS_SETTING.get(settings);

        this.retryDelayStateSync = INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.get(settings);
        // doesn't have to be fast as nodes are reconnected every 10s by default (see InternalClusterService.ReconnectToNodes)
        // and we want to give the master time to remove a faulty node
        this.retryDelayNetwork = INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.get(settings);

        this.internalActionTimeout = INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.get(settings);
        this.internalActionLongTimeout = INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING.get(settings);

        this.activityTimeout = INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.get(settings);


        this.concurrentStreams = INDICES_RECOVERY_CONCURRENT_STREAMS_SETTING.get(settings);
        this.concurrentStreamPool = EsExecutors.newScaling("recovery_stream", 0, concurrentStreams, 60, TimeUnit.SECONDS,
                EsExecutors.daemonThreadFactory(settings, "[recovery_stream]"));
        this.concurrentSmallFileStreams = INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS_SETTING.get(settings);
        this.concurrentSmallFileStreamPool = EsExecutors.newScaling("small_file_recovery_stream", 0, concurrentSmallFileStreams, 60,
                TimeUnit.SECONDS, EsExecutors.daemonThreadFactory(settings, "[small_file_recovery_stream]"));

        this.maxBytesPerSec = INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.get(settings);
        if (maxBytesPerSec.bytes() <= 0) {
            rateLimiter = null;
        } else {
            rateLimiter = new SimpleRateLimiter(maxBytesPerSec.mbFrac());
        }

        logger.debug("using max_bytes_per_sec[{}], concurrent_streams [{}], file_chunk_size [{}], translog_size [{}], translog_ops [{}], and compress [{}]",
                maxBytesPerSec, concurrentStreams, fileChunkSize, translogSize, translogOps, compress);

        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_FILE_CHUNK_SIZE_SETTING, this::setFileChunkSize);
        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_TRANSLOG_OPS_SETTING, this::setTranslogOps);
        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_TRANSLOG_SIZE_SETTING, this::setTranslogSize);
        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_COMPRESS_SETTING, this::setCompress);
        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_CONCURRENT_STREAMS_SETTING, this::setConcurrentStreams);
        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS_SETTING, this::setConcurrentSmallFileStreams);
        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING, this::setMaxBytesPerSec);
        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING, this::setRetryDelayStateSync);
        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING, this::setRetryDelayNetwork);
        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING, this::setInternalActionTimeout);
        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING, this::setInternalActionLongTimeout);
        clusterSettingsService.addSettingsUpdateConsumer(INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING, this::setActivityTimeout);
    }

    @Override
    public void close() {
        ThreadPool.terminate(concurrentStreamPool, 1, TimeUnit.SECONDS);
        ThreadPool.terminate(concurrentSmallFileStreamPool, 1, TimeUnit.SECONDS);
    }

    public ByteSizeValue fileChunkSize() {
        return fileChunkSize;
    }

    public boolean compress() {
        return compress;
    }

    public int translogOps() {
        return translogOps;
    }

    public ByteSizeValue translogSize() {
        return translogSize;
    }

    public int concurrentStreams() {
        return concurrentStreams;
    }

    public ThreadPoolExecutor concurrentStreamPool() {
        return concurrentStreamPool;
    }

    public ThreadPoolExecutor concurrentSmallFileStreamPool() {
        return concurrentSmallFileStreamPool;
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

    public TimeValue internalActionLongTimeout() {
        return internalActionLongTimeout;
    }

    private void setFileChunkSize(ByteSizeValue fileChunkSize) {
        this.fileChunkSize = fileChunkSize;
    }

    private void setCompress(boolean compress) {
        this.compress = compress;
    }

    private void setTranslogOps(int translogOps) {
        this.translogOps = translogOps;
    }

    private void setTranslogSize(ByteSizeValue translogSize) {
        this.translogSize = translogSize;
    }

    private void setConcurrentStreams(int concurrentStreams) {
        this.concurrentStreams = concurrentStreams;
        concurrentStreamPool.setMaximumPoolSize(concurrentStreams);
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
        if (maxBytesPerSec.bytes() <= 0) {
            rateLimiter = null;
        } else if (rateLimiter != null) {
            rateLimiter.setMBPerSec(maxBytesPerSec.mbFrac());
        } else {
            rateLimiter = new SimpleRateLimiter(maxBytesPerSec.mbFrac());
        }
    }

    private void setConcurrentSmallFileStreams(int concurrentSmallFileStreams) {
        this.concurrentSmallFileStreams = concurrentSmallFileStreams;
        concurrentSmallFileStreamPool.setMaximumPoolSize(concurrentSmallFileStreams);
    }
}
