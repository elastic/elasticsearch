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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 */
public class RecoverySettings extends AbstractComponent implements Closeable {

    public static final String INDICES_RECOVERY_FILE_CHUNK_SIZE = "indices.recovery.file_chunk_size";
    public static final String INDICES_RECOVERY_TRANSLOG_OPS = "indices.recovery.translog_ops";
    public static final String INDICES_RECOVERY_TRANSLOG_SIZE = "indices.recovery.translog_size";
    public static final String INDICES_RECOVERY_COMPRESS = "indices.recovery.compress";
    public static final String INDICES_RECOVERY_CONCURRENT_STREAMS = "indices.recovery.concurrent_streams";
    public static final String INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS = "indices.recovery.concurrent_small_file_streams";
    public static final String INDICES_RECOVERY_MAX_BYTES_PER_SEC = "indices.recovery.max_bytes_per_sec";

    /**
     * how long to wait before retrying after issues cause by cluster state syncing between nodes
     * i.e., local node is not yet known on remote node, remote shard not yet started etc.
     */
    public static final String INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC = "indices.recovery.retry_delay_state_sync";

    /** how long to wait before retrying after network related issues */
    public static final String INDICES_RECOVERY_RETRY_DELAY_NETWORK = "indices.recovery.retry_delay_network";

    /**
     * recoveries that don't show any activity for more then this interval will be failed.
     * defaults to `indices.recovery.internal_action_long_timeout`
     */
    public static final String INDICES_RECOVERY_ACTIVITY_TIMEOUT = "indices.recovery.recovery_activity_timeout";

    /** timeout value to use for requests made as part of the recovery process */
    public static final String INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT = "indices.recovery.internal_action_timeout";

    /**
     * timeout value to use for requests made as part of the recovery process that are expected to take long time.
     * defaults to twice `indices.recovery.internal_action_timeout`.
     */
    public static final String INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT = "indices.recovery.internal_action_long_timeout";


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
    public RecoverySettings(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);

        this.fileChunkSize = settings.getAsBytesSize(INDICES_RECOVERY_FILE_CHUNK_SIZE, new ByteSizeValue(512, ByteSizeUnit.KB));
        this.translogOps = settings.getAsInt(INDICES_RECOVERY_TRANSLOG_OPS, 1000);
        this.translogSize = settings.getAsBytesSize(INDICES_RECOVERY_TRANSLOG_SIZE, new ByteSizeValue(512, ByteSizeUnit.KB));
        this.compress = settings.getAsBoolean(INDICES_RECOVERY_COMPRESS, true);

        this.retryDelayStateSync = settings.getAsTime(INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC, TimeValue.timeValueMillis(500));
        // doesn't have to be fast as nodes are reconnected every 10s by default (see InternalClusterService.ReconnectToNodes)
        // and we want to give the master time to remove a faulty node
        this.retryDelayNetwork = settings.getAsTime(INDICES_RECOVERY_RETRY_DELAY_NETWORK, TimeValue.timeValueSeconds(5));

        this.internalActionTimeout = settings.getAsTime(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT, TimeValue.timeValueMinutes(15));
        this.internalActionLongTimeout = settings.getAsTime(INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT, new TimeValue(internalActionTimeout.millis() * 2));

        this.activityTimeout = settings.getAsTime(INDICES_RECOVERY_ACTIVITY_TIMEOUT,
                // default to the internalActionLongTimeout used as timeouts on RecoverySource
                internalActionLongTimeout
        );


        this.concurrentStreams = settings.getAsInt(INDICES_RECOVERY_CONCURRENT_STREAMS, 3);
        this.concurrentStreamPool = EsExecutors.newScaling("recovery_stream", 0, concurrentStreams, 60, TimeUnit.SECONDS,
                EsExecutors.daemonThreadFactory(settings, "[recovery_stream]"));
        this.concurrentSmallFileStreams = settings.getAsInt(INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS, 2);
        this.concurrentSmallFileStreamPool = EsExecutors.newScaling("small_file_recovery_stream", 0, concurrentSmallFileStreams, 60,
                TimeUnit.SECONDS, EsExecutors.daemonThreadFactory(settings, "[small_file_recovery_stream]"));

        this.maxBytesPerSec = settings.getAsBytesSize(INDICES_RECOVERY_MAX_BYTES_PER_SEC, new ByteSizeValue(40, ByteSizeUnit.MB));
        if (maxBytesPerSec.bytes() <= 0) {
            rateLimiter = null;
        } else {
            rateLimiter = new SimpleRateLimiter(maxBytesPerSec.mbFrac());
        }

        logger.debug("using max_bytes_per_sec[{}], concurrent_streams [{}], file_chunk_size [{}], translog_size [{}], translog_ops [{}], and compress [{}]",
                maxBytesPerSec, concurrentStreams, fileChunkSize, translogSize, translogOps, compress);

        nodeSettingsService.addListener(new ApplySettings());
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


    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            ByteSizeValue maxSizePerSec = settings.getAsBytesSize(INDICES_RECOVERY_MAX_BYTES_PER_SEC, RecoverySettings.this.maxBytesPerSec);
            if (!Objects.equals(maxSizePerSec, RecoverySettings.this.maxBytesPerSec)) {
                logger.info("updating [{}] from [{}] to [{}]", INDICES_RECOVERY_MAX_BYTES_PER_SEC, RecoverySettings.this.maxBytesPerSec, maxSizePerSec);
                RecoverySettings.this.maxBytesPerSec = maxSizePerSec;
                if (maxSizePerSec.bytes() <= 0) {
                    rateLimiter = null;
                } else if (rateLimiter != null) {
                    rateLimiter.setMBPerSec(maxSizePerSec.mbFrac());
                } else {
                    rateLimiter = new SimpleRateLimiter(maxSizePerSec.mbFrac());
                }
            }

            ByteSizeValue fileChunkSize = settings.getAsBytesSize(INDICES_RECOVERY_FILE_CHUNK_SIZE, RecoverySettings.this.fileChunkSize);
            if (!fileChunkSize.equals(RecoverySettings.this.fileChunkSize)) {
                logger.info("updating [indices.recovery.file_chunk_size] from [{}] to [{}]", RecoverySettings.this.fileChunkSize, fileChunkSize);
                RecoverySettings.this.fileChunkSize = fileChunkSize;
            }

            int translogOps = settings.getAsInt(INDICES_RECOVERY_TRANSLOG_OPS, RecoverySettings.this.translogOps);
            if (translogOps != RecoverySettings.this.translogOps) {
                logger.info("updating [indices.recovery.translog_ops] from [{}] to [{}]", RecoverySettings.this.translogOps, translogOps);
                RecoverySettings.this.translogOps = translogOps;
            }

            ByteSizeValue translogSize = settings.getAsBytesSize(INDICES_RECOVERY_TRANSLOG_SIZE, RecoverySettings.this.translogSize);
            if (!translogSize.equals(RecoverySettings.this.translogSize)) {
                logger.info("updating [indices.recovery.translog_size] from [{}] to [{}]", RecoverySettings.this.translogSize, translogSize);
                RecoverySettings.this.translogSize = translogSize;
            }

            boolean compress = settings.getAsBoolean(INDICES_RECOVERY_COMPRESS, RecoverySettings.this.compress);
            if (compress != RecoverySettings.this.compress) {
                logger.info("updating [indices.recovery.compress] from [{}] to [{}]", RecoverySettings.this.compress, compress);
                RecoverySettings.this.compress = compress;
            }

            int concurrentStreams = settings.getAsInt(INDICES_RECOVERY_CONCURRENT_STREAMS, RecoverySettings.this.concurrentStreams);
            if (concurrentStreams != RecoverySettings.this.concurrentStreams) {
                logger.info("updating [indices.recovery.concurrent_streams] from [{}] to [{}]", RecoverySettings.this.concurrentStreams, concurrentStreams);
                RecoverySettings.this.concurrentStreams = concurrentStreams;
                RecoverySettings.this.concurrentStreamPool.setMaximumPoolSize(concurrentStreams);
            }

            int concurrentSmallFileStreams = settings.getAsInt(INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS, RecoverySettings.this.concurrentSmallFileStreams);
            if (concurrentSmallFileStreams != RecoverySettings.this.concurrentSmallFileStreams) {
                logger.info("updating [indices.recovery.concurrent_small_file_streams] from [{}] to [{}]", RecoverySettings.this.concurrentSmallFileStreams, concurrentSmallFileStreams);
                RecoverySettings.this.concurrentSmallFileStreams = concurrentSmallFileStreams;
                RecoverySettings.this.concurrentSmallFileStreamPool.setMaximumPoolSize(concurrentSmallFileStreams);
            }

            RecoverySettings.this.retryDelayNetwork = maybeUpdate(RecoverySettings.this.retryDelayNetwork, settings, INDICES_RECOVERY_RETRY_DELAY_NETWORK);
            RecoverySettings.this.retryDelayStateSync = maybeUpdate(RecoverySettings.this.retryDelayStateSync, settings, INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC);
            RecoverySettings.this.activityTimeout = maybeUpdate(RecoverySettings.this.activityTimeout, settings, INDICES_RECOVERY_ACTIVITY_TIMEOUT);
            RecoverySettings.this.internalActionTimeout = maybeUpdate(RecoverySettings.this.internalActionTimeout, settings, INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT);
            RecoverySettings.this.internalActionLongTimeout = maybeUpdate(RecoverySettings.this.internalActionLongTimeout, settings, INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT);
        }

        private TimeValue maybeUpdate(final TimeValue currentValue, final Settings settings, final String key) {
            final TimeValue value = settings.getAsTime(key, currentValue);
            if (value.equals(currentValue)) {
                return currentValue;
            }
            logger.info("updating [] from [{}] to [{}]", key, currentValue, value);
            return value;
        }
    }
}
