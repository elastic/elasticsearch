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

import com.google.common.base.Objects;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.store.RateLimiter.SimpleRateLimiter;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 */
public class RecoverySettings extends AbstractComponent {

    public static final String INDICES_RECOVERY_FILE_CHUNK_SIZE = "indices.recovery.file_chunk_size";
    public static final String INDICES_RECOVERY_TRANSLOG_OPS = "indices.recovery.translog_ops";
    public static final String INDICES_RECOVERY_TRANSLOG_SIZE = "indices.recovery.translog_size";
    public static final String INDICES_RECOVERY_COMPRESS = "indices.recovery.compress";
    public static final String INDICES_RECOVERY_CONCURRENT_STREAMS = "indices.recovery.concurrent_streams";
    public static final String INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS = "indices.recovery.concurrent_small_file_streams";
    public static final String INDICES_RECOVERY_MAX_BYTES_PER_SEC = "indices.recovery.max_bytes_per_sec";

    public static final long SMALL_FILE_CUTOFF_BYTES = ByteSizeValue.parseBytesSizeValue("5mb").bytes();

    /**
     * Use {@link #INDICES_RECOVERY_MAX_BYTES_PER_SEC} instead
     */
    @Deprecated
    public static final String INDICES_RECOVERY_MAX_SIZE_PER_SEC = "indices.recovery.max_size_per_sec";

    public static final ByteSizeValue DEFAULT_FILE_CHUNK_SIZE = new ByteSizeValue(512, ByteSizeUnit.KB);
    public static final int DEFAULT_TRANSLOG_OPS = 1000;
    public static final ByteSizeValue DEFAULT_TRANSLOG_SIZE = new ByteSizeValue(512, ByteSizeUnit.KB);
    public static final boolean DEFAULT_COMPRESS = true;
    public static final int DEFAULT_CONCURRENT_STREAMS = 3;
    public static final int DEFAULT_CONCURRENT_SMALL_FILE_STREAMS = 2;
    public static final ByteSizeValue DEFAULT_MAX_BYTES_PER_SEC = new ByteSizeValue(50, ByteSizeUnit.MB);

    private volatile ByteSizeValue fileChunkSize = DEFAULT_FILE_CHUNK_SIZE;

    private volatile boolean compress = DEFAULT_COMPRESS;
    private volatile int translogOps = DEFAULT_TRANSLOG_OPS;
    private volatile ByteSizeValue translogSize = DEFAULT_TRANSLOG_SIZE;

    private volatile int concurrentStreams = DEFAULT_CONCURRENT_STREAMS;
    private volatile int concurrentSmallFileStreams = DEFAULT_CONCURRENT_SMALL_FILE_STREAMS;
    private final ThreadPoolExecutor concurrentStreamPool;
    private final ThreadPoolExecutor concurrentSmallFileStreamPool;

    private volatile ByteSizeValue maxBytesPerSec = DEFAULT_MAX_BYTES_PER_SEC;
    private volatile SimpleRateLimiter rateLimiter;

    @Inject
    public RecoverySettings(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.concurrentStreamPool = EsExecutors.newScaling(0, concurrentStreams, 60, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory(settings, "[recovery_stream]"));
        this.concurrentSmallFileStreamPool = EsExecutors.newScaling(0, concurrentSmallFileStreams, 60, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory(settings, "[small_file_recovery_stream]"));
        nodeSettingsService.addListener(new ApplySettings(settings));
        logger.debug("using max_bytes_per_sec[{}], concurrent_streams [{}], file_chunk_size [{}], translog_size [{}], translog_ops [{}], and compress [{}]",
                maxBytesPerSec, concurrentStreams, fileChunkSize, translogSize, translogOps, compress);


    }

    public void close() {
        concurrentStreamPool.shutdown();
        try {
            concurrentStreamPool.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // that's fine...
        }
        concurrentStreamPool.shutdownNow();
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

    class ApplySettings extends NodeSettingsService.Listener {
        public ApplySettings(Settings settings) {
            super(settings);
        }

        @Override
        public void onRefreshSettings(Settings settings) {
            ByteSizeValue maxSizePerSec = settings.getAsBytesSize(INDICES_RECOVERY_MAX_BYTES_PER_SEC, settings.getAsBytesSize(INDICES_RECOVERY_MAX_SIZE_PER_SEC, DEFAULT_MAX_BYTES_PER_SEC));
            if (!Objects.equal(maxSizePerSec, RecoverySettings.this.maxBytesPerSec)) {
                logger.info("updating [{}] from [{}] to [{}]", INDICES_RECOVERY_MAX_BYTES_PER_SEC, RecoverySettings.this.maxBytesPerSec, maxSizePerSec);
                RecoverySettings.this.maxBytesPerSec = maxSizePerSec;
                if (maxSizePerSec.bytes() <= 0) {
                    rateLimiter = null;
                } else if (rateLimiter != null) {
                    rateLimiter.setMbPerSec(maxSizePerSec.mbFrac());
                } else {
                    rateLimiter = new SimpleRateLimiter(maxSizePerSec.mbFrac());
                }
            }

            ByteSizeValue fileChunkSize = settings.getAsBytesSize(INDICES_RECOVERY_FILE_CHUNK_SIZE, settings.getAsBytesSize("index.shard.recovery.file_chunk_size", DEFAULT_FILE_CHUNK_SIZE));
            if (!fileChunkSize.equals(RecoverySettings.this.fileChunkSize)) {
                logger.info("updating [indices.recovery.file_chunk_size] from [{}] to [{}]", RecoverySettings.this.fileChunkSize, fileChunkSize);
                RecoverySettings.this.fileChunkSize = fileChunkSize;
            }

            int translogOps = settings.getAsInt(INDICES_RECOVERY_TRANSLOG_OPS, settings.getAsInt("index.shard.recovery.translog_ops", DEFAULT_TRANSLOG_OPS));
            if (translogOps != RecoverySettings.this.translogOps) {
                logger.info("updating [indices.recovery.translog_ops] from [{}] to [{}]", RecoverySettings.this.translogOps, translogOps);
                RecoverySettings.this.translogOps = translogOps;
            }

            ByteSizeValue translogSize = settings.getAsBytesSize(INDICES_RECOVERY_TRANSLOG_SIZE, settings.getAsBytesSize("index.shard.recovery.translog_size", DEFAULT_TRANSLOG_SIZE));
            if (!translogSize.equals(RecoverySettings.this.translogSize)) {
                logger.info("updating [indices.recovery.translog_size] from [{}] to [{}]", RecoverySettings.this.translogSize, translogSize);
                RecoverySettings.this.translogSize = translogSize;
            }

            boolean compress = settings.getAsBoolean(INDICES_RECOVERY_COMPRESS, DEFAULT_COMPRESS);
            if (compress != RecoverySettings.this.compress) {
                logger.info("updating [indices.recovery.compress] from [{}] to [{}]", RecoverySettings.this.compress, compress);
                RecoverySettings.this.compress = compress;
            }

            int concurrentStreams = settings.getAsInt(INDICES_RECOVERY_CONCURRENT_STREAMS, settings.getAsInt("index.shard.recovery.concurrent_streams", DEFAULT_CONCURRENT_STREAMS));
            if (concurrentStreams != RecoverySettings.this.concurrentStreams) {
                logger.info("updating [indices.recovery.concurrent_streams] from [{}] to [{}]", RecoverySettings.this.concurrentStreams, concurrentStreams);
                RecoverySettings.this.concurrentStreams = concurrentStreams;
                RecoverySettings.this.concurrentStreamPool.setMaximumPoolSize(concurrentStreams);
            }

            int concurrentSmallFileStreams = settings.getAsInt(INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS, settings.getAsInt("index.shard.recovery.concurrent_small_file_streams", DEFAULT_CONCURRENT_SMALL_FILE_STREAMS));
            if (concurrentSmallFileStreams != RecoverySettings.this.concurrentSmallFileStreams) {
                logger.info("updating [indices.recovery.concurrent_small_file_streams] from [{}] to [{}]", RecoverySettings.this.concurrentSmallFileStreams, concurrentSmallFileStreams);
                RecoverySettings.this.concurrentSmallFileStreams = concurrentSmallFileStreams;
                RecoverySettings.this.concurrentSmallFileStreamPool.setMaximumPoolSize(concurrentSmallFileStreams);
            }
        }
    }
}
