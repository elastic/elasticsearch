/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;

public class StatelessThreadPools {

    // Thread pool names are defined in the BlobStoreRepository because we need to verify there that no requests are running on other pools.
    public static final String SHARD_READ_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_READ_THREAD_NAME;
    public static final String SHARD_READ_THREAD_POOL_SETTING = "stateless." + SHARD_READ_THREAD_POOL + "_thread_pool";
    public static final String TRANSLOG_THREAD_POOL = BlobStoreRepository.STATELESS_TRANSLOG_THREAD_NAME;
    public static final String TRANSLOG_THREAD_POOL_SETTING = "stateless." + TRANSLOG_THREAD_POOL + "_thread_pool";
    public static final String SHARD_WRITE_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_WRITE_THREAD_NAME;
    public static final String SHARD_WRITE_THREAD_POOL_SETTING = "stateless." + SHARD_WRITE_THREAD_POOL + "_thread_pool";
    public static final String CLUSTER_STATE_READ_WRITE_THREAD_POOL = BlobStoreRepository.STATELESS_CLUSTER_STATE_READ_WRITE_THREAD_NAME;
    public static final String CLUSTER_STATE_READ_WRITE_THREAD_POOL_SETTING = "stateless."
        + CLUSTER_STATE_READ_WRITE_THREAD_POOL
        + "_thread_pool";
    public static final String GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL = "stateless_get_vbcc_chunk";
    public static final String GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING = "stateless."
        + GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL
        + "_thread_pool";
    public static final String FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL = "stateless_fill_vbcc_cache";
    public static final String FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING = "stateless."
        + FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        + "_thread_pool";
    public static final String PREWARM_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_PREWARMING_THREAD_NAME;
    public static final String PREWARM_THREAD_POOL_SETTING = "stateless." + PREWARM_THREAD_POOL + "_thread_pool";
    public static final String UPLOAD_PREWARM_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_UPLOAD_PREWARMING_THREAD_NAME;
    public static final String UPLOAD_PREWARM_THREAD_POOL_SETTING = "stateless." + UPLOAD_PREWARM_THREAD_POOL + "_thread_pool";

    public static final String BLOB_COPY_THREAD_POOL = BlobStoreRepository.STATELESS_BLOB_COPY_THREAD_NAME;
    public static final String BLOB_COPY_THREAD_POOL_SETTING = "stateless." + BLOB_COPY_THREAD_POOL + "_thread_pool";

    public record ThreadPoolConfig(
        int shardReadMaxThreads,
        int translogCoreThreads,
        int translogMaxThreads,
        int shardWriteCoreThreads,
        int shardWriteMaxThreads,
        int clusterStateReadWriteCoreThreads,
        int clusterStateReadWriteMaxThreads,
        int getVirtualBatchedCompoundCommitChunkCoreThreads,
        int getVirtualBatchedCompoundCommitChunkMaxThreads,
        int fillVirtualBatchedCompoundCommitCacheCoreThreads,
        int fillVirtualBatchedCompoundCommitCacheMaxThreads,
        int prewarmMaxThreads,
        int uploadPrewarmCoreThreads,
        int uploadPrewarmMaxThreads,
        int blobCopyCoreThreads,
        int blobCopyMaxThreads
    ) {

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private int shardReadMaxThreads;
            private int translogCoreThreads;
            private int translogMaxThreads;
            private int shardWriteCoreThreads;
            private int shardWriteMaxThreads;
            private int clusterStateReadWriteCoreThreads;
            private int clusterStateReadWriteMaxThreads;
            private int getVirtualBatchedCompoundCommitChunkCoreThreads;
            private int getVirtualBatchedCompoundCommitChunkMaxThreads;
            private int fillVirtualBatchedCompoundCommitCacheCoreThreads;
            private int fillVirtualBatchedCompoundCommitCacheMaxThreads;
            private int prewarmMaxThreads;
            private int uploadPrewarmCoreThreads;
            private int uploadPrewarmMaxThreads;
            private int blobCopyCoreThreads;
            private int blobCopyMaxThreads;

            private Builder() {}

            public Builder shardReadMaxThreads(int maxThreads) {
                this.shardReadMaxThreads = maxThreads;
                return this;
            }

            public Builder translogThreads(int coreThreads, int maxThreads) {
                this.translogCoreThreads = coreThreads;
                this.translogMaxThreads = maxThreads;
                return this;
            }

            public Builder shardWriteThreads(int coreThreads, int maxThreads) {
                this.shardWriteCoreThreads = coreThreads;
                this.shardWriteMaxThreads = maxThreads;
                return this;
            }

            public Builder clusterStateReadWriteThreads(int coreThreads, int maxThreads) {
                this.clusterStateReadWriteCoreThreads = coreThreads;
                this.clusterStateReadWriteMaxThreads = maxThreads;
                return this;
            }

            public Builder getVirtualBatchedCompoundCommitChunkThreads(int coreThreads, int maxThreads) {
                this.getVirtualBatchedCompoundCommitChunkCoreThreads = coreThreads;
                this.getVirtualBatchedCompoundCommitChunkMaxThreads = maxThreads;
                return this;
            }

            public Builder fillVirtualBatchedCompoundCommitCacheThreads(int coreThreads, int maxThreads) {
                this.fillVirtualBatchedCompoundCommitCacheCoreThreads = coreThreads;
                this.fillVirtualBatchedCompoundCommitCacheMaxThreads = maxThreads;
                return this;
            }

            public Builder prewarmMaxThreads(int maxThreads) {
                this.prewarmMaxThreads = maxThreads;
                return this;
            }

            public Builder uploadPrewarmThreads(int coreThreads, int maxThreads) {
                this.uploadPrewarmCoreThreads = coreThreads;
                this.uploadPrewarmMaxThreads = maxThreads;
                return this;
            }

            public Builder blobCopyThreads(int coreThreads, int maxThreads) {
                this.blobCopyCoreThreads = coreThreads;
                this.blobCopyMaxThreads = maxThreads;
                return this;
            }

            public ThreadPoolConfig build() {
                return new ThreadPoolConfig(
                    shardReadMaxThreads,
                    translogCoreThreads,
                    translogMaxThreads,
                    shardWriteCoreThreads,
                    shardWriteMaxThreads,
                    clusterStateReadWriteCoreThreads,
                    clusterStateReadWriteMaxThreads,
                    getVirtualBatchedCompoundCommitChunkCoreThreads,
                    getVirtualBatchedCompoundCommitChunkMaxThreads,
                    fillVirtualBatchedCompoundCommitCacheCoreThreads,
                    fillVirtualBatchedCompoundCommitCacheMaxThreads,
                    prewarmMaxThreads,
                    uploadPrewarmCoreThreads,
                    uploadPrewarmMaxThreads,
                    blobCopyCoreThreads,
                    blobCopyMaxThreads
                );
            }
        }
    }

    public static ExecutorBuilder<?>[] statelessExecutorBuilders(ThreadPoolConfig config) {
        return new ExecutorBuilder<?>[] {
            new ScalingExecutorBuilder(
                SHARD_READ_THREAD_POOL,
                4,
                config.shardReadMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                SHARD_READ_THREAD_POOL_SETTING,
                EsExecutors.TaskTrackingConfig.builder().trackOngoingTasks().trackExecutionTime(0.3).build(),
                EsExecutors.HotThreadsOnLargeQueueConfig.DISABLED
            ),
            new ScalingExecutorBuilder(
                TRANSLOG_THREAD_POOL,
                config.translogCoreThreads,
                config.translogMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                TRANSLOG_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                SHARD_WRITE_THREAD_POOL,
                config.shardWriteCoreThreads,
                config.shardWriteMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                SHARD_WRITE_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                CLUSTER_STATE_READ_WRITE_THREAD_POOL,
                config.clusterStateReadWriteCoreThreads,
                config.clusterStateReadWriteMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                CLUSTER_STATE_READ_WRITE_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL,
                config.getVirtualBatchedCompoundCommitChunkCoreThreads,
                config.getVirtualBatchedCompoundCommitChunkMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL,
                config.fillVirtualBatchedCompoundCommitCacheCoreThreads,
                config.fillVirtualBatchedCompoundCommitCacheMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                PREWARM_THREAD_POOL,
                // these threads use a sizeable thread-local direct buffer which might take a while to GC, so we prefer to keep some idle
                // threads around to reduce churn and re-use the existing buffers more
                config.prewarmMaxThreads / 2,
                config.prewarmMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                PREWARM_THREAD_POOL_SETTING,
                EsExecutors.TaskTrackingConfig.builder().trackOngoingTasks().trackExecutionTime(0.3).build(),
                EsExecutors.HotThreadsOnLargeQueueConfig.DISABLED
            ),
            new ScalingExecutorBuilder(
                UPLOAD_PREWARM_THREAD_POOL,
                config.uploadPrewarmCoreThreads,
                config.uploadPrewarmMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                UPLOAD_PREWARM_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                BLOB_COPY_THREAD_POOL,
                config.blobCopyCoreThreads,
                config.blobCopyMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                BLOB_COPY_THREAD_POOL_SETTING
            ) };
    }
}
