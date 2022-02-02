/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.io.DiskIoBufferPool;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;

import java.nio.file.Path;

/*
 * Holds all the configuration that is used to create a {@link Translog}.
 * Once {@link Translog} has been created with this object, changes to this
 * object will affect the {@link Translog} instance.
 */
public final class TranslogConfig {

    public static final ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(1, ByteSizeUnit.MB);
    public static final ByteSizeValue EMPTY_TRANSLOG_BUFFER_SIZE = new ByteSizeValue(10, ByteSizeUnit.BYTES);
    private final BigArrays bigArrays;
    private final DiskIoBufferPool diskIoBufferPool;
    private final IndexSettings indexSettings;
    private final ShardId shardId;
    private final Path translogPath;
    private final ByteSizeValue bufferSize;

    /**
     * Creates a new TranslogConfig instance
     * @param shardId the shard ID this translog belongs to
     * @param translogPath the path to use for the transaction log files
     * @param indexSettings the index settings used to set internal variables
     * @param bigArrays a bigArrays instance used for temporarily allocating write operations
     */
    public TranslogConfig(ShardId shardId, Path translogPath, IndexSettings indexSettings, BigArrays bigArrays) {
        this(shardId, translogPath, indexSettings, bigArrays, DEFAULT_BUFFER_SIZE, DiskIoBufferPool.INSTANCE);
    }

    TranslogConfig(
        ShardId shardId,
        Path translogPath,
        IndexSettings indexSettings,
        BigArrays bigArrays,
        ByteSizeValue bufferSize,
        DiskIoBufferPool diskIoBufferPool
    ) {
        this.bufferSize = bufferSize;
        this.indexSettings = indexSettings;
        this.shardId = shardId;
        this.translogPath = translogPath;
        this.bigArrays = bigArrays;
        this.diskIoBufferPool = diskIoBufferPool;
    }

    /**
     * Returns the index indexSettings
     */
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Returns the shard ID this config is created for
     */
    public ShardId getShardId() {
        return shardId;
    }

    /**
     * Returns a BigArrays instance for this engine
     */
    public BigArrays getBigArrays() {
        return bigArrays;
    }

    /**
     * Returns the translog path for this engine
     */
    public Path getTranslogPath() {
        return translogPath;
    }

    /**
     * The translog buffer size. Default is {@code 8kb}
     */
    public ByteSizeValue getBufferSize() {
        return bufferSize;
    }

    /**
     * {@link DiskIoBufferPool} for this engine. Used to allow custom pools in tests, always returns
     * {@link DiskIoBufferPool#INSTANCE} in production.
     */
    public DiskIoBufferPool getDiskIoBufferPool() {
        return diskIoBufferPool;
    }
}
