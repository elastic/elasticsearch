/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.io.DiskIoBufferPool;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.ShardId;

import java.nio.file.Path;

/*
 * Holds all the configuration that is used to create a {@link Translog}.
 * Once {@link Translog} has been created with this object, changes to this
 * object will affect the {@link Translog} instance.
 */
public final class TranslogConfig {

    public static final ByteSizeValue DEFAULT_BUFFER_SIZE = ByteSizeValue.of(1, ByteSizeUnit.MB);
    public static final ByteSizeValue EMPTY_TRANSLOG_BUFFER_SIZE = ByteSizeValue.ofBytes(10);
    public static final OperationListener NOOP_OPERATION_LISTENER = (d, s, l) -> {};

    private final ShardId shardId;
    private final Path translogPath;
    private final IndexSettings indexSettings;
    private final BigArrays bigArrays;
    private final ByteSizeValue bufferSize;
    private final DiskIoBufferPool diskIoBufferPool;
    private final OperationListener operationListener;
    private final boolean fsync;

    /**
     * Creates a new TranslogConfig instance
     * @param shardId the shard ID this translog belongs to
     * @param translogPath the path to use for the transaction log files
     * @param indexSettings the index settings used to set internal variables
     * @param bigArrays a bigArrays instance used for temporarily allocating write operations
     */
    public TranslogConfig(ShardId shardId, Path translogPath, IndexSettings indexSettings, BigArrays bigArrays) {
        this(
            shardId,
            translogPath,
            indexSettings,
            bigArrays,
            DEFAULT_BUFFER_SIZE,
            DiskIoBufferPool.INSTANCE,
            NOOP_OPERATION_LISTENER,
            true
        );
    }

    public TranslogConfig(
        ShardId shardId,
        Path translogPath,
        IndexSettings indexSettings,
        BigArrays bigArrays,
        ByteSizeValue bufferSize,
        DiskIoBufferPool diskIoBufferPool,
        OperationListener operationListener
    ) {
        this(shardId, translogPath, indexSettings, bigArrays, bufferSize, diskIoBufferPool, operationListener, true);
    }

    public TranslogConfig(
        ShardId shardId,
        Path translogPath,
        IndexSettings indexSettings,
        BigArrays bigArrays,
        ByteSizeValue bufferSize,
        DiskIoBufferPool diskIoBufferPool,
        OperationListener operationListener,
        boolean fsync
    ) {
        this.bufferSize = bufferSize;
        this.indexSettings = indexSettings;
        this.shardId = shardId;
        this.translogPath = translogPath;
        this.bigArrays = bigArrays;
        this.diskIoBufferPool = diskIoBufferPool;
        this.operationListener = operationListener;
        this.fsync = fsync;
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

    public OperationListener getOperationListener() {
        return operationListener;
    }

    /**
     * @return true if translog writes need to be followed by fsync
     */
    public boolean fsync() {
        return fsync;
    }

    /**
     * @return {@code true} if the configuration allows the Translog files to exist, {@code false} otherwise. In the case there is no
     * translog, the shard is not writeable.
     */
    public boolean hasTranslog() {
        var compatibilityVersion = indexSettings.getIndexMetadata().getCompatibilityVersion();
        if (compatibilityVersion.before(IndexVersions.MINIMUM_COMPATIBLE) || indexSettings.getIndexMetadata().isSearchableSnapshot()) {
            return false;
        }
        return true;
    }
}
