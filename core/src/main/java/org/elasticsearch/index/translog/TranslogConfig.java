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

package org.elasticsearch.index.translog;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog.TranslogGeneration;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.file.Path;

/*
 * Holds all the configuration that is used to create a {@link Translog}.
 * Once {@link Translog} has been created with this object, changes to this
 * object will affect the {@link Translog} instance.
 */
public final class TranslogConfig {

    public static final String INDEX_TRANSLOG_DURABILITY = "index.translog.durability";
    public static final String INDEX_TRANSLOG_FS_TYPE = "index.translog.fs.type";
    public static final String INDEX_TRANSLOG_BUFFER_SIZE = "index.translog.fs.buffer_size";
    public static final String INDEX_TRANSLOG_SYNC_INTERVAL = "index.translog.sync_interval";
    public static final ByteSizeValue INACTIVE_SHARD_TRANSLOG_BUFFER = ByteSizeValue.parseBytesSizeValue("1kb", "INACTIVE_SHARD_TRANSLOG_BUFFER");

    private final TimeValue syncInterval;
    private final BigArrays bigArrays;
    private final ThreadPool threadPool;
    private final boolean syncOnEachOperation;
    private volatile int bufferSize;
    private volatile TranslogGeneration translogGeneration;
    private volatile Translog.Durabilty durabilty = Translog.Durabilty.REQUEST;
    private volatile TranslogWriter.Type type;
    private final Settings indexSettings;
    private final ShardId shardId;
    private final Path translogPath;

    /**
     * Creates a new TranslogConfig instance
     * @param shardId the shard ID this translog belongs to
     * @param translogPath the path to use for the transaction log files
     * @param indexSettings the index settings used to set internal variables
     * @param durabilty the default durability setting for the translog
     * @param bigArrays a bigArrays instance used for temporarily allocating write operations
     * @param threadPool a {@link ThreadPool} to schedule async sync durability
     */
    public TranslogConfig(ShardId shardId, Path translogPath, @IndexSettings Settings indexSettings, Translog.Durabilty durabilty, BigArrays bigArrays, @Nullable ThreadPool threadPool) {
        this.indexSettings = indexSettings;
        this.shardId = shardId;
        this.translogPath = translogPath;
        this.durabilty = durabilty;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.type = TranslogWriter.Type.fromString(indexSettings.get(INDEX_TRANSLOG_FS_TYPE, TranslogWriter.Type.BUFFERED.name()));
        this.bufferSize = (int) indexSettings.getAsBytesSize(INDEX_TRANSLOG_BUFFER_SIZE, ByteSizeValue.parseBytesSizeValue("64k", INDEX_TRANSLOG_BUFFER_SIZE)).bytes(); // Not really interesting, updated by IndexingMemoryController...

        syncInterval = indexSettings.getAsTime(INDEX_TRANSLOG_SYNC_INTERVAL, TimeValue.timeValueSeconds(5));
        if (syncInterval.millis() > 0 && threadPool != null) {
            syncOnEachOperation = false;
        } else if (syncInterval.millis() == 0) {
            syncOnEachOperation = true;
        } else {
            syncOnEachOperation = false;
        }
    }

    /**
     * Returns a {@link ThreadPool} to schedule async durability operations
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    /**
     * Returns the current durability mode of this translog.
     */
    public Translog.Durabilty getDurabilty() {
        return durabilty;
    }

    /**
     * Sets the current durability mode for the translog.
     */
    public void setDurabilty(Translog.Durabilty durabilty) {
        this.durabilty = durabilty;
    }

    /**
     * Returns the translog type
     */
    public TranslogWriter.Type getType() {
        return type;
    }

    /**
     * Sets the TranslogType for this Translog. The change will affect all subsequent translog files.
     */
    public void setType(TranslogWriter.Type type) {
        this.type = type;
    }

    /**
     * Returns <code>true</code> iff each low level operation shoudl be fsynced
     */
    public boolean isSyncOnEachOperation() {
        return syncOnEachOperation;
    }

    /**
     * Retruns the current translog buffer size.
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Sets the current buffer size - for setting a live setting use {@link Translog#updateBuffer(ByteSizeValue)}
     */
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    /**
     * Returns the current async fsync interval
     */
    public TimeValue getSyncInterval() {
        return syncInterval;
    }

    /**
     * Returns the current index settings
     */
    public Settings getIndexSettings() {
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
     * Returns the translog generation to open. If this is <code>null</code> a new translog is created. If non-null
     * the translog tries to open the given translog generation. The generation is treated as the last generation referenced
     * form already committed data. This means all operations that have not yet been committed should be in the translog
     * file referenced by this generation. The translog creation will fail if this generation can't be opened.
     */
    public TranslogGeneration getTranslogGeneration() {
        return translogGeneration;
    }

    /**
     * Set the generation to be opened. Use <code>null</code> to start with a fresh translog.
     * @see #getTranslogGeneration()
     */
    public void setTranslogGeneration(TranslogGeneration translogGeneration) {
        this.translogGeneration = translogGeneration;
    }
}
