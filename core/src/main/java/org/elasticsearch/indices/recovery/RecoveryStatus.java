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

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */


public class RecoveryStatus extends AbstractRefCounted {

    private final ESLogger logger;

    private final static AtomicLong idGenerator = new AtomicLong();

    private final String RECOVERY_PREFIX = "recovery.";

    private final ShardId shardId;
    private final long recoveryId;
    private final IndexShard indexShard;
    private final DiscoveryNode sourceNode;
    private final String tempFilePrefix;
    private final Store store;
    private final RecoveryTarget.RecoveryListener listener;

    private final AtomicBoolean finished = new AtomicBoolean();

    private final ConcurrentMap<String, IndexOutput> openIndexOutputs = ConcurrentCollections.newConcurrentMap();
    private final Store.LegacyChecksums legacyChecksums = new Store.LegacyChecksums();

    private final CancellableThreads cancellableThreads = new CancellableThreads();

    // last time this status was accessed
    private volatile long lastAccessTime = System.nanoTime();

    public RecoveryStatus(IndexShard indexShard, DiscoveryNode sourceNode, RecoveryTarget.RecoveryListener listener) {

        super("recovery_status");
        this.recoveryId = idGenerator.incrementAndGet();
        this.listener = listener;
        this.logger = Loggers.getLogger(getClass(), indexShard.indexSettings(), indexShard.shardId());
        this.indexShard = indexShard;
        this.sourceNode = sourceNode;
        this.shardId = indexShard.shardId();
        this.tempFilePrefix = RECOVERY_PREFIX + indexShard.recoveryState().getTimer().startTime() + ".";
        this.store = indexShard.store();
        // make sure the store is not released until we are done.
        store.incRef();
        indexShard.recoveryStats().incCurrentAsTarget();
    }

    private final Map<String, String> tempFileNames = ConcurrentCollections.newConcurrentMap();

    public long recoveryId() {
        return recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public IndexShard indexShard() {
        ensureRefCount();
        return indexShard;
    }

    public DiscoveryNode sourceNode() {
        return this.sourceNode;
    }

    public RecoveryState state() {
        return indexShard.recoveryState();
    }

    public CancellableThreads CancellableThreads() {
        return cancellableThreads;
    }

    /** return the last time this RecoveryStatus was used (based on System.nanoTime() */
    public long lastAccessTime() {
        return lastAccessTime;
    }

    /** sets the lasAccessTime flag to now */
    public void setLastAccessTime() {
        lastAccessTime = System.nanoTime();
    }

    public Store store() {
        ensureRefCount();
        return store;
    }

    public RecoveryState.Stage stage() {
        return state().getStage();
    }

    public Store.LegacyChecksums legacyChecksums() {
        return legacyChecksums;
    }

    /** renames all temporary files to their true name, potentially overriding existing files */
    public void renameAllTempFiles() throws IOException {
        ensureRefCount();
        store.renameTempFilesSafe(tempFileNames);
    }

    /**
     * cancel the recovery. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     * <p/>
     * if {@link #CancellableThreads()} was used, the threads will be interrupted.
     */
    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("recovery canceled (reason: [{}])", reason);
                cancellableThreads.cancel(reason);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
        }
    }

    /**
     * fail the recovery and call listener
     *
     * @param e                exception that encapsulating the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     */
    public void fail(RecoveryFailedException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                listener.onRecoveryFailure(state(), e, sendShardFailure);
            } finally {
                try {
                    cancellableThreads.cancel("failed recovery [" + e.getMessage() + "]");
                } finally {
                    // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                    decRef();
                }
            }
        }
    }

    /** mark the current recovery as done */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            assert tempFileNames.isEmpty() : "not all temporary files are renamed";
            try {
                // this might still throw an exception ie. if the shard is CLOSED due to some other event.
                // it's safer to decrement the reference in a try finally here.
                indexShard.postRecovery("peer recovery done");
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
            listener.onRecoveryDone(state());
        }
    }

    /** Get a temporary name for the provided file name. */
    public String getTempNameForFile(String origFile) {
        return tempFilePrefix + origFile;
    }

    public IndexOutput getOpenIndexOutput(String key) {
        ensureRefCount();
        return openIndexOutputs.get(key);
    }

    /** remove and {@link org.apache.lucene.store.IndexOutput} for a given file. It is the caller's responsibility to close it */
    public IndexOutput removeOpenIndexOutputs(String name) {
        ensureRefCount();
        return openIndexOutputs.remove(name);
    }

    /**
     * Creates an {@link org.apache.lucene.store.IndexOutput} for the given file name. Note that the
     * IndexOutput actually point at a temporary file.
     * <p/>
     * Note: You can use {@link #getOpenIndexOutput(String)} with the same filename to retrieve the same IndexOutput
     * at a later stage
     */
    public IndexOutput openAndPutIndexOutput(String fileName, StoreFileMetaData metaData, Store store) throws IOException {
        ensureRefCount();
        String tempFileName = getTempNameForFile(fileName);
        if (tempFileNames.containsKey(tempFileName)) {
            throw new IllegalStateException("output for file [" + fileName + "] has already been created");
        }
        // add first, before it's created
        tempFileNames.put(tempFileName, fileName);
        IndexOutput indexOutput = store.createVerifyingOutput(tempFileName, metaData, IOContext.DEFAULT);
        openIndexOutputs.put(fileName, indexOutput);
        return indexOutput;
    }

    public void resetRecovery() throws IOException {
        cleanOpenFiles();
        indexShard().performRecoveryRestart();
    }

    @Override
    protected void closeInternal() {
        try {
            cleanOpenFiles();
        } finally {
            // free store. increment happens in constructor
            store.decRef();
            indexShard.recoveryStats().decCurrentAsTarget();
        }
    }

    protected void cleanOpenFiles() {
        // clean open index outputs
        Iterator<Entry<String, IndexOutput>> iterator = openIndexOutputs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, IndexOutput> entry = iterator.next();
            logger.trace("closing IndexOutput file [{}]", entry.getValue());
            try {
                entry.getValue().close();
            } catch (Throwable t) {
                logger.debug("error while closing recovery output [{}]", t, entry.getValue());
            }
            iterator.remove();
        }
        // trash temporary files
        for (String file : tempFileNames.keySet()) {
            logger.trace("cleaning temporary file [{}]", file);
            store.deleteQuiet(file);
        }
        legacyChecksums.clear();
    }

    @Override
    public String toString() {
        return shardId + " [" + recoveryId + "]";
    }

    private void ensureRefCount() {
        if (refCount() <= 0) {
            throw new ElasticsearchException("RecoveryStatus is used but it's refcount is 0. Probably a mismatch between incRef/decRef calls");
        }
    }

}
