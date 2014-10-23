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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */


public class RecoveryStatus extends AbstractRefCounted {

    private final ESLogger logger;

    private final static AtomicLong idGenerator = new AtomicLong();

    private final String RECOVERY_PREFIX = "recovery.";

    private final ShardId shardId;
    private final long recoveryId;
    private final InternalIndexShard indexShard;
    private final RecoveryState state;
    private final DiscoveryNode sourceNode;
    private final String tempFilePrefix;
    private final Store store;
    private final RecoveryTarget.RecoveryListener listener;

    private AtomicReference<Thread> waitingRecoveryThread = new AtomicReference<>();

    private final AtomicBoolean finished = new AtomicBoolean();

    private final ConcurrentMap<String, IndexOutput> openIndexOutputs = ConcurrentCollections.newConcurrentMap();
    private final Store.LegacyChecksums legacyChecksums = new Store.LegacyChecksums();

    public RecoveryStatus(InternalIndexShard indexShard, DiscoveryNode sourceNode, RecoveryState state, RecoveryTarget.RecoveryListener listener) {
        super("recovery_status");
        this.recoveryId = idGenerator.incrementAndGet();
        this.listener = listener;
        this.logger = Loggers.getLogger(getClass(), indexShard.indexSettings(), indexShard.shardId());
        this.indexShard = indexShard;
        this.sourceNode = sourceNode;
        this.shardId = indexShard.shardId();
        this.state = state;
        this.state.getTimer().startTime(System.currentTimeMillis());
        this.tempFilePrefix = RECOVERY_PREFIX + this.state.getTimer().startTime() + ".";
        this.store = indexShard.store();
        // make sure the store is not released until we are done.
        store.incRef();
    }

    private final Set<String> tempFileNames = ConcurrentCollections.newConcurrentSet();

    public long recoveryId() {
        return recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public InternalIndexShard indexShard() {
        ensureNotFinished();
        return indexShard;
    }

    public DiscoveryNode sourceNode() {
        return this.sourceNode;
    }

    public RecoveryState state() {
        return state;
    }

    public Store store() {
        ensureNotFinished();
        return store;
    }

    /** set a thread that should be interrupted if the recovery is canceled */
    public void setWaitingRecoveryThread(Thread thread) {
        waitingRecoveryThread.set(thread);
    }

    /**
     * clear the thread set by {@link #setWaitingRecoveryThread(Thread)}, making sure we
     * do not override another thread.
     */
    public void clearWaitingRecoveryThread(Thread threadToClear) {
        waitingRecoveryThread.compareAndSet(threadToClear, null);
    }

    public void stage(RecoveryState.Stage stage) {
        state.setStage(stage);
    }

    public RecoveryState.Stage stage() {
        return state.getStage();
    }

    public Store.LegacyChecksums legacyChecksums() {
        return legacyChecksums;
    }

    /** renames all temporary files to their true name, potentially overriding existing files */
    public void renameAllTempFiles() throws IOException {
        ensureNotFinished();
        Iterator<String> tempFileIterator = tempFileNames.iterator();
        final Directory directory = store.directory();
        while (tempFileIterator.hasNext()) {
            String tempFile = tempFileIterator.next();
            String origFile = originalNameForTempFile(tempFile);
            // first, go and delete the existing ones
            try {
                directory.deleteFile(origFile);
            } catch (NoSuchFileException e) {

            } catch (Throwable ex) {
                logger.debug("failed to delete file [{}]", ex, origFile);
            }
            // now, rename the files... and fail it it won't work
            store.renameFile(tempFile, origFile);
            // upon success, remove the temp file
            tempFileIterator.remove();
        }
    }

    /** cancel the recovery. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     *
     * if {@link #setWaitingRecoveryThread(Thread)} was used, the thread will be interrupted.
     */
    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            logger.debug("recovery canceled (reason: [{}])", reason);
            // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
            decRef();

            final Thread thread = waitingRecoveryThread.get();
            if (thread != null) {
                thread.interrupt();
            }
        }
    }

    /**
     * fail the recovery and call listener
     *
     * @param e exception that encapsulating the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     **/
    public void fail(RecoveryFailedException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                listener.onRecoveryFailure(state, e, sendShardFailure);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
        }
    }

    /** mark the current recovery as done */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            assert tempFileNames.isEmpty() : "not all temporary files are renamed";
            // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
            decRef();
            listener.onRecoveryDone(state);
        }
    }

    private String getTempNameForFile(String origFile) {
        return tempFilePrefix + origFile;
    }

    /** return true if the give file is a temporary file name issued by this recovery */
    private boolean isTempFile(String filename) {
        return tempFileNames.contains(filename);
    }

    public IndexOutput getOpenIndexOutput(String key) {
        ensureNotFinished();
        return openIndexOutputs.get(key);
    }

    /** returns the original file name for a temporary file name issued by this recovery */
    private String originalNameForTempFile(String tempFile) {
        if (!isTempFile(tempFile)) {
            throw new ElasticsearchException("[" + tempFile + "] is not a temporary file made by this recovery");
        }
        return tempFile.substring(tempFilePrefix.length());
    }

    /** remove and {@link org.apache.lucene.store.IndexOutput} for a given file. It is the caller's responsibility to close it */
    public IndexOutput removeOpenIndexOutputs(String name) {
        ensureNotFinished();
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
        ensureNotFinished();
        String tempFileName = getTempNameForFile(fileName);
        // add first, before it's created
        tempFileNames.add(tempFileName);
        IndexOutput indexOutput = store.createVerifyingOutput(tempFileName, IOContext.DEFAULT, metaData);
        openIndexOutputs.put(fileName, indexOutput);
        return indexOutput;
    }

    @Override
    protected void closeInternal() {
        try {
            // clean open index outputs
            Iterator<Entry<String, IndexOutput>> iterator = openIndexOutputs.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, IndexOutput> entry = iterator.next();
                IOUtils.closeWhileHandlingException(entry.getValue());
                iterator.remove();
            }
            // trash temporary files
            for (String file : tempFileNames) {
                logger.trace("cleaning temporary file [{}]", file);
                store.deleteQuiet(file);
            }
            legacyChecksums.clear();
        } finally {
            // free store. increment happens in constructor
            store.decRef();
        }
    }

    @Override
    public String toString() {
        return shardId + " [" + recoveryId + "]";
    }

    private void ensureNotFinished() {
        if (finished.get()) {
            throw new ElasticsearchException("RecoveryStatus is used after it was finished. Probably a mismatch between incRef/decRef calls");
        }
    }

}
