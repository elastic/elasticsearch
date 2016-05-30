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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */


public class RecoveryTarget extends AbstractRefCounted implements RecoveryTargetHandler {

    private final ESLogger logger;

    private final static AtomicLong idGenerator = new AtomicLong();

    private final String RECOVERY_PREFIX = "recovery.";

    private final ShardId shardId;
    private final long recoveryId;
    private final IndexShard indexShard;
    private final DiscoveryNode sourceNode;
    private final String tempFilePrefix;
    private final Store store;
    private final RecoveryTargetService.RecoveryListener listener;

    private final AtomicBoolean finished = new AtomicBoolean();

    private final ConcurrentMap<String, IndexOutput> openIndexOutputs = ConcurrentCollections.newConcurrentMap();
    private final CancellableThreads cancellableThreads = new CancellableThreads();

    // last time this status was accessed
    private volatile long lastAccessTime = System.nanoTime();

    private final Map<String, String> tempFileNames = ConcurrentCollections.newConcurrentMap();

    public RecoveryTarget(IndexShard indexShard, DiscoveryNode sourceNode, RecoveryTargetService.RecoveryListener listener) {

        super("recovery_status");
        this.recoveryId = idGenerator.incrementAndGet();
        this.listener = listener;
        this.logger = Loggers.getLogger(getClass(), indexShard.indexSettings().getSettings(), indexShard.shardId());
        this.indexShard = indexShard;
        this.sourceNode = sourceNode;
        this.shardId = indexShard.shardId();
        this.tempFilePrefix = RECOVERY_PREFIX + indexShard.recoveryState().getTimer().startTime() + ".";
        this.store = indexShard.store();
        indexShard.recoveryStats().incCurrentAsTarget();
        // make sure the store is not released until we are done.
        store.incRef();
    }

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

    /** renames all temporary files to their true name, potentially overriding existing files */
    public void renameAllTempFiles() throws IOException {
        ensureRefCount();
        store.renameTempFilesSafe(tempFileNames);
    }

    /**
     * cancel the recovery. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     * <p>
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
                    cancellableThreads.cancel("failed recovery [" + ExceptionsHelper.stackTrace(e) + "]");
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
     * <p>
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
    }

    @Override
    public String toString() {
        return shardId + " [" + recoveryId + "]";
    }

    private void ensureRefCount() {
        if (refCount() <= 0) {
            throw new ElasticsearchException("RecoveryStatus is used but it's refcount is 0. Probably a mismatch between incRef/decRef " +
                    "calls");
        }
    }

    /*** Implementation of {@link RecoveryTargetHandler } */

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps) throws IOException {
        state().getTranslog().totalOperations(totalTranslogOps);
        indexShard().skipTranslogRecovery();
    }

    @Override
    public void finalizeRecovery() {
        indexShard().finalizeRecovery();
    }

    @Override
    public void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps) throws TranslogRecoveryPerformer
            .BatchOperationException {
        final RecoveryState.Translog translog = state().getTranslog();
        translog.totalOperations(totalTranslogOps);
        assert indexShard().recoveryState() == state();
        indexShard().performBatchRecovery(operations);
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames,
                                List<Long> phase1FileSizes,
                                List<String> phase1ExistingFileNames,
                                List<Long> phase1ExistingFileSizes,
                                int totalTranslogOps) {
        final RecoveryState.Index index = state().getIndex();
        for (int i = 0; i < phase1ExistingFileNames.size(); i++) {
            index.addFileDetail(phase1ExistingFileNames.get(i), phase1ExistingFileSizes.get(i), true);
        }
        for (int i = 0; i < phase1FileNames.size(); i++) {
            index.addFileDetail(phase1FileNames.get(i), phase1FileSizes.get(i), false);
        }
        state().getTranslog().totalOperations(totalTranslogOps);
        state().getTranslog().totalOperationsOnStart(totalTranslogOps);

    }

    @Override
    public void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException {
        state().getTranslog().totalOperations(totalTranslogOps);
        // first, we go and move files that were created with the recovery id suffix to
        // the actual names, its ok if we have a corrupted index here, since we have replicas
        // to recover from in case of a full cluster shutdown just when this code executes...
        renameAllTempFiles();
        final Store store = store();
        try {
            store.cleanupAndVerify("recovery CleanFilesRequestHandler", sourceMetaData);
        } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
            // this is a fatal exception at this stage.
            // this means we transferred files from the remote that have not be checksummed and they are
            // broken. We have to clean up this shard entirely, remove all files and bubble it up to the
            // source shard since this index might be broken there as well? The Source can handle this and checks
            // its content on disk if possible.
            try {
                try {
                    store.removeCorruptionMarker();
                } finally {
                    Lucene.cleanLuceneIndex(store.directory()); // clean up and delete all files
                }
            } catch (Throwable e) {
                logger.debug("Failed to clean lucene index", e);
                ex.addSuppressed(e);
            }
            RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
            fail(rfe, true);
            throw rfe;
        } catch (Exception ex) {
            RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
            fail(rfe, true);
            throw rfe;
        }
    }

    @Override
    public void writeFileChunk(StoreFileMetaData fileMetaData, long position, BytesReference content,
                               boolean lastChunk, int totalTranslogOps) throws IOException {
        final Store store = store();
        final String name = fileMetaData.name();
        state().getTranslog().totalOperations(totalTranslogOps);
        final RecoveryState.Index indexState = state().getIndex();
        IndexOutput indexOutput;
        if (position == 0) {
            indexOutput = openAndPutIndexOutput(name, fileMetaData, store);
        } else {
            indexOutput = getOpenIndexOutput(name);
        }
        if (content.hasArray() == false) {
            content = content.toBytesArray();
        }
        indexOutput.writeBytes(content.array(), content.arrayOffset(), content.length());
        indexState.addRecoveredBytesToFile(name, content.length());
        if (indexOutput.getFilePointer() >= fileMetaData.length() || lastChunk) {
            try {
                Store.verify(indexOutput);
            } finally {
                // we are done
                indexOutput.close();
            }
            final String temporaryFileName = getTempNameForFile(name);
            assert Arrays.asList(store.directory().listAll()).contains(temporaryFileName);
            store.directory().sync(Collections.singleton(temporaryFileName));
            IndexOutput remove = removeOpenIndexOutputs(name);
            assert remove == null || remove == indexOutput; // remove maybe null if we got finished
        }
    }
}
