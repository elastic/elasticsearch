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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

/**
 * Represents a recovery where the current node is the target node of the recovery.
 * To track recoveries in a central place, instances of
 * this class are created through {@link RecoveriesCollection}.
 */
public class FileRecoveryTarget extends OpsRecoveryTarget implements FileRecoveryTargetHandler {

    private final String RECOVERY_PREFIX = "recovery.";

    private final String tempFilePrefix;
    private final Store store;

    private final ConcurrentMap<String, IndexOutput> openIndexOutputs =
        ConcurrentCollections.newConcurrentMap();

    private final Map<String, String> tempFileNames = ConcurrentCollections.newConcurrentMap();

    /**
     * Creates a new recovery target object that represents a recovery to the provided shard.
     *
     * @param indexShard                        local shard where we want to recover to
     * @param sourceNode                        source node of the recovery where we recover from
     * @param listener                          called when recovery is completed/failed
     */
    public FileRecoveryTarget(final IndexShard indexShard,
                              final DiscoveryNode sourceNode,
                              final PeerRecoveryTargetService.RecoveryListener listener) {
        super(indexShard, sourceNode, listener);
        this.tempFilePrefix = RECOVERY_PREFIX + UUIDs.base64UUID() + ".";
        this.store = indexShard.store();
        // make sure the store is not released until we are done.
        store.incRef();
        assert indexShard.commitStats() == null : "engine should be closed";
        assert indexShard.recoveryState().getStage() == RecoveryState.Stage.INDEX :
            indexShard.recoveryState().getStage();
    }


    @Override
    public FileRecoveryTarget retryCopy() {
        return new FileRecoveryTarget(indexShard(), sourceNode(), listener()
        );
    }

    public Store store() {
        ensureRefCount();
        return store;
    }
    /** renames all temporary files to their true name, potentially overriding existing files */
    public void renameAllTempFiles() throws IOException {
        ensureRefCount();
        store.renameTempFilesSafe(tempFileNames);
    }

    @Override
    protected void onResetRecovery() throws IOException {
        indexShard().performRecoveryRestart();
    }

    @Override
    public String startRecoveryActionName() {
        return PeerRecoverySourceService.Actions.START_FILE_RECOVERY;
    }

    /**
     * Prepares the target to receive translog operations, after all file have been copied
     *
     * @param totalTranslogOps total translog operations expected to be sent
     * @param maxUnsafeAutoIdTimestamp the max timestamp that is used to de-optimize documents with auto-generated IDs in the engine.
     * This is used to ensure we don't add duplicate documents when we assume an append only case based on auto-generated IDs
     */
    public void prepareForTranslogOperations(int totalTranslogOps, long maxUnsafeAutoIdTimestamp) throws IOException {
        state().getTranslog().totalOperations(totalTranslogOps);
        indexShard().skipTranslogRecovery(maxUnsafeAutoIdTimestamp);
    }

    @Override
    public StartRecoveryRequest createStartRecoveryRequest(Logger logger, DiscoveryNode localNode) {
        logger.trace("{} collecting local files for [{}]", shardId(), sourceNode());

        final Store.MetadataSnapshot metadataSnapshot = getStoreMetadataSnapshot(logger);
        logger.trace("{} local file count [{}]", shardId(), metadataSnapshot.size());

        return new StartFileRecoveryRequest(shardId(), sourceNode(), localNode, metadataSnapshot,
            recoveryId());
    }

    /**
     * Obtains a snapshot of the store metadata for the recovery target.
     */
    private Store.MetadataSnapshot getStoreMetadataSnapshot(Logger logger) {
        try {
            if (indexShard().indexSettings().isOnSharedFilesystem()) {
                // we are not going to copy any files, so don't bother listing files, potentially running into concurrency issues with the
                // primary changing files underneath us
                return Store.MetadataSnapshot.EMPTY;
            } else {
                return indexShard().snapshotStoreMetadata();
            }
        } catch (final org.apache.lucene.index.IndexNotFoundException e) {
            // happens on an empty folder. no need to log
            logger.trace("{} shard folder empty, recovering all files", this);
            return Store.MetadataSnapshot.EMPTY;
        } catch (final IOException e) {
            logger.warn("error while listing local files, recovering as if there are none", e);
            return Store.MetadataSnapshot.EMPTY;
        }
    }

    @Override
    protected boolean assertOnDone() {
        assert tempFileNames.isEmpty() : "not all temporary files are renamed";
        return true;
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

    @Override
    protected void doClose() {
        try {
            // clean open index outputs
            Iterator<Entry<String, IndexOutput>> iterator = openIndexOutputs.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, IndexOutput> entry = iterator.next();
                logger.trace("closing IndexOutput file [{}]", entry.getValue());
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    logger.debug(
                        (Supplier<?>) () -> new ParameterizedMessage("error while closing recovery output [{}]", entry.getValue()), e);
                }
                iterator.remove();
            }
            // trash temporary files
            for (String file : tempFileNames.keySet()) {
                logger.trace("cleaning temporary file [{}]", file);
                store.deleteQuiet(file);
            }
        } finally {
            // free store. increment happens in constructor
            store.decRef();
        }
    }

    /**
     * Notifies the target of the files it is going to receive
     */
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

    /**
     * After all source files has been sent over, this command is sent to the target so it can clean any local
     * files that are not part of the source store
     * @param totalTranslogOps an update number of translog operations that will be replayed later on
     * @param sourceMetaData meta data of the source store
     */
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
            } catch (Exception e) {
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

    /** writes a partial file chunk to the target store */
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
        BytesRefIterator iterator = content.iterator();
        BytesRef scratch;
        while((scratch = iterator.next()) != null) { // we iterate over all pages - this is a 0-copy for all core impls
            indexOutput.writeBytes(scratch.bytes, scratch.offset, scratch.length);
        }
        indexState.addRecoveredBytesToFile(name, content.length());
        if (indexOutput.getFilePointer() >= fileMetaData.length() || lastChunk) {
            try {
                Store.verify(indexOutput);
            } finally {
                // we are done
                indexOutput.close();
            }
            final String temporaryFileName = getTempNameForFile(name);
            assert Arrays.asList(store.directory().listAll()).contains(temporaryFileName) :
                "expected: [" + temporaryFileName + "] in " + Arrays.toString(store.directory().listAll());
            store.directory().sync(Collections.singleton(temporaryFileName));
            IndexOutput remove = removeOpenIndexOutputs(name);
            assert remove == null || remove == indexOutput; // remove maybe null if we got finished
        }
    }
}
