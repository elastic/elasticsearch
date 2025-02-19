/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE;

class ShardSyncState {

    private final ShardId shardId;
    private final long startingPrimaryTerm;
    private final LongSupplier currentPrimaryTerm;
    private final LongConsumer persistedSeqNoConsumer;
    private final ThreadContext threadContext;
    private final PriorityQueue<SyncListener> listeners = new PriorityQueue<>();
    private final TreeMap<Long, TranslogReplicator.BlobTranslogFile> translogFiles = new TreeMap<>();
    private long markedTranslogStartFile = -1;
    private long markedTranslogDeleteGeneration = -1;
    private volatile Translog.Location processedLocation = new Translog.Location(0, 0, 0);
    private volatile Translog.Location syncedLocation = new Translog.Location(0, 0, 0);
    private volatile boolean isClosed = false;

    ShardSyncState(
        ShardId shardId,
        long primaryTerm,
        LongSupplier currentPrimaryTerm,
        LongConsumer persistedSeqNoConsumer,
        ThreadContext threadContext
    ) {
        this.shardId = shardId;
        this.startingPrimaryTerm = primaryTerm;
        this.currentPrimaryTerm = currentPrimaryTerm;
        this.persistedSeqNoConsumer = persistedSeqNoConsumer;
        this.threadContext = threadContext;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public long getStartingPrimaryTerm() {
        return startingPrimaryTerm;
    }

    static AlreadyClosedException alreadyClosedException(ShardId shardId) {
        return new AlreadyClosedException("The translog for shard [" + shardId + "] is already closed.");
    }

    boolean syncNeeded() {
        return processedLocation.compareTo(syncedLocation) > 0;
    }

    boolean waitForAllSynced(ActionListener<Void> listener) {
        // Single volatile read
        Translog.Location processedLocationCopy = processedLocation;
        if (processedLocationCopy.compareTo(syncedLocation) > 0) {
            return ensureSynced(processedLocationCopy, listener);
        } else {
            if (isClosed) {
                listener.onFailure(alreadyClosedException(shardId));
            } else {
                listener.onResponse(null);
            }
            return true;
        }
    }

    boolean ensureSynced(Translog.Location location, ActionListener<Void> listener) {
        assert location.compareTo(processedLocation) <= 0;
        boolean completeListener = true;
        boolean alreadyClosed = false;
        if (location.compareTo(syncedLocation) > 0) {
            synchronized (listeners) {
                if (isClosed) {
                    alreadyClosed = true;
                } else if (location.compareTo(syncedLocation) > 0) {
                    ContextPreservingActionListener<Void> contextPreservingActionListener = ContextPreservingActionListener
                        .wrapPreservingContext(listener, threadContext);
                    listeners.add(new SyncListener(location, contextPreservingActionListener));
                    completeListener = false;
                }
            }
        }

        if (completeListener) {
            if (alreadyClosed) {
                listener.onFailure(alreadyClosedException(shardId));
            } else {
                listener.onResponse(null);
            }
            return true;
        } else {
            return false;
        }
    }

    public void markSyncStarting(long primaryTerm, TranslogReplicator.BlobTranslogFile translogFile) {
        // If the primary term changed this shard will eventually be closed and the listeners will be failed at that point, so we can
        // ignore them here.
        if (primaryTerm == currentPrimaryTerm.getAsLong()) {
            synchronized (translogFiles) {
                // Since this is call before initiating an upload, the marked translog start file should never be less than the recovery
                // start file
                assert translogFile.generation() >= markedTranslogStartFile;
                assert translogFiles.keySet().stream().allMatch(l -> l < translogFile.generation());
                if (isClosed == false) {
                    // Add if the shard is open. Ignore if the shard is closed. We cannot safely decrement as we don't know if the file will
                    // be needed for a different recovery
                    translogFiles.put(translogFile.generation(), translogFile);
                }
            }
        } else {
            // Just decrement since this was sync was generated in a different primary term
            translogFile.decRef();
        }
    }

    public boolean markSyncFinished(SyncMarker syncMarker) {
        // If the primary term changed this shard will eventually be closed and the listeners will be failed at that point, so we can
        // ignore them here.
        if (syncMarker.primaryTerm() == currentPrimaryTerm()) {
            assert syncMarker.location().compareTo(syncedLocation) > 0;
            // We mark the seqNos of persisted before exposing the synced location. This matches what we do in the TranlogWriter.
            // Some assertions in TransportVerifyShardBeforeCloseAction depend on the seqNos marked as persisted before the sync is exposed.
            syncMarker.syncedSeqNos().forEach(persistedSeqNoConsumer::accept);
            syncedLocation = syncMarker.location();
            return true;
        } else {
            return false;
        }
    }

    public void markCommitUploaded(long translogStartFile) {
        synchronized (translogFiles) {
            if (isClosed == false) {
                // TODO Find a better way to holow translog recovery in https://elasticco.atlassian.net/browse/ES-10718
                if (translogStartFile == HOLLOW_TRANSLOG_RECOVERY_START_FILE) {
                    for (var file : translogFiles.tailMap(markedTranslogStartFile, true).values()) {
                        file.markUnsafeForDelete(shardId);
                        file.decRef();
                    }
                    translogFiles.clear();
                    markedTranslogStartFile = -1;
                } else if (translogStartFile > markedTranslogStartFile) {
                    for (TranslogReplicator.BlobTranslogFile file : translogFiles.subMap(markedTranslogStartFile, translogStartFile)
                        .values()) {
                        file.decRef();
                    }
                    markedTranslogStartFile = translogStartFile;
                }
            }
        }
    }

    public void markTranslogDeleted(long translogGeneration) {
        synchronized (translogFiles) {
            if (isClosed == false) {
                markedTranslogDeleteGeneration = Math.max(translogGeneration, markedTranslogDeleteGeneration);
                if (markedTranslogDeleteGeneration == translogGeneration) {
                    // Remove all files prior to this generation. There is a possibility that lower generation files may still be
                    // referenced by other shards. For example imagine these translog files: [0, 1]. Shard A has not committed yet
                    // and has ops in generation 0. Shard B has committed with a translogStartFile=2 and has ops in [0, 1]. Shard B has
                    // decremented its references to 0 and 1. With this file 1 is deleted and cluster consistency passed. Shard B can now
                    // remove its reference to both file 0 and 1, even though file 0 is hanging around for Shard A.
                    NavigableMap<Long, TranslogReplicator.BlobTranslogFile> toRemove = translogFiles.headMap(translogGeneration, true);
                    toRemove.clear();
                } else {
                    assert translogFiles.headMap(translogGeneration, true).isEmpty();
                }
            }
        }
    }

    public long currentPrimaryTerm() {
        return currentPrimaryTerm.getAsLong();
    }

    void notifyListeners() {
        var toComplete = new ArrayList<ActionListener<Void>>();
        synchronized (listeners) {
            SyncListener listener;
            while ((listener = listeners.peek()) != null && syncedLocation.compareTo(listener.location) >= 0) {
                toComplete.add(listener);
                listeners.poll();
            }
        }
        ActionListener.onResponse(toComplete, null);
    }

    public void updateProcessedLocation(Translog.Location newProcessedLocation) {
        assert newProcessedLocation.compareTo(processedLocation) > 0;
        processedLocation = newProcessedLocation;
    }

    public TranslogMetadata.Directory createDirectory(long generation, long currentOperations) {
        final int[] referencedTranslogFileOffsets;
        long estimatedOps = currentOperations;
        synchronized (translogFiles) {
            referencedTranslogFileOffsets = new int[translogFiles.size()];
            int i = 0;
            for (TranslogReplicator.BlobTranslogFile referencedFile : translogFiles.values()) {
                estimatedOps += referencedFile.operations().get(shardId).totalOps();
                referencedTranslogFileOffsets[i] = Math.toIntExact(generation - referencedFile.generation());
                assert referencedTranslogFileOffsets[i] > 0 : generation + " " + referencedFile.generation();
                ++i;
            }
        }

        return new TranslogMetadata.Directory(estimatedOps, referencedTranslogFileOffsets);
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void close() {
        final ArrayList<ActionListener<Void>> toComplete;
        isClosed = true;
        synchronized (translogFiles) {
            // The relocation hand-off forces a flush while holding the operation permits to a clean relocation should fully release the
            // files. If we get here and there are still referenced files we must mark them as unsafe for delete. This is because this shard
            // may be closed exceptionally and the files will still be necessary for a recovery.
            translogFiles.tailMap(markedTranslogStartFile, true).forEach((ignored, blobTranslogFile) -> {
                blobTranslogFile.markUnsafeForDelete(shardId);
                blobTranslogFile.decRef();
            });
            translogFiles.clear();
        }
        synchronized (listeners) {
            toComplete = new ArrayList<>(listeners);
            listeners.clear();
        }

        ActionListener.onFailure(toComplete, alreadyClosedException(shardId));
    }

    private record SyncListener(Translog.Location location, ActionListener<Void> listener)
        implements
            ActionListener<Void>,
            Comparable<SyncListener> {

        @Override
        public void onResponse(Void unused) {
            listener.onResponse(unused);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public int compareTo(SyncListener o) {
            return location.compareTo(o.location);
        }
    }

    record SyncMarker(long primaryTerm, Translog.Location location, List<Long> syncedSeqNos) {}

}
