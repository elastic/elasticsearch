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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

class ShardSyncState {

    private final ShardId shardId;
    private final long startingPrimaryTerm;
    private final LongSupplier currentPrimaryTerm;
    private final LongConsumer persistedSeqNoConsumer;
    private final ThreadContext threadContext;
    private final BigArrays bigArrays;
    private final PriorityQueue<SyncListener> listeners = new PriorityQueue<>();
    private final PriorityQueue<TranslogReplicator.BlobTranslogFile> referencedTranslogFiles = new PriorityQueue<>();
    private long markedTranslogStartFile = -1;
    private volatile Translog.Location processedLocation = new Translog.Location(0, 0, 0);
    private volatile Translog.Location syncedLocation = new Translog.Location(0, 0, 0);
    private final Object bufferLock = new Object();
    // This resets to 0 after a recovery. However, this is fine because we will always force a flush prior to startig new indexing
    // operations meaning that the translog start file will be marked.
    private long shardTranslogGeneration = 0;
    private BufferState bufferState = null;
    private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);

    ShardSyncState(
        ShardId shardId,
        long primaryTerm,
        LongSupplier currentPrimaryTerm,
        LongConsumer persistedSeqNoConsumer,
        ThreadContext threadContext,
        BigArrays bigArrays
    ) {
        this.shardId = shardId;
        this.startingPrimaryTerm = primaryTerm;
        this.currentPrimaryTerm = currentPrimaryTerm;
        this.persistedSeqNoConsumer = persistedSeqNoConsumer;
        this.threadContext = threadContext;
        this.bigArrays = bigArrays;
    }

    static AlreadyClosedException alreadyClosedException(ShardId shardId) {
        return new AlreadyClosedException("The translog for shard [" + shardId + "] is already closed.");
    }

    boolean syncNeeded() {
        return processedLocation.compareTo(syncedLocation) > 0;
    }

    void waitForAllSynced(ActionListener<Void> listener) {
        // Single volatile read
        Translog.Location processedLocationCopy = processedLocation;
        if (processedLocationCopy.compareTo(syncedLocation) > 0) {
            ensureSynced(processedLocationCopy, listener);
        } else {
            if (state.get() != State.OPEN) {
                listener.onFailure(alreadyClosedException(shardId));
            } else {
                listener.onResponse(null);
            }
        }
    }

    void ensureSynced(Translog.Location location, ActionListener<Void> listener) {
        boolean completeListener = true;
        boolean alreadyClosed = false;
        if (location.compareTo(syncedLocation) > 0) {
            synchronized (listeners) {
                if (state.get() != State.OPEN) {
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
        }
    }

    public void markSyncStarting(long primaryTerm, TranslogReplicator.BlobTranslogFile translogFile) {
        // If the primary term changed this shard will eventually be closed and the listeners will be failed at that point, so we can
        // ignore them here.
        if (primaryTerm == currentPrimaryTerm.getAsLong()) {
            synchronized (referencedTranslogFiles) {
                if (markedTranslogStartFile > translogFile.generation()) {
                    translogFile.decRef();
                } else {
                    assert referencedTranslogFiles.stream().allMatch(t -> t.generation() < translogFile.generation());
                    switch (state.get()) {
                        // Add if the shard is open. Decrement if shard is closed. Ignore is node is closing.
                        case OPEN -> referencedTranslogFiles.add(translogFile);
                        case CLOSED, CLOSED_NODE_STOPPING -> {
                        }
                    }
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
        synchronized (referencedTranslogFiles) {
            markedTranslogStartFile = Math.max(translogStartFile, markedTranslogStartFile);
            releaseReferencedTranslogFiles(translogStartFile);
        }
    }

    private void releaseReferencedTranslogFiles(long bound) {
        TranslogReplicator.BlobTranslogFile activeTranslogFile;
        while ((activeTranslogFile = referencedTranslogFiles.peek()) != null && bound > activeTranslogFile.generation()) {
            referencedTranslogFiles.poll();
            activeTranslogFile.decRef();
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

    public void writeToBuffer(BytesReference data, long seqNo, Translog.Location location) throws IOException {
        synchronized (bufferLock) {
            if (state.get() != State.OPEN) {
                throw alreadyClosedException(shardId);
            }
            Translog.Location newProcessedLocation = new Translog.Location(
                location.generation,
                location.translogLocation + location.size,
                0
            );
            assert newProcessedLocation.compareTo(processedLocation) > 0;
            processedLocation = newProcessedLocation;
            if (bufferState == null) {
                bufferState = new BufferState(new ReleasableBytesStreamOutput(bigArrays), shardTranslogGeneration++);
            } else {
                assert location.compareTo(bufferState.location) >= 0;
            }
            bufferState.append(data, seqNo, location);
        }
    }

    public long currentBufferSize() {
        synchronized (bufferLock) {
            return bufferState != null ? bufferState.data.size() : 0L;
        }
    }

    public SyncState pollSync(long generation) {
        final int[] referencedTranslogFileOffsets;
        long estimatedOps = 0;
        synchronized (referencedTranslogFiles) {
            referencedTranslogFileOffsets = new int[referencedTranslogFiles.size()];

            int i = 0;
            for (TranslogReplicator.BlobTranslogFile referencedFile : referencedTranslogFiles) {
                estimatedOps += referencedFile.checkpoints().get(shardId).totalOps();
                referencedTranslogFileOffsets[i] = Math.toIntExact(generation - referencedFile.generation());
                assert referencedTranslogFileOffsets[i] > 0 : generation + " " + referencedFile.generation();
                ++i;
            }
        }
        synchronized (bufferLock) {
            BufferState toReturn = bufferState;
            bufferState = null;
            estimatedOps += toReturn != null ? toReturn.totalOps() : 0;
            return new SyncState(estimatedOps, referencedTranslogFileOffsets, toReturn);
        }
    }

    public void close(boolean nodeStopping) {
        final ArrayList<ActionListener<Void>> toComplete;
        if (nodeStopping) {
            state.set(State.CLOSED_NODE_STOPPING);
        } else {
            state.set(State.CLOSED);
        }
        synchronized (listeners) {
            toComplete = new ArrayList<>(listeners);
            listeners.clear();
        }
        synchronized (bufferLock) {
            Releasables.close(bufferState);
            bufferState = null;
        }

        // The relocation hand-off forces a flush while holding the operation permits to a clean relocation should fully release the files
        // TODO: Not dec-ing files on close will cause them to leak in the translog replicator list. Clean-up in follow-up.

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

    record SyncState(long estimatedOps, int[] referencedTranslogFileOffsets, BufferState buffer) {

        TranslogMetadata metadata(long position, long size) {
            if (size == 0) {
                assert buffer == null;
                return new TranslogMetadata(
                    position,
                    0,
                    SequenceNumbers.NO_OPS_PERFORMED,
                    SequenceNumbers.NO_OPS_PERFORMED,
                    0,
                    -1L,
                    new TranslogMetadata.Directory(estimatedOps, referencedTranslogFileOffsets)
                );
            } else {
                return new TranslogMetadata(
                    position,
                    size,
                    buffer.minSeqNo(),
                    buffer.maxSeqNo(),
                    buffer.totalOps(),
                    buffer.getShardTranslogGeneration(),
                    new TranslogMetadata.Directory(estimatedOps, referencedTranslogFileOffsets)
                );
            }
        }
    }

    class BufferState implements Releasable {

        private final ReleasableBytesStreamOutput data;
        private final ArrayList<Long> seqNos;
        private final long shardTranslogGeneration;
        private long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        private long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        private long totalOps = 0;

        private Translog.Location location;

        private BufferState(ReleasableBytesStreamOutput data, long shardTranslogGeneration) {
            this.data = data;
            this.seqNos = new ArrayList<>();
            this.shardTranslogGeneration = shardTranslogGeneration;
        }

        public final void append(BytesReference data, long seqNo, Translog.Location location) throws IOException {
            data.writeTo(this.data);
            seqNos.add(seqNo);
            minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
            maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);
            totalOps++;
            this.location = location;
        }

        public ReleasableBytesStreamOutput data() {
            return data;
        }

        public long minSeqNo() {
            return minSeqNo;
        }

        public long maxSeqNo() {
            return maxSeqNo;
        }

        public long totalOps() {
            return totalOps;
        }

        public long getShardTranslogGeneration() {
            return shardTranslogGeneration;
        }

        private Translog.Location syncLocation() {
            return new Translog.Location(location.generation, location.translogLocation + location.size, 0);
        }

        public SyncMarker syncMarker() {
            return new SyncMarker(startingPrimaryTerm, syncLocation(), seqNos);
        }

        @Override
        public void close() {
            data.close();
        }
    }

    record SyncMarker(long primaryTerm, Translog.Location location, List<Long> syncedSeqNos) {}

    private enum State {
        OPEN,
        CLOSED,
        CLOSED_NODE_STOPPING
    }
}
