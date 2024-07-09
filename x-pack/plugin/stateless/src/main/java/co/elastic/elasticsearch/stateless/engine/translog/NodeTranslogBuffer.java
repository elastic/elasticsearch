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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class NodeTranslogBuffer implements Releasable {

    private final Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);

    private final BigArrays bigArrays;
    private final long flushSizeThreshold;
    private final Map<ShardSyncState, ShardBuffer> buffers = ConcurrentCollections.newConcurrentMap();
    private final AtomicLong bufferSize = new AtomicLong();
    private final AtomicBoolean minimumIntervalExhausted = new AtomicBoolean(false);
    private final AtomicBoolean syncRequested = new AtomicBoolean(false);
    private final AtomicBoolean flushTaken = new AtomicBoolean(false);

    public NodeTranslogBuffer(BigArrays bigArrays, long flushSizeThreshold) {
        this.bigArrays = bigArrays;
        this.flushSizeThreshold = flushSizeThreshold;
    }

    /**
     * Returns true if a flush should be performed
     */
    public boolean markMinimumIntervalExhausted() {
        // Only schedule a single interval check
        assert minimumIntervalExhausted.get() == false;
        minimumIntervalExhausted.set(true);
        return syncRequested.get() && flushTaken.compareAndSet(false, true);
    }

    /**
     * Returns true if a flush should be performed
     */
    public boolean markSyncRequested() {
        // If this is the first sync request the minimum interval has been exhausted then attempt to take the flush
        return syncRequested.getAndSet(true) == false && minimumIntervalExhausted.get() && flushTaken.compareAndSet(false, true);
    }

    public boolean shouldFlushBufferDueToSize() {
        return bufferSize.get() >= flushSizeThreshold && flushTaken.compareAndSet(false, true);
    }

    /**
     * Returns true if the write to the buffer succeeded. Otherwise, this buffer has been closed for writing and the user must try again
     * on the next node buffer.
     */
    boolean writeToBuffer(ShardSyncState shardSyncState, BytesReference data, long seqNo, Translog.Location location) throws IOException {
        if (semaphore.tryAcquire()) {
            try {
                Translog.Location newProcessedLocation = new Translog.Location(
                    location.generation(),
                    location.translogLocation() + location.size(),
                    0
                );
                shardSyncState.updateProcessedLocation(newProcessedLocation);
                ShardBuffer shardBuffer = buffers.computeIfAbsent(
                    shardSyncState,
                    (k) -> new ShardBuffer(shardSyncState.getStartingPrimaryTerm(), new ReleasableBytesStreamOutput(bigArrays))
                );
                shardBuffer.append(data, seqNo, location);
                bufferSize.getAndAdd(data.length());
            } finally {
                semaphore.release();
            }
            return true;
        } else {
            return false;
        }
    }

    TranslogReplicator.CompoundTranslog complete(long generation, Collection<ShardSyncState> activeShards) throws IOException {
        try {
            semaphore.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        try (Releasable ignored = () -> Releasables.close(buffers.values())) {
            boolean dataToSync = false;
            var metadata = new HashMap<ShardId, TranslogMetadata>();
            var syncedLocations = new HashMap<ShardId, ShardSyncState.SyncMarker>();
            var compoundTranslogStream = new ReleasableBytesStreamOutput(bigArrays);
            var headerStream = new ReleasableBytesStreamOutput(bigArrays);

            for (var state : activeShards) {
                ShardId shardId = state.getShardId();

                long position = compoundTranslogStream.position();
                ShardBuffer buffer = buffers.get(state);
                TranslogMetadata.Directory directory = state.createDirectory(generation, buffer == null ? 0 : buffer.totalOps());

                // If the ShardSyncState is closed ignore. If shard has been closed, then there is a potentially a race with creating an
                // accurate directory. The safest approach is to not include this shard in the translog metadata.
                if (state.isClosed() == false) {
                    if (buffer != null) {
                        dataToSync = true;
                        buffer.data().bytes().writeTo(compoundTranslogStream);
                        metadata.put(shardId, metadata(buffer, position, compoundTranslogStream.position() - position, directory));
                        syncedLocations.put(shardId, buffer.syncMarker());
                    } else {
                        metadata.put(shardId, metadata(null, position, compoundTranslogStream.position() - position, directory));
                    }
                }
            }

            // It is possible that there were operations in the buffer which are no longer associated with active shards. If there is not
            // data to sync related to active shards, do not produce a translog to sync
            if (dataToSync == false) {
                Releasables.close(headerStream, compoundTranslogStream);
                return null;
            }

            // Write the header to the stream
            new CompoundTranslogHeader(metadata).writeToStore(headerStream);

            TranslogReplicator.CompoundTranslogBytes compoundTranslogBytes = new TranslogReplicator.CompoundTranslogBytes(
                CompositeBytesReference.of(headerStream.bytes(), compoundTranslogStream.bytes()),
                () -> Releasables.close(headerStream, compoundTranslogStream)
            );
            TranslogReplicator.CompoundTranslogMetadata compoundMetadata = new TranslogReplicator.CompoundTranslogMetadata(
                Strings.format("%019d", generation),
                generation,
                metadata,
                syncedLocations
            );

            return new TranslogReplicator.CompoundTranslog(compoundMetadata, compoundTranslogBytes);
        } finally {
            buffers.clear();
        }
    }

    @Override
    public void close() {
        Releasables.close(buffers.values());
    }

    private TranslogMetadata metadata(ShardBuffer buffer, long position, long size, TranslogMetadata.Directory directory) {
        if (size == 0) {
            assert buffer == null;
            return new TranslogMetadata(position, 0, SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED, 0, directory);
        } else {
            return new TranslogMetadata(position, size, buffer.minSeqNo(), buffer.maxSeqNo(), buffer.totalOps(), directory);
        }
    }

    private static class ShardBuffer implements Releasable {

        private final long primaryTerm;
        private final ReleasableBytesStreamOutput data;
        private final ArrayList<Long> seqNos;
        private long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        private long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        private long totalOps = 0;

        private Translog.Location location;

        private ShardBuffer(long primaryTerm, ReleasableBytesStreamOutput data) {
            this.primaryTerm = primaryTerm;
            this.data = data;
            this.seqNos = new ArrayList<>();
        }

        private void append(BytesReference data, long seqNo, Translog.Location location) throws IOException {
            data.writeTo(this.data);
            seqNos.add(seqNo);
            minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
            maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);
            totalOps++;
            this.location = location;
        }

        private ReleasableBytesStreamOutput data() {
            return data;
        }

        private long minSeqNo() {
            return minSeqNo;
        }

        private long maxSeqNo() {
            return maxSeqNo;
        }

        private long totalOps() {
            return totalOps;
        }

        private ShardSyncState.SyncMarker syncMarker() {
            return new ShardSyncState.SyncMarker(
                primaryTerm,
                new Translog.Location(location.generation(), location.translogLocation() + location.size(), 0),
                seqNos
            );
        }

        @Override
        public void close() {
            data.close();
        }
    }
}
