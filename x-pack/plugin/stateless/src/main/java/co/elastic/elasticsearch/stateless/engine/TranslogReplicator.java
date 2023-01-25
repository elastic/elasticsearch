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

package co.elastic.elasticsearch.stateless.engine;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TranslogReplicator extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(TranslogReplicator.class);

    // TODO: Find Recycling Instance
    private final BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    private final TriConsumer<String, BytesReference, ActionListener<Void>> client;
    private final ThreadPool threadPool;
    private final ConcurrentHashMap<ShardId, ShardSyncState> shardSyncStateByShardId = new ConcurrentHashMap<>();
    private final Object generateFlushLock = new Object();
    private final AtomicLong fileName = new AtomicLong(0);

    public TranslogReplicator(final ThreadPool threadPool, final TriConsumer<String, BytesReference, ActionListener<Void>> client) {
        this.threadPool = threadPool;
        this.client = client;
    }

    @Override
    protected void doStart() {
        threadPool.scheduler().scheduleAtFixedRate(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                logger.error("Unexpected exception when running translog replication task", e);
            }

            @Override
            protected void doRun() throws IOException {
                flush();
            }
        }, 200, 200, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {
        // TODO: Clean-up buffer states and finish listeners
    }

    public void add(final ShardId shardId, final BytesReference data, final long seqNo, final Translog.Location location) {
        try {
            ShardSyncState shardState = shardSyncStateByShardId.computeIfAbsent(shardId, (s) -> new ShardSyncState());
            shardState.writeToBuffer(data, seqNo, location);
        } catch (IOException e) {
            // TODO: IOException is required by the interface of BytesReference#write. However, it should never throw. If it were to throw,
            // this exception would propogate to the TranslogWriter and I think fail the engine. However, we should discuss whether this is
            // enough protection.
            assert false;
            throw new UncheckedIOException(e);
        }
    }

    public void sync(final ShardId shardId, Translog.Location location, ActionListener<Void> listener) {
        ShardSyncState shardSyncState = this.shardSyncStateByShardId.computeIfAbsent(shardId, (k) -> new ShardSyncState());
        shardSyncState.ensureSynced(new Translog.Location(location.generation, location.translogLocation + location.size, 0), listener);
    }

    private void flush() throws IOException {
        // This lock only needs to lock modifications to shardSyncStateByShardId and fileName generation.
        // In the future what it "locks" could be slimmed down.
        synchronized (generateFlushLock) {
            Map<ShardId, BufferState> toFlush = Maps.newMapWithExpectedSize(shardSyncStateByShardId.size());
            for (Map.Entry<ShardId, ShardSyncState> syncState : shardSyncStateByShardId.entrySet()) {
                BufferState bufferState = syncState.getValue().pollBufferForSync();
                if (bufferState != null) {
                    toFlush.put(syncState.getKey(), bufferState);
                }
            }
            if (toFlush.isEmpty()) {
                return;
            }

            ArrayList<Runnable> toRun = new ArrayList<>(toFlush.size());
            Map<ShardId, TranslogMetadata> checkpoints = new HashMap<>();
            ReleasableBytesStreamOutput compoundTranslog = new ReleasableBytesStreamOutput(bigArrays);
            boolean success = false;
            try {
                // TODO: Harden exception handling here. There are no known throws, but we need to close resources and notify listeners if
                // something fails.
                for (Map.Entry<ShardId, BufferState> entry : toFlush.entrySet()) {
                    BufferState buffer = entry.getValue();
                    Translog.Location lastOpLocation = buffer.location;
                    Translog.Location syncedLocation = new Translog.Location(
                        lastOpLocation.generation,
                        lastOpLocation.translogLocation + lastOpLocation.size,
                        0
                    );
                    ShardSyncState shardSyncState = this.shardSyncStateByShardId.computeIfAbsent(
                        entry.getKey(),
                        (s) -> new ShardSyncState()
                    );
                    shardSyncState.markSyncStarting(syncedLocation);
                    toRun.add(() -> shardSyncState.markSyncFinished(syncedLocation));
                    long position = compoundTranslog.position();
                    buffer.streamOutput.bytes().writeTo(compoundTranslog);
                    long size = compoundTranslog.position() - position;
                    checkpoints.put(
                        entry.getKey(),
                        new TranslogMetadata(position, size, buffer.minSeqNo, buffer.maxSeqNo, buffer.totalOps)
                    );
                    buffer.close();
                }

                try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
                    streamOutput.writeMap(checkpoints);
                    client.apply(
                        String.valueOf(fileName.getAndIncrement()),
                        CompositeBytesReference.of(streamOutput.bytes(), compoundTranslog.bytes()),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(Void unused) {
                                compoundTranslog.close();
                                for (Runnable runnable : toRun) {
                                    runnable.run();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                compoundTranslog.close();
                                logger.error("Unexpected error calling replicator client", e);
                                // TODO: RETRY and propagate error HANDLING. Currently the sync waiters do not know the difference between
                                // an error. A failure here MUST not WRITE ACK and needs to propagate a failure to the client for SYNC
                                // durability.
                                for (Runnable runnable : toRun) {
                                    runnable.run();
                                }
                            }

                        }
                    );
                }
                success = true;
            } finally {
                if (success == false) {
                    compoundTranslog.close();
                }
            }
        }

    }

    private static class BufferState implements Releasable {

        private final ReleasableBytesStreamOutput streamOutput;
        private long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        private long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        private long totalOps = 0;
        private Translog.Location location;

        private BufferState(Translog.Location location, ReleasableBytesStreamOutput streamOutput) {
            this.streamOutput = streamOutput;
            this.location = location;
        }

        public void markNewOp(long seqNo) {
            minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
            maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);
            ++totalOps;
        }

        @Override
        public void close() {
            streamOutput.close();
        }
    }

    private class ShardSyncState {

        private final Object bufferLock = new Object();
        private final TreeMap<Translog.Location, Boolean> ongoingSyncs = new TreeMap<>();
        private final PriorityQueue<SyncListener> listeners = new PriorityQueue<>();
        private volatile Translog.Location syncedLocation = new Translog.Location(0, 0, 0);
        private BufferState bufferState = null;

        private synchronized void markSyncStarting(Translog.Location location) {
            synchronized (ongoingSyncs) {
                ongoingSyncs.put(location, false);
            }
        }

        private void ensureSynced(Translog.Location location, ActionListener<Void> listener) {
            boolean completeListener = true;
            if (location.compareTo(syncedLocation) > 0) {
                synchronized (listeners) {
                    if (location.compareTo(syncedLocation) > 0) {
                        listeners.add(new SyncListener(location, listener));
                        completeListener = false;
                    }
                }
            }

            if (completeListener) {
                // TODO: Error Handling
                listener.onResponse(null);
            }
        }

        public void markSyncFinished(Translog.Location newlySyncedLocation) {
            boolean completeListeners = false;
            synchronized (ongoingSyncs) {
                Translog.Location firstLocation = ongoingSyncs.firstKey();
                if (firstLocation.equals(newlySyncedLocation)) {
                    ongoingSyncs.pollFirstEntry();
                    syncedLocation = firstLocation;

                    Iterator<Map.Entry<Translog.Location, Boolean>> iterator = ongoingSyncs.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Translog.Location, Boolean> entry = iterator.next();
                        if (entry.getValue()) {
                            syncedLocation = entry.getKey();
                            iterator.remove();
                        } else {
                            break;
                        }
                    }
                    completeListeners = true;
                } else {
                    Boolean previousValue = ongoingSyncs.put(newlySyncedLocation, true);
                    assert previousValue != null && previousValue == false;
                }
            }

            if (completeListeners) {
                ArrayList<SyncListener> toComplete = new ArrayList<>();
                synchronized (listeners) {
                    SyncListener listener;
                    while ((listener = listeners.peek()) != null && syncedLocation.compareTo(listener.location) >= 0) {
                        toComplete.add(listener);
                        listeners.poll();
                    }
                }
                for (SyncListener listener : toComplete) {
                    // TODO: Error handling
                    listener.listener().onResponse(null);
                }
            }

        }

        public void writeToBuffer(BytesReference data, long seqNo, Translog.Location location) throws IOException {
            synchronized (bufferLock) {
                if (bufferState == null) {
                    bufferState = new BufferState(location, new ReleasableBytesStreamOutput(bigArrays));
                }
                assert location.compareTo(bufferState.location) >= 0;
                bufferState.location = location;
                data.writeTo(bufferState.streamOutput);
                bufferState.markNewOp(seqNo);
            }

        }

        public BufferState pollBufferForSync() {
            synchronized (bufferLock) {
                BufferState toReturn = bufferState;
                bufferState = null;
                return toReturn;
            }
        }

        private record SyncListener(Translog.Location location, ActionListener<Void> listener) implements Comparable<SyncListener> {

            @Override
            public int compareTo(SyncListener o) {
                return location.compareTo(o.location);
            }
        }

    }
}
