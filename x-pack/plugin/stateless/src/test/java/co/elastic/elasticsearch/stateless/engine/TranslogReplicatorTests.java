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

import co.elastic.elasticsearch.stateless.ObjectStoreService;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TranslogReplicatorTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        Settings settings = Settings.builder().put("node.name", TranslogReplicatorTests.class.getSimpleName()).build();
        // TODO: Eventually we will move to more complicated scheduling logic than scheduleAtFixedRate. At that time switch to
        // DeterministicTaskQueue (which does not support scheduleAtFixedRate)
        threadPool = new ThreadPool(settings);
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    public void testTranslogBytesAreSynced() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);

        ArrayList<BytesReference> references = new ArrayList<>();
        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        doAnswer(invocation -> {
            references.add(getBytes(invocation.getArgument(1)));
            invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            return null;
        }).when(objectStoreService).uploadTranslogFile(any(), any(), any());

        TranslogReplicator translogReplicator = new TranslogReplicator(threadPool, Settings.EMPTY, objectStoreService);
        translogReplicator.doStart();

        BytesArray bytesArray = new BytesArray(new byte[16]);
        translogReplicator.add(shardId, bytesArray, 0, new Translog.Location(0, 0, bytesArray.length()));
        translogReplicator.add(shardId, bytesArray, 1, new Translog.Location(0, bytesArray.length(), bytesArray.length()));
        translogReplicator.add(shardId, bytesArray, 3, new Translog.Location(0, bytesArray.length() * 2L, bytesArray.length()));
        Translog.Location finalLocation = new Translog.Location(0, bytesArray.length() * 3L, bytesArray.length());
        translogReplicator.add(shardId, bytesArray, 2, finalLocation);

        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        translogReplicator.sync(shardId, finalLocation, future);
        future.actionGet();

        assertThat(references.size(), equalTo(1));
        StreamInput streamInput = references.get(0).streamInput();
        var expected = Map.of(shardId, new TranslogMetadata(0, 64, 0, 3, 4));
        assertThat("Unexpected metadata", streamInput.readMap(ShardId::new, TranslogMetadata::new), equalTo(expected));
        assertThat("Unexpected checksum", streamInput.readLong(), equalTo(calculateCrcChecksum(expected)));
    }

    public void testTranslogBytesAreSyncedEdgeCondition() {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);

        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        doAnswer(invocation -> {
            invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            return null;
        }).when(objectStoreService).uploadTranslogFile(any(), any(), any());

        TranslogReplicator translogReplicator = new TranslogReplicator(threadPool, Settings.EMPTY, objectStoreService);
        translogReplicator.doStart();

        BytesArray bytesArray = new BytesArray(new byte[16]);
        Translog.Location location = new Translog.Location(0, 0, bytesArray.length());
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        translogReplicator.sync(shardId, location, future);
        assertFalse(future.isDone());

        translogReplicator.add(shardId, bytesArray, 0, location);

        future.actionGet();

        PlainActionFuture<Void> synchronouslyCompleteFuture = PlainActionFuture.newFuture();
        translogReplicator.sync(shardId, location, synchronouslyCompleteFuture);
        assertTrue(synchronouslyCompleteFuture.isDone());

        PlainActionFuture<Void> synchronouslyIncompleteFuture = PlainActionFuture.newFuture();
        Translog.Location incompleteLocation = new Translog.Location(location.generation, location.translogLocation + location.size, 1);
        translogReplicator.sync(shardId, incompleteLocation, synchronouslyIncompleteFuture);
        assertFalse(synchronouslyIncompleteFuture.isDone());
    }

    public void testTranslogSyncOnlyCompletedOnceAllPriorFilesSynced() throws InterruptedException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);

        CountDownLatch intermediateStartedLatch = new CountDownLatch(1);
        CountDownLatch finalSyncStartedLatch = new CountDownLatch(1);
        AtomicReference<ActionListener<Void>> firstSyncCompleter = new AtomicReference<>();

        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        doAnswer(invocation -> {
            try {
                var metadata = invocation.<BytesReference>getArgument(1).streamInput().readMap(ShardId::new, TranslogMetadata::new);
                if (metadata.get(shardId).maxSeqNo() == 1L) {
                    firstSyncCompleter.set(invocation.getArgument(2));
                    intermediateStartedLatch.countDown();
                    return null;
                }
                if (metadata.get(shardId).maxSeqNo() == 3L) {
                    finalSyncStartedLatch.countDown();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            return null;
        }).when(objectStoreService).uploadTranslogFile(any(), any(), any());

        TranslogReplicator translogReplicator = new TranslogReplicator(threadPool, Settings.EMPTY, objectStoreService);
        translogReplicator.doStart();

        BytesArray bytesArray = new BytesArray(new byte[16]);
        translogReplicator.add(shardId, bytesArray, 0, new Translog.Location(0, 0, bytesArray.length()));
        Translog.Location intermediateLocation = new Translog.Location(0, bytesArray.length(), bytesArray.length());
        translogReplicator.add(shardId, bytesArray, 1, intermediateLocation);
        intermediateStartedLatch.await();

        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        translogReplicator.sync(shardId, intermediateLocation, future);
        expectThrows(ElasticsearchTimeoutException.class, () -> future.actionGet(300));

        translogReplicator.add(shardId, bytesArray, 2, new Translog.Location(0, bytesArray.length() * 2L, bytesArray.length()));
        Translog.Location finalLocation = new Translog.Location(0, bytesArray.length() * 3L, bytesArray.length());
        translogReplicator.add(shardId, bytesArray, 3, finalLocation);
        finalSyncStartedLatch.await();

        PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
        translogReplicator.sync(shardId, finalLocation, future2);
        expectThrows(ElasticsearchTimeoutException.class, () -> future2.actionGet(300));

        firstSyncCompleter.get().onResponse(null);

        future.actionGet();
        future2.actionGet();
    }

    public void testCompoudTranslogFile() throws IOException {
        ShardId shardId1 = new ShardId(new Index("name1", "uuid"), 0);
        ShardId shardId2 = new ShardId(new Index("name2", "uuid"), 0);

        ArrayList<BytesReference> references = new ArrayList<>();
        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        doAnswer(invocation -> {
            references.add(getBytes(invocation.getArgument(1)));
            invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            return null;
        }).when(objectStoreService).uploadTranslogFile(any(), any(), any());
        TranslogReplicator translogReplicator = new TranslogReplicator(threadPool, Settings.EMPTY, objectStoreService);
        translogReplicator.doStart();

        BytesArray bytesArray = new BytesArray(new byte[16]);
        translogReplicator.add(shardId2, bytesArray, 0, new Translog.Location(0, 0, bytesArray.length()));
        translogReplicator.add(shardId1, bytesArray, 0, new Translog.Location(0, 0, bytesArray.length()));
        Translog.Location finalLocationShard1 = new Translog.Location(0, bytesArray.length(), bytesArray.length());
        translogReplicator.add(shardId1, bytesArray, 1, finalLocationShard1);
        translogReplicator.add(shardId2, bytesArray, 1, finalLocationShard1);
        Translog.Location intermediateLocationShard2 = new Translog.Location(0, bytesArray.length() * 2L, bytesArray.length());
        translogReplicator.add(shardId2, bytesArray, 3, intermediateLocationShard2);

        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        translogReplicator.sync(shardId1, finalLocationShard1, future);
        future.actionGet();

        assertThat(references.size(), equalTo(1));
        StreamInput streamInput = references.get(0).streamInput();

        var expected1 = new LinkedHashMap<ShardId, TranslogMetadata>();// iteration order sensitive
        expected1.put(shardId1, new TranslogMetadata(48, 32, 0, 1, 2));
        expected1.put(shardId2, new TranslogMetadata(0, 48, 0, 3, 3));
        assertThat("Unexpected metadata", streamInput.readMap(ShardId::new, TranslogMetadata::new), equalTo(expected1));
        assertThat("Unexpected checksum", streamInput.readLong(), equalTo(calculateCrcChecksum(expected1)));

        PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
        translogReplicator.sync(shardId2, intermediateLocationShard2, future2);
        assertTrue(future2.isDone());

        Translog.Location finalLocationShard2 = new Translog.Location(0, bytesArray.length() * 3L, bytesArray.length());

        PlainActionFuture<Void> future3 = PlainActionFuture.newFuture();
        translogReplicator.sync(shardId2, finalLocationShard2, future3);
        assertFalse(future3.isDone());

        translogReplicator.add(shardId2, bytesArray, 2, finalLocationShard2);
        future3.actionGet();

        assertThat(references.size(), equalTo(2));
        streamInput = references.get(1).streamInput();
        var expected2 = Map.of(shardId2, new TranslogMetadata(0, 16, 2, 2, 1));
        assertThat("Unexpected metadata", streamInput.readMap(ShardId::new, TranslogMetadata::new), equalTo(expected2));
        assertThat("Unexpected checksum", streamInput.readLong(), equalTo(calculateCrcChecksum(expected2)));
    }

    public void testDefaultFlushInterval() {
        var threadPool = Mockito.mock(ThreadPool.class);
        var scheduler = Mockito.mock(ScheduledExecutorService.class);
        Mockito.when(threadPool.scheduler()).thenReturn(scheduler);
        try (var translogReplicator = new TranslogReplicator(threadPool, Settings.EMPTY, mock(ObjectStoreService.class))) {
            translogReplicator.doStart();
            verify(scheduler).scheduleAtFixedRate(any(Runnable.class), eq(200L), eq(200L), eq(TimeUnit.MILLISECONDS));
        }
    }

    public void testCustomFlushInterval() {
        var threadPool = Mockito.mock(ThreadPool.class);
        var scheduler = Mockito.mock(ScheduledExecutorService.class);
        Mockito.when(threadPool.scheduler()).thenReturn(scheduler);
        try (
            var translogReplicator = new TranslogReplicator(
                threadPool,
                Settings.builder().put("stateless.translog.flush_interval", new TimeValue(1, TimeUnit.SECONDS)).build(),
                mock(ObjectStoreService.class)
            )
        ) {
            translogReplicator.doStart();
            verify(scheduler).scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        }
    }

    public void testMinFlushInterval() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TranslogReplicator(
                threadPool,
                Settings.builder().put("stateless.translog.flush_interval", new TimeValue(5, TimeUnit.MILLISECONDS)).build(),
                mock(ObjectStoreService.class)
            )
        );
        assertEquals(
            "failed to parse value [5ms] for setting [stateless.translog.flush_interval], must be >= [10ms]",
            exception.getMessage()
        );
    }

    private static BytesReference getBytes(BytesReference reference) {
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();) {
            reference.writeTo(bytesStreamOutput);
            return bytesStreamOutput.bytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long calculateCrcChecksum(Map<ShardId, TranslogMetadata> metadata) throws IOException {
        var crc32 = new CRC32();
        new StreamOutput() {
            @Override
            public void writeByte(byte b) {
                crc32.update(b);
            }

            @Override
            public void writeBytes(byte[] b, int offset, int length) {
                crc32.update(b, offset, length);
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        }.writeMap(metadata);
        return crc32.getValue();
    }
}
