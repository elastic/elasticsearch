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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
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

    public void testTranslogBytesAreSyncedPeriodically() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);

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

        assertThat(compoundFiles.size(), equalTo(1));

        assertTranslogContains(
            new TranslogReplicatorReader(objectStoreService, shardId, 0),
            new TranslogEntry(new TranslogMetadata(0, 64, 0, 3, 4), repeatBytes(bytesArray.array(), 4))
        );
    }

    public void testListenerThreadContextPreserved() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        String header = "header";
        String preserved = "preserved";

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);

        TranslogReplicator translogReplicator = new TranslogReplicator(threadPool, Settings.EMPTY, objectStoreService);
        translogReplicator.doStart();

        BytesArray bytesArray = new BytesArray(new byte[16]);
        Translog.Location location = new Translog.Location(0, 0, bytesArray.length());
        translogReplicator.add(shardId, bytesArray, 0, location);

        AtomicReference<String> value = new AtomicReference<>();
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();

        threadPool.getThreadContext().putHeader(header, preserved);

        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                value.set(threadPool.getThreadContext().getHeader(header));
                future.onResponse(unused);
            }

            @Override
            public void onFailure(Exception e) {
                future.onFailure(e);
            }
        };
        if (randomBoolean()) {
            translogReplicator.sync(shardId, location, listener);
        } else {
            translogReplicator.syncAll(shardId, listener);
        }
        threadPool.getThreadContext().stashContext();
        assertNull(threadPool.getThreadContext().getHeader(header));

        future.actionGet();
        assertThat(value.get(), equalTo(preserved));
    }

    public void testTranslogBytesAreSyncedWhenReachingSizeThreshold() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            Settings.builder()
                .put(TranslogReplicator.FLUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(1))
                .put(TranslogReplicator.FLUSH_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(64))
                .build(),
            objectStoreService
        );
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

        assertTranslogContains(
            new TranslogReplicatorReader(objectStoreService, shardId, 0),
            new TranslogEntry(new TranslogMetadata(0, 64, 0, 3, 4), repeatBytes(bytesArray.array(), 4))
        );
    }

    public void testTranslogBytesAreSyncedAfterRetry() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        AtomicInteger attempt = new AtomicInteger(0);
        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        doAnswer(invocation -> {
            if (attempt.incrementAndGet() < 3) {
                invocation.<ActionListener<Void>>getArgument(2).onFailure(new IOException("test"));
            } else {
                compoundFiles.add(getBytes(invocation.getArgument(1)));
                invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            }
            return null;
        }).when(objectStoreService).uploadTranslogFile(any(), any(), any());

        BlobContainer blobContainer = mock(BlobContainer.class);
        doAnswer(invocation -> {
            var map = new LinkedHashMap<String, BlobMetadata>();
            for (int i = 0; i < compoundFiles.size(); i++) {
                String filename = Strings.format("%019d", i);
                map.put(filename, new BlobMetadata(filename, compoundFiles.get(i).length()));
            }
            return map;
        }).when(blobContainer).listBlobs();
        doAnswer(invocation -> {
            String filename = invocation.getArgument(0);
            int index = Integer.parseInt(filename);
            return compoundFiles.get(index).streamInput();
        }).when(blobContainer).readBlob(any());
        doReturn(blobContainer).when(objectStoreService).getLocalTranslogBlobContainer();

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

        assertThat(compoundFiles.size(), equalTo(1));
        assertTranslogContains(
            new TranslogReplicatorReader(objectStoreService, shardId, 0),
            new TranslogEntry(new TranslogMetadata(0, 64, 0, 3, 4), repeatBytes(bytesArray.array(), 4))
        );
    }

    public void testTranslogBytesAreSyncedEdgeCondition() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);

        ObjectStoreService objectStoreService = mockObjectStoreService(new ArrayList<>());

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

    public void testTranslogChecksumIsChecked() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);

        TranslogReplicator translogReplicator = new TranslogReplicator(threadPool, Settings.EMPTY, objectStoreService);
        translogReplicator.doStart();

        BytesArray bytesArray = new BytesArray(new byte[16]);
        Translog.Location finalLocation = new Translog.Location(0, 0, bytesArray.length());
        translogReplicator.add(shardId, bytesArray, 0, finalLocation);

        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        translogReplicator.sync(shardId, finalLocation, future);
        future.actionGet();

        assertThat(compoundFiles.size(), equalTo(1));

        // Fiddle the checksum of the compound file
        try (StreamInput streamInput = compoundFiles.get(0).streamInput()) {
            BufferedChecksumStreamInput checksumStreamInput = new BufferedChecksumStreamInput(compoundFiles.get(0).streamInput(), "test");
            Map<ShardId, TranslogMetadata> checkpoints = checksumStreamInput.readMap(ShardId::new, TranslogMetadata::new);
            long wrongChecksum = checksumStreamInput.getChecksum() + randomLongBetween(1, 100);
            BytesReference translogs = streamInput.readBytesReference();
            try (BytesStreamOutput newCompoundFile = new BytesStreamOutput()) {
                newCompoundFile.writeMap(checkpoints);
                newCompoundFile.writeLong(wrongChecksum);
                translogs.writeTo(newCompoundFile);
                compoundFiles.replaceAll((ignored) -> newCompoundFile.bytes());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        var reader = new TranslogReplicatorReader(objectStoreService, shardId, 0);
        var exception = expectThrows(TranslogCorruptedException.class, reader::next);
        assertThat(exception.getMessage(), containsString("checksum verification failed"));
    }

    public void testTranslogSyncOnlyCompletedOnceAllPriorFilesSynced() throws InterruptedException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);

        CountDownLatch intermediateStartedLatch = new CountDownLatch(1);
        CountDownLatch finalSyncStartedLatch = new CountDownLatch(1);
        AtomicReference<ActionListener<Void>> firstSyncCompleter = new AtomicReference<>();

        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        doAnswer(invocation -> {
            var metadata = invocation.<BytesReference>getArgument(1).streamInput().readMap(ShardId::new, TranslogMetadata::new);
            if (metadata.get(shardId).maxSeqNo() == 1L) {
                firstSyncCompleter.set(invocation.getArgument(2));
                intermediateStartedLatch.countDown();
                return null;
            }
            if (metadata.get(shardId).maxSeqNo() == 3L) {
                finalSyncStartedLatch.countDown();
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

    public void testCompoundTranslogFile() throws IOException {
        ShardId shardId1 = new ShardId(new Index("name1", "uuid"), 0);
        ShardId shardId2 = new ShardId(new Index("name2", "uuid"), 0);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);

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

        assertThat(compoundFiles.size(), equalTo(1));

        assertTranslogContains(
            new TranslogReplicatorReader(objectStoreService, shardId1, 0),
            new TranslogEntry(new TranslogMetadata(0, 32, 0, 1, 2), repeatBytes(bytesArray.array(), 2))
        );
        assertTranslogContains(
            new TranslogReplicatorReader(objectStoreService, shardId2, 0),
            new TranslogEntry(new TranslogMetadata(32, 48, 0, 3, 3), repeatBytes(bytesArray.array(), 3))
        );

        PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
        translogReplicator.sync(shardId2, intermediateLocationShard2, future2);
        assertTrue(future2.isDone());

        Translog.Location finalLocationShard2 = new Translog.Location(0, bytesArray.length() * 3L, bytesArray.length());

        PlainActionFuture<Void> future3 = PlainActionFuture.newFuture();
        translogReplicator.sync(shardId2, finalLocationShard2, future3);
        assertFalse(future3.isDone());

        translogReplicator.add(shardId2, bytesArray, 2, finalLocationShard2);
        future3.actionGet();

        assertThat(compoundFiles.size(), equalTo(2));

        assertTranslogContains(
            new TranslogReplicatorReader(objectStoreService, shardId1, 0),
            new TranslogEntry(new TranslogMetadata(0, 32, 0, 1, 2), repeatBytes(bytesArray.array(), 2))
        );
        assertTranslogContains(
            new TranslogReplicatorReader(objectStoreService, shardId2, 0),
            new TranslogEntry(new TranslogMetadata(32, 48, 0, 3, 3), repeatBytes(bytesArray.array(), 3)),
            new TranslogEntry(new TranslogMetadata(0, 16, 2, 2, 1), bytesArray.array())
        );
    }

    public void testSyncAll() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);

        TranslogReplicator translogReplicator = new TranslogReplicator(threadPool, Settings.EMPTY, objectStoreService);
        translogReplicator.doStart();

        BytesArray bytesArray = new BytesArray(new byte[16]);
        translogReplicator.add(shardId, bytesArray, 0, new Translog.Location(0, 0, bytesArray.length()));
        translogReplicator.add(shardId, bytesArray, 1, new Translog.Location(0, bytesArray.length(), bytesArray.length()));
        translogReplicator.add(shardId, bytesArray, 3, new Translog.Location(0, bytesArray.length() * 2L, bytesArray.length()));
        Translog.Location finalLocation = new Translog.Location(0, bytesArray.length() * 3L, bytesArray.length());
        translogReplicator.add(shardId, bytesArray, 2, finalLocation);

        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        translogReplicator.syncAll(shardId, future);
        future.actionGet();

        assertThat(compoundFiles.size(), equalTo(1));

        assertTranslogContains(
            new TranslogReplicatorReader(objectStoreService, shardId, 0),
            new TranslogEntry(new TranslogMetadata(0, 64, 0, 3, 4), repeatBytes(bytesArray.array(), 4))
        );
    }

    @SuppressWarnings("unchecked")
    public void testSchedulesFlushCheck() {
        var threadPool = mock(ThreadPool.class);
        try (var translogReplicator = new TranslogReplicator(threadPool, Settings.EMPTY, mock(ObjectStoreService.class))) {
            translogReplicator.doStart();
            verify(threadPool).scheduleWithFixedDelay(any(Runnable.class), eq(TimeValue.timeValueMillis(50)), eq(ThreadPool.Names.GENERIC));
        }
    }

    private static BytesReference getBytes(BytesReference reference) throws IOException {
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            reference.writeTo(bytesStreamOutput);
            return bytesStreamOutput.bytes();
        }
    }

    private ObjectStoreService mockObjectStoreService(ArrayList<BytesReference> compoundFiles) throws IOException {
        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        doAnswer(invocation -> {
            compoundFiles.add(getBytes(invocation.getArgument(1)));
            invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            return null;
        }).when(objectStoreService).uploadTranslogFile(any(), any(), any());

        BlobContainer blobContainer = mock(BlobContainer.class);
        doAnswer(invocation -> {
            var map = new LinkedHashMap<String, BlobMetadata>();
            for (int i = 0; i < compoundFiles.size(); i++) {
                String filename = Strings.format("%019d", i);
                map.put(filename, new BlobMetadata(filename, compoundFiles.get(i).length()));
            }
            return map;
        }).when(blobContainer).listBlobs();
        doAnswer(invocation -> {
            String filename = invocation.getArgument(0);
            int index = Integer.parseInt(filename);
            return compoundFiles.get(index).streamInput();
        }).when(blobContainer).readBlob(any());
        doReturn(blobContainer).when(objectStoreService).getLocalTranslogBlobContainer();

        return objectStoreService;
    }

    private byte[] repeatBytes(byte[] array, int times) throws IOException {
        ByteArrayOutputStream expectedBytesStream = new ByteArrayOutputStream();
        for (int i = 0; i < times; i++) {
            expectedBytesStream.write(array);
        }
        return expectedBytesStream.toByteArray();
    }

    private static void assertTranslogContains(TranslogReplicatorReader reader, TranslogEntry... entries) {
        for (int i = 0; i < entries.length; i++) {
            assertThat("Reader does not have expected entry", reader.hasNext(), equalTo(true));
            var entry = reader.next();
            assertThat(entry.metadata(), equalTo(entries[i].metadata()));
            assertArrayEquals("unexpected byte contents", entries[i].data(), BytesReference.toBytes(entry.data()));
        }
        assertThat("Reader has unexpected extra entries", reader.hasNext(), equalTo(false));
    }

    private record TranslogEntry(TranslogMetadata metadata, byte[] data) {}
}
