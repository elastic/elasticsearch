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

import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics.TRANSLOG_FILES_NETWORK_TIME_METRIC;
import static co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics.TRANSLOG_FILES_SIZE_METRIC;
import static co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics.TRANSLOG_FILES_TOTAL_METRIC;
import static co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics.TRANSLOG_OPERATIONS_SIZE_METRIC;
import static co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics.TRANSLOG_OPERATIONS_TOTAL_METRIC;
import static co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics.TRANSLOG_REPLAY_TIME_METRIC;
import static org.elasticsearch.indices.recovery.RecoverySourceHandlerTests.generateOperation;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TranslogReplicatorTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        Settings settings = Settings.builder().put("node.name", TranslogReplicatorTests.class.getSimpleName()).build();
        // TODO: Eventually we will move to more complicated scheduling logic than scheduleAtFixedRate. At that time switch to
        // DeterministicTaskQueue (which does not support scheduleAtFixedRate)
        threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    private static Settings getSettings() {
        // Lower the flush intervals to reduce test runtime
        return Settings.builder()
            .put(TranslogReplicator.FLUSH_RETRY_INITIAL_DELAY_SETTING.getKey(), TimeValue.timeValueMillis(randomLongBetween(10, 20)))
            .put(TranslogReplicator.FLUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(randomLongBetween(50, 75)))
            .build();
    }

    public void testTranslogBytesAreSyncedPeriodically() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(4);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();
        translogReplicator.add(shardId, operationsBytes[1], 1, new Translog.Location(0, currentLocation, operationsBytes[1].length()));
        currentLocation += operationsBytes[1].length();
        translogReplicator.add(shardId, operationsBytes[3], 3, new Translog.Location(0, currentLocation, operationsBytes[3].length()));
        currentLocation += operationsBytes[3].length();
        Translog.Location finalLocation = new Translog.Location(0, currentLocation, operationsBytes[2].length());
        translogReplicator.add(shardId, operationsBytes[2], 2, finalLocation);

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.sync(shardId, finalLocation, future);
        future.actionGet();

        assertThat(compoundFiles.size(), equalTo(Math.toIntExact(translogReplicator.getMaxUploadedFile() + 1)));

        assertTranslogContains(
            objectStoreService.getTranslogBlobContainer(),
            shardId,
            operations[0],
            operations[1],
            operations[3],
            operations[2]
        );
    }

    public void testTranslogReplicatorReaderMinMaxSeqNo() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(6);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();
        translogReplicator.add(shardId, operationsBytes[1], 1, new Translog.Location(0, currentLocation, operationsBytes[1].length()));
        currentLocation += operationsBytes[1].length();
        Translog.Location intermediateLocation = new Translog.Location(0, currentLocation, operationsBytes[3].length());
        translogReplicator.add(shardId, operationsBytes[3], 3, intermediateLocation);
        currentLocation += operationsBytes[3].length();

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.sync(shardId, intermediateLocation, future);
        future.actionGet();

        translogReplicator.add(shardId, operationsBytes[5], 5, new Translog.Location(0, currentLocation, operationsBytes[5].length()));
        currentLocation += operationsBytes[5].length();
        translogReplicator.add(shardId, operationsBytes[2], 2, new Translog.Location(0, currentLocation, operationsBytes[2].length()));
        currentLocation += operationsBytes[2].length();
        Translog.Location finalLocation = new Translog.Location(0, currentLocation, operationsBytes[4].length());
        translogReplicator.add(shardId, operationsBytes[4], 4, finalLocation);

        future = new PlainActionFuture<>();
        translogReplicator.sync(shardId, finalLocation, future);
        future.actionGet();

        assertThat(compoundFiles.size(), equalTo(Math.toIntExact(translogReplicator.getMaxUploadedFile() + 1)));

        assertTranslogContains(
            objectStoreService.getTranslogBlobContainer(),
            shardId,
            0,
            5,
            0,
            () -> false,
            operations[0],
            operations[1],
            operations[3],
            operations[5],
            operations[2],
            operations[4]
        );

        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId, 1, 1, 0, () -> false, operations[1]);
        assertTranslogContains(
            objectStoreService.getTranslogBlobContainer(),
            shardId,
            1,
            3,
            0,
            () -> false,
            operations[1],
            operations[3],
            operations[2]
        );
        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId, 4, 5, 0, () -> false, operations[5], operations[4]);
        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId, 8, 10, 0, () -> false);
    }

    public void testTranslogReplicatorReaderStartingTranslogFile() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(6);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();
        translogReplicator.add(shardId, operationsBytes[1], 1, new Translog.Location(0, currentLocation, operationsBytes[1].length()));
        currentLocation += operationsBytes[1].length();
        Translog.Location intermediateLocation = new Translog.Location(0, currentLocation, operationsBytes[2].length());
        translogReplicator.add(shardId, operationsBytes[2], 2, intermediateLocation);
        currentLocation += operationsBytes[2].length();

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.sync(shardId, intermediateLocation, future);
        future.actionGet();

        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId, operations[0], operations[1], operations[2]);

        assertThat(
            translogReplicator.getMaxUploadedFile(),
            equalTo((long) objectStoreService.getTranslogBlobContainer().listBlobs(OperationPurpose.TRANSLOG).size() - 1)
        );
        long startRecoveryFile = translogReplicator.getMaxUploadedFile() + 1;

        translogReplicator.add(shardId, operationsBytes[3], 3, new Translog.Location(0, currentLocation, operationsBytes[3].length()));
        currentLocation += operationsBytes[5].length();
        translogReplicator.add(shardId, operationsBytes[4], 4, new Translog.Location(0, currentLocation, operationsBytes[4].length()));
        currentLocation += operationsBytes[2].length();
        Translog.Location finalLocation = new Translog.Location(0, currentLocation, operationsBytes[5].length());
        translogReplicator.add(shardId, operationsBytes[5], 5, finalLocation);

        future = new PlainActionFuture<>();
        translogReplicator.sync(shardId, finalLocation, future);
        future.actionGet();

        assertThat(
            translogReplicator.getMaxUploadedFile(),
            equalTo((long) objectStoreService.getTranslogBlobContainer().listBlobs(OperationPurpose.TRANSLOG).size() - 1)
        );

        assertTranslogContains(
            objectStoreService.getTranslogBlobContainer(),
            shardId,
            operations[0],
            operations[1],
            operations[2],
            operations[3],
            operations[4],
            operations[5]
        );

        assertTranslogContains(
            objectStoreService.getTranslogBlobContainer(),
            shardId,
            0,
            Long.MAX_VALUE,
            startRecoveryFile,
            () -> false,
            operations[3],
            operations[4],
            operations[5]
        );
    }

    public void testTranslogReplicatorReaderUsesDirectoryToSkipFiles() throws Exception {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(6);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();
        translogReplicator.add(shardId, operationsBytes[1], 1, new Translog.Location(0, currentLocation, operationsBytes[1].length()));
        currentLocation += operationsBytes[1].length();
        translogReplicator.add(shardId, operationsBytes[2], 2, new Translog.Location(0, currentLocation, operationsBytes[2].length()));
        currentLocation += operationsBytes[2].length();

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future);
        future.actionGet();

        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId, operations[0], operations[1], operations[2]);

        assertThat(
            translogReplicator.getMaxUploadedFile(),
            equalTo((long) objectStoreService.getTranslogBlobContainer().listBlobs(OperationPurpose.TRANSLOG).size() - 1)
        );
        long commitFileNewStartingPoint = translogReplicator.getMaxUploadedFile() + 1;

        translogReplicator.add(shardId, operationsBytes[3], 3, new Translog.Location(0, currentLocation, operationsBytes[3].length()));
        currentLocation += operationsBytes[3].length();
        translogReplicator.add(shardId, operationsBytes[4], 4, new Translog.Location(0, currentLocation, operationsBytes[4].length()));
        currentLocation += operationsBytes[4].length();

        future = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future);
        future.actionGet();

        assertThat(
            translogReplicator.getMaxUploadedFile(),
            equalTo((long) objectStoreService.getTranslogBlobContainer().listBlobs(OperationPurpose.TRANSLOG).size() - 1)
        );

        assertTranslogContains(
            objectStoreService.getTranslogBlobContainer(),
            shardId,
            operations[0],
            operations[1],
            operations[2],
            operations[3],
            operations[4]
        );

        translogReplicator.markShardCommitUploaded(shardId, commitFileNewStartingPoint);

        assertBusy(() -> assertThat(translogReplicator.getTranslogFilesToDelete(), empty()));

        translogReplicator.add(shardId, operationsBytes[5], 5, new Translog.Location(0, currentLocation, operationsBytes[5].length()));

        future = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future);
        future.actionGet();

        assertThat(
            translogReplicator.getMaxUploadedFile(),
            equalTo((long) objectStoreService.getTranslogBlobContainer().listBlobs(OperationPurpose.TRANSLOG).size() - 1)
        );

        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId, operations[3], operations[4], operations[5]);
    }

    public void testListenerThreadContextPreserved() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);
        String header = "header";
        String preserved = "preserved";

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        BytesArray bytesArray = new BytesArray(new byte[16]);
        Translog.Location location = new Translog.Location(0, 0, bytesArray.length());
        translogReplicator.add(shardId, bytesArray, 0, location);

        AtomicReference<String> value = new AtomicReference<>();
        PlainActionFuture<Void> future = new PlainActionFuture<>();

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
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        int threshold = randomIntBetween(128, 512);
        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            Settings.builder()
                .put(TranslogReplicator.FLUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(1))
                .put(TranslogReplicator.FLUSH_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(threshold))
                .build(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        ArrayList<Translog.Operation> operations = new ArrayList<>();
        ArrayList<BytesReference> operationsBytes = new ArrayList<>();
        int bytes = 0;
        int seqNo = 0;
        while (bytes <= threshold) {
            Translog.Operation operation = generateOperation(seqNo++);
            operations.add(operation);
            BytesReference ref = convertOperationsToBytes(new Translog.Operation[] { operation })[0];
            operationsBytes.add(ref);
            bytes += ref.length();
        }

        Translog.Location location = new Translog.Location(0, 0, 0);
        long currentLocation = 0;
        seqNo = 0;
        for (BytesReference ref : operationsBytes) {
            location = new Translog.Location(0, currentLocation, ref.length());
            translogReplicator.add(shardId, ref, seqNo++, location);
            currentLocation += ref.length();
        }

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.sync(shardId, location, future);
        future.actionGet();

        assertThat(compoundFiles.size(), equalTo(Math.toIntExact(translogReplicator.getMaxUploadedFile() + 1)));
        Translog.Operation[] expectedOperations = operations.toArray(new Translog.Operation[0]);
        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId, expectedOperations);

    }

    public void testTranslogBytesAreSyncedAfterRetry() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        AtomicInteger attempt = new AtomicInteger(0);
        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        when(objectStoreService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
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
        }).when(blobContainer).listBlobs(OperationPurpose.TRANSLOG);
        doAnswer(invocation -> {
            String filename = invocation.getArgument(1);
            int index = Integer.parseInt(filename);
            return compoundFiles.get(index).streamInput();
        }).when(blobContainer).readBlob(eq(OperationPurpose.TRANSLOG), any());
        doReturn(blobContainer).when(objectStoreService).getTranslogBlobContainer();
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(4);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();
        translogReplicator.add(shardId, operationsBytes[1], 1, new Translog.Location(0, currentLocation, operationsBytes[1].length()));
        currentLocation += operationsBytes[1].length();
        translogReplicator.add(shardId, operationsBytes[3], 3, new Translog.Location(0, currentLocation, operationsBytes[3].length()));
        currentLocation += operationsBytes[3].length();
        Translog.Location finalLocation = new Translog.Location(0, currentLocation, operationsBytes[2].length());
        translogReplicator.add(shardId, operationsBytes[2], 2, finalLocation);

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.sync(shardId, finalLocation, future);
        future.actionGet();

        assertThat(compoundFiles.size(), equalTo(Math.toIntExact(translogReplicator.getMaxUploadedFile() + 1)));
        assertTranslogContains(
            objectStoreService.getTranslogBlobContainer(),
            shardId,
            operations[0],
            operations[1],
            operations[3],
            operations[2]
        );
    }

    // Test a sync that hits the exact number of bytes is completed and a location 1 byte past the amount flushed will wait on the next
    // flush
    public void testTranslogBytesAreSyncedEdgeCondition() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);
        AtomicReference<CountDownLatch> blockerRef = new AtomicReference<>(new CountDownLatch(0));

        ObjectStoreService objectStoreService = mockObjectStoreService(new ArrayList<>(), blockerRef);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        BytesArray bytesArray = new BytesArray(new byte[16]);
        Translog.Location location = new Translog.Location(0, 0, bytesArray.length());
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.add(shardId, bytesArray, 0, location);
        translogReplicator.sync(shardId, location, future);
        future.actionGet();

        PlainActionFuture<Void> synchronouslyCompleteFuture = new PlainActionFuture<>();
        translogReplicator.sync(shardId, location, synchronouslyCompleteFuture);
        assertTrue(synchronouslyCompleteFuture.isDone());

        PlainActionFuture<Void> synchronouslyIncompleteFuture = new PlainActionFuture<>();
        Translog.Location incompleteLocation = new Translog.Location(
            location.generation(),
            location.translogLocation() + location.size(),
            1
        );
        CountDownLatch blocker = new CountDownLatch(1);
        blockerRef.set(blocker);
        translogReplicator.add(shardId, new BytesArray(new byte[1]), 1, incompleteLocation);
        translogReplicator.sync(shardId, incompleteLocation, synchronouslyIncompleteFuture);
        assertFalse(synchronouslyIncompleteFuture.isDone());
        blocker.countDown();
        synchronouslyIncompleteFuture.actionGet();
    }

    public void testTranslogNotFlushedUntilSyncRequested() throws Exception {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        PlainActionFuture<Void> syncStartedFuture = new PlainActionFuture<>();

        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        when(objectStoreService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        doAnswer(invocation -> {
            syncStartedFuture.onResponse(null);
            invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            return null;
        }).when(objectStoreService).uploadTranslogFile(any(), any(), any());
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, (seqNo) -> {});

        Translog.Operation[] operations = generateRandomOperations(4);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();
        Translog.Location intermediateLocation = new Translog.Location(0, currentLocation, operationsBytes[1].length());
        translogReplicator.add(shardId, operationsBytes[1], 1, intermediateLocation);

        expectThrows(ElasticsearchTimeoutException.class, () -> syncStartedFuture.actionGet(300, TimeUnit.MILLISECONDS));

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        if (randomBoolean()) {
            translogReplicator.sync(shardId, intermediateLocation, future);
        } else {
            translogReplicator.syncAll(shardId, future);
        }

        syncStartedFuture.actionGet();
        future.actionGet();
    }

    public void testTranslogSyncOnlyCompletedOnceAllPriorFilesSynced() throws Exception {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        CountDownLatch intermediateStartedLatch = new CountDownLatch(1);
        CountDownLatch finalSyncStartedLatch = new CountDownLatch(1);
        AtomicReference<ActionListener<Void>> firstSyncCompleter = new AtomicReference<>();

        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        when(objectStoreService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        doAnswer(invocation -> {
            var metadata = CompoundTranslogHeader.readFromStore("test", invocation.<BytesReference>getArgument(1).streamInput()).metadata();
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
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(4);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();
        Translog.Location intermediateLocation = new Translog.Location(0, currentLocation, operationsBytes[1].length());
        translogReplicator.add(shardId, operationsBytes[1], 1, intermediateLocation);
        currentLocation += operationsBytes[1].length();

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.sync(shardId, intermediateLocation, future);
        safeAwait(intermediateStartedLatch);
        expectThrows(ElasticsearchTimeoutException.class, () -> future.actionGet(300, TimeUnit.MILLISECONDS));

        translogReplicator.add(shardId, operationsBytes[2], 2, new Translog.Location(0, currentLocation, operationsBytes[2].length()));
        currentLocation += operationsBytes[2].length();
        Translog.Location finalLocation = new Translog.Location(0, currentLocation, operationsBytes[3].length());
        translogReplicator.add(shardId, operationsBytes[3], 3, finalLocation);

        PlainActionFuture<Void> future2 = new PlainActionFuture<>();
        translogReplicator.sync(shardId, finalLocation, future2);
        safeAwait(finalSyncStartedLatch);
        expectThrows(ElasticsearchTimeoutException.class, () -> future2.actionGet(300, TimeUnit.MILLISECONDS));

        firstSyncCompleter.get().onResponse(null);

        future.actionGet();
        future2.actionGet();
    }

    public void testCompoundTranslogFile() throws Exception {
        ShardId shardId1 = new ShardId(new Index("name1", "uuid"), 0);
        ShardId shardId2 = new ShardId(new Index("name2", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId1, primaryTerm, seqNo -> {});
        translogReplicator.register(shardId2, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(4);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId2, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        translogReplicator.add(shardId1, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();
        Translog.Location finalLocationShard1 = new Translog.Location(0, currentLocation, operationsBytes[1].length());
        translogReplicator.add(shardId1, operationsBytes[1], 1, finalLocationShard1);
        translogReplicator.add(shardId2, operationsBytes[1], 1, finalLocationShard1);
        currentLocation += operationsBytes[1].length();
        Translog.Location intermediateLocationShard2 = new Translog.Location(0, currentLocation, operationsBytes[3].length());
        translogReplicator.add(shardId2, operationsBytes[3], 3, intermediateLocationShard2);
        currentLocation += operationsBytes[3].length();

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId1, future);
        future.actionGet();
        PlainActionFuture<Void> future2 = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId2, future2);
        future2.actionGet();

        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId1, operations[0], operations[1]);

        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId2, operations[0], operations[1], operations[3]);

        Translog.Location finalLocationShard2 = new Translog.Location(0, currentLocation, operationsBytes[2].length());

        PlainActionFuture<Void> future3 = new PlainActionFuture<>();
        translogReplicator.add(shardId2, operationsBytes[2], 2, finalLocationShard2);
        translogReplicator.sync(shardId2, finalLocationShard2, future3);
        future3.actionGet();

        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId1, operations[0], operations[1]);

        assertTranslogContains(
            objectStoreService.getTranslogBlobContainer(),
            shardId2,
            operations[0],
            operations[1],
            operations[3],
            operations[2]
        );
    }

    public void testSyncAll() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(4);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();
        translogReplicator.add(shardId, operationsBytes[1], 1, new Translog.Location(0, currentLocation, operationsBytes[1].length()));
        currentLocation += operationsBytes[1].length();
        translogReplicator.add(shardId, operationsBytes[3], 3, new Translog.Location(0, currentLocation, operationsBytes[3].length()));
        currentLocation += operationsBytes[3].length();
        Translog.Location finalLocation = new Translog.Location(0, currentLocation, operationsBytes[2].length());
        translogReplicator.add(shardId, operationsBytes[2], 2, finalLocation);

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future);
        future.actionGet();

        assertThat(compoundFiles.size(), equalTo(Math.toIntExact(translogReplicator.getMaxUploadedFile() + 1)));

        Translog.Operation[] expectedOperations = { operations[0], operations[1], operations[3], operations[2] };
        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId, expectedOperations);
    }

    public void testCheckShardStillAllocated() throws Exception {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm + 1
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(1);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future);

        assertBusy(() -> {
            assertThat(compoundFiles.size(), equalTo(1));
            assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId, operations[0]);
        });

        // This sync should never complete. The actual sync will be failed when the shard is closed.
        assertFalse(future.isDone());
        translogReplicator.unregister(shardId);
        expectThrows(AlreadyClosedException.class, future::actionGet);
    }

    public void testTranslogReplicatorReaderCanCancel() throws Exception {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(4);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();
        translogReplicator.add(shardId, operationsBytes[1], 1, new Translog.Location(0, currentLocation, operationsBytes[1].length()));
        currentLocation += operationsBytes[1].length();
        Translog.Location intermediateLocation = new Translog.Location(0, currentLocation, operationsBytes[2].length());
        translogReplicator.add(shardId, operationsBytes[2], 2, intermediateLocation);
        currentLocation += operationsBytes[2].length();

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.sync(shardId, intermediateLocation, future);
        future.actionGet();

        currentLocation += operationsBytes[3].length();
        Translog.Location finalLocation = new Translog.Location(0, currentLocation, operationsBytes[3].length());
        translogReplicator.add(shardId, operationsBytes[3], 3, finalLocation);

        future = new PlainActionFuture<>();
        translogReplicator.sync(shardId, finalLocation, future);
        future.actionGet();

        assertThat(compoundFiles.size(), greaterThan(1));
        assertThat(compoundFiles.size(), equalTo(Math.toIntExact(translogReplicator.getMaxUploadedFile() + 1)));

        assertTranslogContains(
            objectStoreService.getTranslogBlobContainer(),
            shardId,
            0,
            3,
            0,
            () -> false,
            operations[0],
            operations[1],
            operations[2],
            operations[3]
        );

        List<Boolean> isClosing = Arrays.asList(randomBoolean(), true);
        AtomicInteger n = new AtomicInteger();

        expectThrows(
            AlreadyClosedException.class,
            () -> assertTranslogContains(
                objectStoreService.getTranslogBlobContainer(),
                shardId,
                0,
                3,
                0,
                () -> isClosing.get(n.getAndIncrement()),
                operations[0],
                operations[1],
                operations[2],
                operations[3]
            )
        );
    }

    public void testCompleteListenerWithExceptionWhenShardIsUnregistered() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        CountDownLatch latch = new CountDownLatch(1);
        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        when(objectStoreService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        doAnswer(invocation -> {
            safeAwait(latch);
            invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            return null;
        }).when(objectStoreService).uploadTranslogFile(any(), any(), any());
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(1);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        Translog.Location location = new Translog.Location(0, 0, operationsBytes[0].length());
        translogReplicator.add(shardId, operationsBytes[0], 0, location);

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        if (randomBoolean()) {
            translogReplicator.syncAll(shardId, future);
        } else {
            translogReplicator.sync(shardId, location, future);
        }
        translogReplicator.unregister(shardId);
        latch.countDown();

        expectThrows(AlreadyClosedException.class, future::actionGet);
    }

    public void testAddThrowsWhenShardUnregistered() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);
        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        when(objectStoreService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(1);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        Translog.Location location = new Translog.Location(0, 0, operationsBytes[0].length());

        translogReplicator.unregister(shardId);
        expectThrows(AlreadyClosedException.class, () -> translogReplicator.add(shardId, operationsBytes[0], 0, location));
    }

    public void testReplicatorReaderHandlesConcurrentDeleteIfNotInDirectory() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        ShardId shardId2 = new ShardId(new Index("name", "uuid2"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});
        translogReplicator.register(shardId2, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(3);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future);
        future.actionGet();

        assertThat(compoundFiles.size(), equalTo(1));

        translogReplicator.add(shardId, operationsBytes[1], 1, new Translog.Location(0, currentLocation, operationsBytes[1].length()));

        PlainActionFuture<Void> future2 = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future2);
        future2.actionGet();

        assertThat(compoundFiles.size(), equalTo(2));

        // Unregister so that the last translog file will not contain a directory
        translogReplicator.unregister(shardId);

        translogReplicator.add(shardId2, operationsBytes[2], 2, new Translog.Location(0, 0, operationsBytes[2].length()));

        PlainActionFuture<Void> future3 = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId2, future3);
        future3.actionGet();

        assertThat(compoundFiles.size(), equalTo(3));

        // List call happens in ctor
        TranslogReplicatorReader reader = new TranslogReplicatorReader(objectStoreService.getTranslogBlobContainer(), shardId);

        // Delete file with shard 2 operations
        compoundFiles.set(2, null);

        // If the translog recovery start file has advanced past the missing hole, then we get all the operations
        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId, operations[0], operations[1]);
    }

    public void testReplicatorReaderStopsRecoveringWhenMissingExpectedDirectoryFilesWhenAtTheBeginning() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(4);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future);
        future.actionGet();

        assertThat(compoundFiles.size(), equalTo(1));

        translogReplicator.add(shardId, operationsBytes[1], 1, new Translog.Location(0, currentLocation, operationsBytes[1].length()));
        currentLocation += operationsBytes[1].length();

        PlainActionFuture<Void> future2 = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future2);
        future2.actionGet();

        assertThat(compoundFiles.size(), equalTo(2));

        translogReplicator.add(shardId, operationsBytes[3], 3, new Translog.Location(0, currentLocation, operationsBytes[3].length()));
        currentLocation += operationsBytes[3].length();
        Translog.Location finalLocation = new Translog.Location(0, currentLocation, operationsBytes[2].length());
        translogReplicator.add(shardId, operationsBytes[2], 2, finalLocation);

        PlainActionFuture<Void> future3 = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future3);
        future3.actionGet();

        assertThat(compoundFiles.size(), equalTo(Math.toIntExact(translogReplicator.getMaxUploadedFile() + 1)));

        compoundFiles.set(0, null);

        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId);

        // If the translog recovery start file has advanced past the missing hole, then we get all the operations
        assertTranslogContains(
            objectStoreService.getTranslogBlobContainer(),
            shardId,
            0,
            Long.MAX_VALUE,
            1,
            () -> false,
            operations[1],
            operations[3],
            operations[2]
        );
    }

    public void testReplicatorReaderStopsRecoveringAfterHoleInExpectedDirectoryFiles() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ArrayList<BytesReference> compoundFiles = new ArrayList<>();
        ObjectStoreService objectStoreService = mockObjectStoreService(compoundFiles);
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();
        translogReplicator.register(shardId, primaryTerm, seqNo -> {});

        Translog.Operation[] operations = generateRandomOperations(4);
        BytesReference[] operationsBytes = convertOperationsToBytes(operations);
        long currentLocation = 0;
        translogReplicator.add(shardId, operationsBytes[0], 0, new Translog.Location(0, currentLocation, operationsBytes[0].length()));
        currentLocation += operationsBytes[0].length();

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future);
        future.actionGet();

        assertThat(compoundFiles.size(), equalTo(1));

        translogReplicator.add(shardId, operationsBytes[1], 1, new Translog.Location(0, currentLocation, operationsBytes[1].length()));
        currentLocation += operationsBytes[1].length();

        PlainActionFuture<Void> future2 = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future2);
        future2.actionGet();

        assertThat(compoundFiles.size(), equalTo(2));

        translogReplicator.add(shardId, operationsBytes[3], 3, new Translog.Location(0, currentLocation, operationsBytes[3].length()));
        currentLocation += operationsBytes[3].length();
        Translog.Location finalLocation = new Translog.Location(0, currentLocation, operationsBytes[2].length());
        translogReplicator.add(shardId, operationsBytes[2], 2, finalLocation);

        PlainActionFuture<Void> future3 = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future3);
        future3.actionGet();

        assertThat(compoundFiles.size(), equalTo(Math.toIntExact(translogReplicator.getMaxUploadedFile() + 1)));

        compoundFiles.set(1, null);

        assertTranslogContains(objectStoreService.getTranslogBlobContainer(), shardId, operations[0]);
    }

    public void testClosedShardSyncTriggersListener() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 10);

        ObjectStoreService objectStoreService = mockObjectStoreService(new ArrayList<>());
        StatelessClusterConsistencyService consistencyService = mockConsistencyService();

        TranslogReplicator translogReplicator = new TranslogReplicator(
            threadPool,
            getSettings(),
            objectStoreService,
            consistencyService,
            (sId) -> primaryTerm
        );
        translogReplicator.doStart();

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        if (randomBoolean()) {
            translogReplicator.syncAll(shardId, future);
        } else {
            translogReplicator.sync(shardId, new Translog.Location(0, 0, 0), future);
        }

        expectThrows(AlreadyClosedException.class, future::actionGet);
    }

    private static Translog.Operation[] generateRandomOperations(int numOps) {
        final Translog.Operation[] operations = new Translog.Operation[numOps];
        for (int i = 0; i < numOps; i++) {
            operations[i] = generateOperation(i);
        }
        return operations;
    }

    private static BytesReference[] convertOperationsToBytes(Translog.Operation[] operations) throws IOException {
        BytesReference[] bytesReferences = new BytesReference[operations.length];
        for (int i = 0; i < bytesReferences.length; i++) {
            Translog.Operation operation = operations[i];
            try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
                Translog.writeOperationWithSize(bytesStreamOutput, operation);
                bytesReferences[i] = bytesStreamOutput.bytes();
            }
        }
        return bytesReferences;
    }

    private static BytesReference getBytes(BytesReference reference) throws IOException {
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            reference.writeTo(bytesStreamOutput);
            return bytesStreamOutput.bytes();
        }
    }

    private ObjectStoreService mockObjectStoreService(ArrayList<BytesReference> compoundFiles) throws IOException {
        return mockObjectStoreService(compoundFiles, new AtomicReference<>(new CountDownLatch(0)));
    }

    private ObjectStoreService mockObjectStoreService(
        ArrayList<BytesReference> compoundFiles,
        AtomicReference<CountDownLatch> uploadBlocker
    ) throws IOException {
        ObjectStoreService objectStoreService = mock(ObjectStoreService.class);
        when(objectStoreService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        doAnswer(invocation -> {
            uploadBlocker.get().await();
            compoundFiles.add(getBytes(invocation.getArgument(1)));
            invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            return null;
        }).when(objectStoreService).uploadTranslogFile(any(), any(), any());

        BlobContainer blobContainer = mock(BlobContainer.class);
        doAnswer(invocation -> {
            var map = new LinkedHashMap<String, BlobMetadata>();
            for (int i = 0; i < compoundFiles.size(); i++) {
                String filename = Strings.format("%019d", i);
                if (compoundFiles.get(i) != null) {
                    map.put(filename, new BlobMetadata(filename, compoundFiles.get(i).length()));
                }
            }
            return map;
        }).when(blobContainer).listBlobs(OperationPurpose.TRANSLOG);
        doAnswer(invocation -> {
            String filename = invocation.getArgument(1);
            int index = Integer.parseInt(filename);
            return compoundFiles.get(index).streamInput();
        }).when(blobContainer).readBlob(eq(OperationPurpose.TRANSLOG), any());
        doReturn(blobContainer).when(objectStoreService).getTranslogBlobContainer();

        return objectStoreService;
    }

    private StatelessClusterConsistencyService mockConsistencyService() {
        StatelessClusterConsistencyService consistencyService = mock(StatelessClusterConsistencyService.class);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(0);
            listener.onResponse(null);
            return null;
        }).when(consistencyService).ensureClusterStateConsistentWithRootBlob(any(), any());
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(0);
            listener.onResponse(null);
            return null;
        }).when(consistencyService).delayedEnsureClusterStateConsistentWithRootBlob(any());
        when(consistencyService.state()).thenReturn(ClusterState.EMPTY_STATE);
        return consistencyService;
    }

    private static Map<InstrumentType, List<String>> metricsByType = Map.of(
        InstrumentType.LONG_COUNTER,
        List.of(TRANSLOG_OPERATIONS_TOTAL_METRIC, TRANSLOG_OPERATIONS_SIZE_METRIC, TRANSLOG_FILES_TOTAL_METRIC, TRANSLOG_FILES_SIZE_METRIC),
        InstrumentType.LONG_HISTOGRAM,
        List.of(TRANSLOG_REPLAY_TIME_METRIC, TRANSLOG_FILES_NETWORK_TIME_METRIC)
    );

    private static void assertTranslogContains(
        final BlobContainer blobContainer,
        final ShardId shardId,
        final long fromSeqNo,
        final long toSeqNo,
        final long translogRecoveryStartFile,
        final BooleanSupplier isClosing,
        Translog.Operation... operations
    ) throws IOException {
        var recordingMeterRegistry = new RecordingMeterRegistry();
        try (
            var reader = new TranslogReplicatorReader(
                blobContainer,
                shardId,
                fromSeqNo,
                toSeqNo,
                translogRecoveryStartFile,
                isClosing,
                new TranslogRecoveryMetrics(recordingMeterRegistry)
            )
        ) {
            for (Translog.Operation value : operations) {
                Translog.Operation operation = reader.next();
                assertThat("Reader does not have a next operation", operation, notNullValue());
                assertThat("Next operation is not equal to expected operation", operation, equalTo(value));
            }
            assertThat("Reader has unexpected extra entries", reader.next(), equalTo(null));
        }
        // check that all expected metrics are emitted. Do not assert their values
        // because some metrics depend on system timer and cannot be reliably asserted
        metricsByType.forEach((type, metrics) -> metrics.forEach(metric -> assertMetricsEmitted(recordingMeterRegistry, type, metric)));
    }

    private static void assertTranslogContains(BlobContainer blobContainer, ShardId shardId, Translog.Operation... operations)
        throws IOException {
        assertTranslogContains(blobContainer, shardId, 0, Long.MAX_VALUE, 0, () -> false, operations);
    }

    private static void assertMetricsEmitted(RecordingMeterRegistry recordingMeterRegistry, InstrumentType type, String name) {
        var measurements = recordingMeterRegistry.getRecorder().getMeasurements(type, name);
        var expected = switch (name) {
            case TRANSLOG_OPERATIONS_TOTAL_METRIC -> 3; // index, delete, noop
            case TRANSLOG_FILES_TOTAL_METRIC, TRANSLOG_FILES_SIZE_METRIC -> 2;  // referenced and unreferenced translog files
            default -> 1;
        };
        assertThat("Metric " + name + " was not emitted", measurements.size(), equalTo(expected));
    }
}
