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

package org.elasticsearch.index.translog;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.mockfile.FilterFileChannel;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.LocalCheckpointTrackerTests;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomLongBetween;
import static org.elasticsearch.common.util.BigArrays.NON_RECYCLING_INSTANCE;
import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class TranslogTests extends ESTestCase {

    protected final ShardId shardId = new ShardId("index", "_na_", 1);

    protected Translog translog;
    private AtomicLong globalCheckpoint;
    protected Path translogDir;

    @Override
    protected void afterIfSuccessful() throws Exception {
        super.afterIfSuccessful();

        if (translog.isOpen()) {
            if (translog.currentFileGeneration() > 1) {
                markCurrentGenAsCommitted(translog);
                translog.trimUnreferencedReaders();
                assertFileDeleted(translog, translog.currentFileGeneration() - 1);
            }
            translog.close();
        }
        assertFileIsPresent(translog, translog.currentFileGeneration());
        IOUtils.rm(translog.location()); // delete all the locations

    }

    protected Translog createTranslog(TranslogConfig config, String translogUUID) throws IOException {
        return new Translog(config, translogUUID, createTranslogDeletionPolicy(config.getIndexSettings()),
            () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
    }

    private void markCurrentGenAsCommitted(Translog translog) throws IOException {
        commit(translog, translog.currentFileGeneration());
    }

    private void rollAndCommit(Translog translog) throws IOException {
        translog.rollGeneration();
        commit(translog, translog.currentFileGeneration());
    }

    private void commit(Translog translog, long genToCommit) throws IOException {
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        deletionPolicy.setMinTranslogGenerationForRecovery(genToCommit);
        long minGenRequired = deletionPolicy.minTranslogGenRequired(translog.getReaders(), translog.getCurrent());
        translog.trimUnreferencedReaders();
        assertThat(minGenRequired, equalTo(translog.getMinFileGeneration()));
        assertFilePresences(translog);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // if a previous test failed we clean up things here
        translogDir = createTempDir();
        translog = create(translogDir);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            translog.getDeletionPolicy().assertNoOpenTranslogRefs();
            translog.close();
        } finally {
            super.tearDown();
        }
    }

    private Translog create(Path path) throws IOException {
        globalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        final TranslogConfig translogConfig = getTranslogConfig(path);
        final TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy(translogConfig.getIndexSettings());
        return new Translog(translogConfig, null, deletionPolicy, () -> globalCheckpoint.get());
    }

    private TranslogConfig getTranslogConfig(final Path path) {
        final Settings settings = Settings
            .builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT)
            // only randomize between nog age retention and a long one, so failures will have a chance of reproducing
            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomBoolean() ? "-1ms" : "1h")
            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), randomIntBetween(-1, 2048) + "b")
            .build();
        return getTranslogConfig(path, settings);
    }

    private TranslogConfig getTranslogConfig(final Path path, final Settings settings) {
        final ByteSizeValue bufferSize;
        if (randomBoolean()) {
            bufferSize = TranslogConfig.DEFAULT_BUFFER_SIZE;
        } else {
            bufferSize = new ByteSizeValue(10 + randomInt(128 * 1024), ByteSizeUnit.BYTES);
        }

        final IndexSettings indexSettings =
                IndexSettingsModule.newIndexSettings(shardId.getIndex(), settings);
        return new TranslogConfig(shardId, path, indexSettings, NON_RECYCLING_INSTANCE, bufferSize);
    }

    private void addToTranslogAndList(Translog translog, ArrayList<Translog.Operation> list, Translog.Operation op) throws IOException {
        list.add(op);
        translog.add(op);
    }

    public void testIdParsingFromFile() {
        long id = randomIntBetween(0, Integer.MAX_VALUE);
        Path file = translogDir.resolve(Translog.TRANSLOG_FILE_PREFIX + id + ".tlog");
        assertThat(Translog.parseIdFromFileName(file), equalTo(id));

        id = randomIntBetween(0, Integer.MAX_VALUE);
        file = translogDir.resolve(Translog.TRANSLOG_FILE_PREFIX + id);
        try {
            Translog.parseIdFromFileName(file);
            fail("invalid pattern");
        } catch (IllegalArgumentException ex) {
            // all good
        }

        file = translogDir.resolve(Translog.TRANSLOG_FILE_PREFIX + id + ".recovering");
        try {
            Translog.parseIdFromFileName(file);
            fail("invalid pattern");
        } catch (IllegalArgumentException ex) {
            // all good
        }

        file = translogDir.resolve(Translog.TRANSLOG_FILE_PREFIX + randomNonTranslogPatternString(1, 10) + id);
        try {
            Translog.parseIdFromFileName(file);
            fail("invalid pattern");
        } catch (IllegalArgumentException ex) {
            // all good
        }
        file = translogDir.resolve(randomNonTranslogPatternString(1, Translog.TRANSLOG_FILE_PREFIX.length() - 1));
        try {
            Translog.parseIdFromFileName(file);
            fail("invalid pattern");
        } catch (IllegalArgumentException ex) {
            // all good
        }
    }

    private String randomNonTranslogPatternString(int min, int max) {
        String string;
        boolean validPathString;
        do {
            validPathString = false;
            string = randomRealisticUnicodeOfCodepointLength(randomIntBetween(min, max));
            try {
                final Path resolved = translogDir.resolve(string);
                // some strings (like '/' , '..') do not refer to a file, which we this method should return
                validPathString = resolved.getFileName() != null;
            } catch (InvalidPathException ex) {
                // some FS don't like our random file names -- let's just skip these random choices
            }
        } while (Translog.PARSE_STRICT_ID_PATTERN.matcher(string).matches() || validPathString == false);
        return string;
    }


    public void testSimpleOperations() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.size(0));
        }

        addToTranslogAndList(translog, ops, new Translog.Index("test", "1", 0, new byte[]{1}));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
            assertThat(snapshot.totalOperations(), equalTo(ops.size()));
        }

        addToTranslogAndList(translog, ops, new Translog.Delete("test", "2", 1, newUid("2")));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
            assertThat(snapshot.totalOperations(), equalTo(ops.size()));
        }

        final long seqNo = randomNonNegativeLong();
        final long primaryTerm = randomNonNegativeLong();
        final String reason = randomAlphaOfLength(16);
        addToTranslogAndList(translog, ops, new Translog.NoOp(seqNo, primaryTerm, reason));

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {

            Translog.Index index = (Translog.Index) snapshot.next();
            assertNotNull(index);
            assertThat(BytesReference.toBytes(index.source()), equalTo(new byte[]{1}));

            Translog.Delete delete = (Translog.Delete) snapshot.next();
            assertNotNull(delete);
            assertThat(delete.uid(), equalTo(newUid("2")));

            Translog.NoOp noOp = (Translog.NoOp) snapshot.next();
            assertNotNull(noOp);
            assertThat(noOp.seqNo(), equalTo(seqNo));
            assertThat(noOp.primaryTerm(), equalTo(primaryTerm));
            assertThat(noOp.reason(), equalTo(reason));

            assertNull(snapshot.next());
        }

        long firstId = translog.currentFileGeneration();
        translog.rollGeneration();
        assertThat(translog.currentFileGeneration(), Matchers.not(equalTo(firstId)));

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
            assertThat(snapshot.totalOperations(), equalTo(ops.size()));
        }

        markCurrentGenAsCommitted(translog);
        try (Translog.Snapshot snapshot = translog.newSnapshotFromGen(firstId + 1)) {
            assertThat(snapshot, SnapshotMatchers.size(0));
            assertThat(snapshot.totalOperations(), equalTo(0));
        }
    }

    protected TranslogStats stats() throws IOException {
        // force flushing and updating of stats
        translog.sync();
        TranslogStats stats = translog.stats();
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            stats.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            stats = new TranslogStats();
            stats.readFrom(in);
        }
        return stats;
    }

    public void testStats() throws IOException {
        // self control cleaning for test
        translog.getDeletionPolicy().setRetentionSizeInBytes(1024 * 1024);
        translog.getDeletionPolicy().setRetentionAgeInMillis(3600 * 1000);
        final long firstOperationPosition = translog.getFirstOperationPosition();
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations(), equalTo(0));
        }
        assertThat((int) firstOperationPosition, greaterThan(CodecUtil.headerLength(TranslogWriter.TRANSLOG_CODEC)));
        translog.add(new Translog.Index("test", "1", 0, new byte[]{1}));

        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations(), equalTo(1));
            assertThat(stats.getTranslogSizeInBytes(), equalTo(97L));
            assertThat(stats.getUncommittedOperations(), equalTo(1));
            assertThat(stats.getUncommittedSizeInBytes(), equalTo(97L));
        }

        translog.add(new Translog.Delete("test", "2", 1, newUid("2")));
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations(), equalTo(2));
            assertThat(stats.getTranslogSizeInBytes(), equalTo(146L));
            assertThat(stats.getUncommittedOperations(), equalTo(2));
            assertThat(stats.getUncommittedSizeInBytes(), equalTo(146L));
        }

        translog.add(new Translog.Delete("test", "3", 2, newUid("3")));
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations(), equalTo(3));
            assertThat(stats.getTranslogSizeInBytes(), equalTo(195L));
            assertThat(stats.getUncommittedOperations(), equalTo(3));
            assertThat(stats.getUncommittedSizeInBytes(), equalTo(195L));
        }

        translog.add(new Translog.NoOp(3, 1, randomAlphaOfLength(16)));
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations(), equalTo(4));
            assertThat(stats.getTranslogSizeInBytes(), equalTo(237L));
            assertThat(stats.getUncommittedOperations(), equalTo(4));
            assertThat(stats.getUncommittedSizeInBytes(), equalTo(237L));
        }

        final long expectedSizeInBytes = 280L;
        translog.rollGeneration();
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations(), equalTo(4));
            assertThat(stats.getTranslogSizeInBytes(), equalTo(expectedSizeInBytes));
            assertThat(stats.getUncommittedOperations(), equalTo(4));
            assertThat(stats.getUncommittedSizeInBytes(), equalTo(expectedSizeInBytes));
        }

        {
            final TranslogStats stats = stats();
            final BytesStreamOutput out = new BytesStreamOutput();
            stats.writeTo(out);
            final TranslogStats copy = new TranslogStats();
            copy.readFrom(out.bytes().streamInput());

            assertThat(copy.estimatedNumberOfOperations(), equalTo(4));
            assertThat(copy.getTranslogSizeInBytes(), equalTo(expectedSizeInBytes));

            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                copy.toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
                assertThat(builder.string(), equalTo("{\"translog\":{\"operations\":4,\"size_in_bytes\":" + expectedSizeInBytes
                    + ",\"uncommitted_operations\":4,\"uncommitted_size_in_bytes\":" + expectedSizeInBytes + "}}"));
            }
        }

        markCurrentGenAsCommitted(translog);
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations(), equalTo(4));
            assertThat(stats.getTranslogSizeInBytes(), equalTo(expectedSizeInBytes));
            assertThat(stats.getUncommittedOperations(), equalTo(0));
            assertThat(stats.getUncommittedSizeInBytes(), equalTo(firstOperationPosition));
        }
    }

    public void testTotalTests() {
        final TranslogStats total = new TranslogStats();
        final int n = randomIntBetween(0, 16);
        final List<TranslogStats> statsList = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final TranslogStats stats = new TranslogStats(randomIntBetween(1, 4096), randomIntBetween(1, 1 << 20),
                randomIntBetween(1, 1 << 20), randomIntBetween(1, 4096));
            statsList.add(stats);
            total.add(stats);
        }

        assertThat(
            total.estimatedNumberOfOperations(),
            equalTo(statsList.stream().mapToInt(TranslogStats::estimatedNumberOfOperations).sum()));
        assertThat(
            total.getTranslogSizeInBytes(),
            equalTo(statsList.stream().mapToLong(TranslogStats::getTranslogSizeInBytes).sum()));
        assertThat(
            total.getUncommittedOperations(),
            equalTo(statsList.stream().mapToInt(TranslogStats::getUncommittedOperations).sum()));
        assertThat(
            total.getUncommittedSizeInBytes(),
            equalTo(statsList.stream().mapToLong(TranslogStats::getUncommittedSizeInBytes).sum()));
    }

    public void testNegativeNumberOfOperations() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TranslogStats(-1, 1, 1, 1));
        assertThat(e, hasToString(containsString("numberOfOperations must be >= 0")));
        e = expectThrows(IllegalArgumentException.class, () -> new TranslogStats(1, 1, -1, 1));
        assertThat(e, hasToString(containsString("uncommittedOperations must be >= 0")));
    }

    public void testNegativeSizeInBytes() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TranslogStats(1, -1, 1, 1));
        assertThat(e, hasToString(containsString("translogSizeInBytes must be >= 0")));
        e = expectThrows(IllegalArgumentException.class, () -> new TranslogStats(1, 1, 1, -1));
        assertThat(e, hasToString(containsString("uncommittedSizeInBytes must be >= 0")));
    }

    public void testSnapshot() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.size(0));
        }

        addToTranslogAndList(translog, ops, new Translog.Index("test", "1", 0, new byte[]{1}));

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
            assertThat(snapshot.totalOperations(), equalTo(1));
        }

        try (Translog.Snapshot snapshot = translog.newSnapshot();
            Translog.Snapshot snapshot1 = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
            assertThat(snapshot.totalOperations(), equalTo(1));

            assertThat(snapshot1, SnapshotMatchers.size(1));
            assertThat(snapshot1.totalOperations(), equalTo(1));
        }
    }

    public void testSnapshotWithNewTranslog() throws IOException {
        List<Closeable> toClose = new ArrayList<>();
        try {
            ArrayList<Translog.Operation> ops = new ArrayList<>();
            Translog.Snapshot snapshot = translog.newSnapshot();
            toClose.add(snapshot);
            assertThat(snapshot, SnapshotMatchers.size(0));

            addToTranslogAndList(translog, ops, new Translog.Index("test", "1", 0, new byte[]{1}));
            Translog.Snapshot snapshot1 = translog.newSnapshot();
            toClose.add(snapshot1);

            addToTranslogAndList(translog, ops, new Translog.Index("test", "2", 1, new byte[]{2}));

            assertThat(snapshot1, SnapshotMatchers.equalsTo(ops.get(0)));

            translog.rollGeneration();
            addToTranslogAndList(translog, ops, new Translog.Index("test", "3", 2, new byte[]{3}));

            Translog.Snapshot snapshot2 = translog.newSnapshot();
            toClose.add(snapshot2);
            markCurrentGenAsCommitted(translog);
            assertThat(snapshot2, SnapshotMatchers.equalsTo(ops));
            assertThat(snapshot2.totalOperations(), equalTo(ops.size()));
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }
    }

    public void testSnapshotOnClosedTranslog() throws IOException {
        assertTrue(Files.exists(translogDir.resolve(Translog.getFilename(1))));
        translog.add(new Translog.Index("test", "1", 0, new byte[]{1}));
        translog.close();
        try {
            Translog.Snapshot snapshot = translog.newSnapshot();
            fail("translog is closed");
        } catch (AlreadyClosedException ex) {
            assertEquals(ex.getMessage(), "translog is already closed");
        }
    }

    public void assertFileIsPresent(Translog translog, long id) {
        if (Files.exists(translog.location().resolve(Translog.getFilename(id)))) {
            return;
        }
        fail(Translog.getFilename(id) + " is not present in any location: " + translog.location());
    }

    public void assertFileDeleted(Translog translog, long id) {
        assertFalse("translog [" + id + "] still exists", Files.exists(translog.location().resolve(Translog.getFilename(id))));
    }

    private void assertFilePresences(Translog translog) {
        for (long gen = translog.getMinFileGeneration(); gen < translog.currentFileGeneration(); gen++) {
            assertFileIsPresent(translog, gen);
        }
        for (long gen = 1; gen < translog.getMinFileGeneration(); gen++) {
            assertFileDeleted(translog, gen);
        }
    }

    static class LocationOperation implements Comparable<LocationOperation> {
        final Translog.Operation operation;
        final Translog.Location location;

        LocationOperation(Translog.Operation operation, Translog.Location location) {
            this.operation = operation;
            this.location = location;
        }

        @Override
        public int compareTo(LocationOperation o) {
            return location.compareTo(o.location);
        }
    }

    public void testConcurrentWritesWithVaryingSize() throws Throwable {
        final int opsPerThread = randomIntBetween(10, 200);
        int threadCount = 2 + randomInt(5);

        logger.info("testing with [{}] threads, each doing [{}] ops", threadCount, opsPerThread);
        final BlockingQueue<LocationOperation> writtenOperations = new ArrayBlockingQueue<>(threadCount * opsPerThread);

        Thread[] threads = new Thread[threadCount];
        final Exception[] threadExceptions = new Exception[threadCount];
        final AtomicLong seqNoGenerator = new AtomicLong();
        final CountDownLatch downLatch = new CountDownLatch(1);
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new TranslogThread(translog, downLatch, opsPerThread, threadId, writtenOperations, seqNoGenerator, threadExceptions);
            threads[i].setDaemon(true);
            threads[i].start();
        }

        downLatch.countDown();

        for (int i = 0; i < threadCount; i++) {
            if (threadExceptions[i] != null) {
                throw threadExceptions[i];
            }
            threads[i].join(60 * 1000);
        }

        List<LocationOperation> collect = new ArrayList<>(writtenOperations);
        Collections.sort(collect);
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            for (LocationOperation locationOperation : collect) {
                Translog.Operation op = snapshot.next();
                assertNotNull(op);
                Translog.Operation expectedOp = locationOperation.operation;
                assertEquals(expectedOp.opType(), op.opType());
                switch (op.opType()) {
                    case INDEX:
                        Translog.Index indexOp = (Translog.Index) op;
                        Translog.Index expIndexOp = (Translog.Index) expectedOp;
                        assertEquals(expIndexOp.id(), indexOp.id());
                        assertEquals(expIndexOp.routing(), indexOp.routing());
                        assertEquals(expIndexOp.type(), indexOp.type());
                        assertEquals(expIndexOp.source(), indexOp.source());
                        assertEquals(expIndexOp.version(), indexOp.version());
                        assertEquals(expIndexOp.versionType(), indexOp.versionType());
                        break;
                    case DELETE:
                        Translog.Delete delOp = (Translog.Delete) op;
                        Translog.Delete expDelOp = (Translog.Delete) expectedOp;
                        assertEquals(expDelOp.uid(), delOp.uid());
                        assertEquals(expDelOp.version(), delOp.version());
                        assertEquals(expDelOp.versionType(), delOp.versionType());
                        break;
                    case NO_OP:
                        final Translog.NoOp noOp = (Translog.NoOp) op;
                        final Translog.NoOp expectedNoOp = (Translog.NoOp) expectedOp;
                        assertThat(noOp.seqNo(), equalTo(expectedNoOp.seqNo()));
                        assertThat(noOp.primaryTerm(), equalTo(expectedNoOp.primaryTerm()));
                        assertThat(noOp.reason(), equalTo(expectedNoOp.reason()));
                        break;
                    default:
                        throw new AssertionError("unsupported operation type [" + op.opType() + "]");
                }
            }
            assertNull(snapshot.next());
        }

    }

    public void testTranslogChecksums() throws Exception {
        List<Translog.Location> locations = new ArrayList<>();

        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations; op++) {
            String ascii = randomAlphaOfLengthBetween(1, 50);
            locations.add(translog.add(new Translog.Index("test", "" + op, op, ascii.getBytes("UTF-8"))));
        }
        translog.sync();

        corruptTranslogs(translogDir);

        AtomicInteger corruptionsCaught = new AtomicInteger(0);
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            for (Translog.Location location : locations) {
                try {
                    Translog.Operation next = snapshot.next();
                    assertNotNull(next);
                } catch (TranslogCorruptedException e) {
                    corruptionsCaught.incrementAndGet();
                }
            }
            expectThrows(TranslogCorruptedException.class, snapshot::next);
            assertThat("at least one corruption was caused and caught", corruptionsCaught.get(), greaterThanOrEqualTo(1));
        }
    }

    public void testTruncatedTranslogs() throws Exception {
        List<Translog.Location> locations = new ArrayList<>();

        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations; op++) {
            String ascii = randomAlphaOfLengthBetween(1, 50);
            locations.add(translog.add(new Translog.Index("test", "" + op, op, ascii.getBytes("UTF-8"))));
        }
        translog.sync();

        truncateTranslogs(translogDir);

        AtomicInteger truncations = new AtomicInteger(0);
        try (Translog.Snapshot snap = translog.newSnapshot()) {
            for (Translog.Location location : locations) {
                try {
                    assertNotNull(snap.next());
                } catch (EOFException e) {
                    truncations.incrementAndGet();
                }
            }
        }
        assertThat("at least one truncation was caused and caught", truncations.get(), greaterThanOrEqualTo(1));
    }

    /**
     * Randomly truncate some bytes in the translog files
     */
    private void truncateTranslogs(Path directory) throws Exception {
        Path[] files = FileSystemUtils.files(directory, "translog-*");
        for (Path file : files) {
            try (FileChannel f = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                long prevSize = f.size();
                long newSize = prevSize - randomIntBetween(1, (int) prevSize / 2);
                logger.info("--> truncating {}, prev: {}, now: {}", file, prevSize, newSize);
                f.truncate(newSize);
            }
        }
    }


    /**
     * Randomly overwrite some bytes in the translog files
     */
    private void corruptTranslogs(Path directory) throws Exception {
        Path[] files = FileSystemUtils.files(directory, "translog-*");
        for (Path file : files) {
            logger.info("--> corrupting {}...", file);
            FileChannel f = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
            int corruptions = scaledRandomIntBetween(10, 50);
            for (int i = 0; i < corruptions; i++) {
                // note: with the current logic, this will sometimes be a no-op
                long pos = randomIntBetween(0, (int) f.size());
                ByteBuffer junk = ByteBuffer.wrap(new byte[]{randomByte()});
                f.write(junk, pos);
            }
            f.close();
        }
    }

    private Term newUid(ParsedDocument doc) {
        return new Term("_uid", Uid.createUidAsBytes(doc.type(), doc.id()));
    }

    private Term newUid(String uid) {
        return new Term("_uid", uid);
    }

    public void testVerifyTranslogIsNotDeleted() throws IOException {
        assertFileIsPresent(translog, 1);
        translog.add(new Translog.Index("test", "1", 0, new byte[]{1}));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.size(1));
            assertFileIsPresent(translog, 1);
            assertThat(snapshot.totalOperations(), equalTo(1));
        }
        translog.close();

        assertFileIsPresent(translog, 1);
    }

    /**
     * Tests that concurrent readers and writes maintain view and snapshot semantics
     */
    public void testConcurrentWriteViewsAndSnapshot() throws Throwable {
        final Thread[] writers = new Thread[randomIntBetween(1, 3)];
        final Thread[] readers = new Thread[randomIntBetween(1, 3)];
        final int flushEveryOps = randomIntBetween(5, 100);
        final int maxOps = randomIntBetween(200, 1000);
        final Object signalReaderSomeDataWasIndexed = new Object();
        final AtomicLong idGenerator = new AtomicLong();
        final CyclicBarrier barrier = new CyclicBarrier(writers.length + readers.length + 1);

        // a map of all written ops and their returned location.
        final Map<Translog.Operation, Translog.Location> writtenOps = ConcurrentCollections.newConcurrentMap();

        // a signal for all threads to stop
        final AtomicBoolean run = new AtomicBoolean(true);

        final Object flushMutex = new Object();
        final AtomicLong lastCommittedLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final LocalCheckpointTracker tracker = LocalCheckpointTrackerTests.createEmptyTracker();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        // any errors on threads
        final List<Exception> errors = new CopyOnWriteArrayList<>();
        logger.info("using [{}] readers. [{}] writers. flushing every ~[{}] ops.", readers.length, writers.length, flushEveryOps);
        for (int i = 0; i < writers.length; i++) {
            final String threadName = "writer_" + i;
            final int threadId = i;
            writers[i] = new Thread(new AbstractRunnable() {
                @Override
                public void doRun() throws BrokenBarrierException, InterruptedException, IOException {
                    barrier.await();
                    int counter = 0;
                    while (run.get() && idGenerator.get() < maxOps) {
                        long id = idGenerator.getAndIncrement();
                        final Translog.Operation op;
                        final Translog.Operation.Type type =
                            Translog.Operation.Type.values()[((int) (id % Translog.Operation.Type.values().length))];
                        switch (type) {
                            case CREATE:
                            case INDEX:
                                op = new Translog.Index("type", "" + id, id, new byte[]{(byte) id});
                                break;
                            case DELETE:
                                op = new Translog.Delete("test", Long.toString(id), id, newUid(Long.toString(id)));
                                break;
                            case NO_OP:
                                op = new Translog.NoOp(id, 1, Long.toString(id));
                                break;
                            default:
                                throw new AssertionError("unsupported operation type [" + type + "]");
                        }
                        Translog.Location location = translog.add(op);
                        tracker.markSeqNoAsCompleted(id);
                        Translog.Location existing = writtenOps.put(op, location);
                        if (existing != null) {
                            fail("duplicate op [" + op + "], old entry at " + location);
                        }
                        if (id % writers.length == threadId) {
                            translog.ensureSynced(location);
                        }
                        if (id % flushEveryOps == 0) {
                            synchronized (flushMutex) {
                                // we need not do this concurrently as we need to make sure that the generation
                                // we're committing - is still present when we're committing
                                long localCheckpoint = tracker.getCheckpoint();
                                translog.rollGeneration();
                                // expose the new checkpoint (simulating a commit), before we trim the translog
                                lastCommittedLocalCheckpoint.set(localCheckpoint);
                                deletionPolicy.setMinTranslogGenerationForRecovery(
                                    translog.getMinGenerationForSeqNo(localCheckpoint + 1).translogFileGeneration);
                                translog.trimUnreferencedReaders();
                            }
                        }
                        if (id % 7 == 0) {
                            synchronized (signalReaderSomeDataWasIndexed) {
                                signalReaderSomeDataWasIndexed.notifyAll();
                            }
                        }
                        counter++;
                    }
                    logger.info("--> [{}] done. wrote [{}] ops.", threadName, counter);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error((Supplier<?>) () -> new ParameterizedMessage("--> writer [{}] had an error", threadName), e);
                    errors.add(e);
                }
            }, threadName);
            writers[i].start();
        }

        for (int i = 0; i < readers.length; i++) {
            final String threadId = "reader_" + i;
            readers[i] = new Thread(new AbstractRunnable() {
                Closeable retentionLock = null;
                long committedLocalCheckpointAtView;

                @Override
                public void onFailure(Exception e) {
                    logger.error((Supplier<?>) () -> new ParameterizedMessage("--> reader [{}] had an error", threadId), e);
                    errors.add(e);
                    try {
                        closeRetentionLock();
                    } catch (IOException inner) {
                        inner.addSuppressed(e);
                        logger.error("unexpected error while closing view, after failure", inner);
                    }
                }

                void closeRetentionLock() throws IOException {
                    if (retentionLock != null) {
                        retentionLock.close();
                    }
                }

                void acquireRetentionLock() throws IOException {
                    closeRetentionLock();
                    retentionLock = translog.acquireRetentionLock();
                    // captures the last committed checkpoint, while holding the view, simulating
                    // recovery logic which captures a view and gets a lucene commit
                    committedLocalCheckpointAtView = lastCommittedLocalCheckpoint.get();
                    logger.info("--> [{}] min gen after acquiring lock [{}]", threadId, translog.getMinFileGeneration());
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    int iter = 0;
                    while (idGenerator.get() < maxOps) {
                        if (iter++ % 10 == 0) {
                            acquireRetentionLock();
                        }

                        // captures al views that are written since the view was created (with a small caveat see bellow)
                        // these are what we expect the snapshot to return (and potentially some more).
                        Set<Translog.Operation> expectedOps = new HashSet<>(writtenOps.keySet());
                        expectedOps.removeIf(op -> op.seqNo() <= committedLocalCheckpointAtView);
                        try (Translog.Snapshot snapshot = translog.newSnapshotFromMinSeqNo(committedLocalCheckpointAtView + 1L)) {
                            Translog.Operation op;
                            while ((op = snapshot.next()) != null) {
                                expectedOps.remove(op);
                            }
                        }
                        if (expectedOps.isEmpty() == false) {
                            StringBuilder missed = new StringBuilder("missed ").append(expectedOps.size())
                                .append(" operations from [").append(committedLocalCheckpointAtView + 1L).append("]");
                            boolean failed = false;
                            for (Translog.Operation expectedOp : expectedOps) {
                                final Translog.Location loc = writtenOps.get(expectedOp);
                                failed = true;
                                missed.append("\n --> [").append(expectedOp).append("] written at ").append(loc);
                            }
                            if (failed) {
                                fail(missed.toString());
                            }
                        }
                        // slow down things a bit and spread out testing..
                        synchronized (signalReaderSomeDataWasIndexed) {
                            if (idGenerator.get() < maxOps) {
                                signalReaderSomeDataWasIndexed.wait();
                            }
                        }
                    }
                    closeRetentionLock();
                    logger.info("--> [{}] done. tested [{}] snapshots", threadId, iter);
                }
            }, threadId);
            readers[i].start();
        }

        barrier.await();
        logger.debug("--> waiting for threads to stop");
        for (Thread thread : writers) {
            thread.join();
        }
        logger.debug("--> waiting for readers to stop");
        // force stopping, if all writers crashed
        synchronized (signalReaderSomeDataWasIndexed) {
            idGenerator.set(Long.MAX_VALUE);
            signalReaderSomeDataWasIndexed.notifyAll();
        }
        for (Thread thread : readers) {
            thread.join();
        }
        if (errors.size() > 0) {
            Throwable e = errors.get(0);
            for (Throwable suppress : errors.subList(1, errors.size())) {
                e.addSuppressed(suppress);
            }
            throw e;
        }
        logger.info("--> test done. total ops written [{}]", writtenOps.size());
    }

    public void testSyncUpTo() throws IOException {
        int translogOperations = randomIntBetween(10, 100);
        int count = 0;
        for (int op = 0; op < translogOperations; op++) {
            int seqNo = ++count;
            final Translog.Location location =
                translog.add(new Translog.Index("test", "" + op, seqNo, Integer.toString(seqNo).getBytes(Charset.forName("UTF-8"))));
            if (randomBoolean()) {
                assertTrue("at least one operation pending", translog.syncNeeded());
                assertTrue("this operation has not been synced", translog.ensureSynced(location));
                assertFalse("the last call to ensureSycned synced all previous ops", translog.syncNeeded()); // we are the last location so everything should be synced
                seqNo = ++count;
                translog.add(new Translog.Index("test", "" + op, seqNo, Integer.toString(seqNo).getBytes(Charset.forName("UTF-8"))));
                assertTrue("one pending operation", translog.syncNeeded());
                assertFalse("this op has been synced before", translog.ensureSynced(location)); // not syncing now
                assertTrue("we only synced a previous operation yet", translog.syncNeeded());
            }
            if (rarely()) {
                rollAndCommit(translog);
                assertFalse("location is from a previous translog - already synced", translog.ensureSynced(location)); // not syncing now
                assertFalse("no sync needed since no operations in current translog", translog.syncNeeded());
            }

            if (randomBoolean()) {
                translog.sync();
                assertFalse("translog has been synced already", translog.ensureSynced(location));
            }
        }
    }

    public void testSyncUpToStream() throws IOException {
        int iters = randomIntBetween(5, 10);
        for (int i = 0; i < iters; i++) {
            int translogOperations = randomIntBetween(10, 100);
            int count = 0;
            ArrayList<Location> locations = new ArrayList<>();
            for (int op = 0; op < translogOperations; op++) {
                if (rarely()) {
                    rollAndCommit(translog); // do this first so that there is at least one pending tlog entry
                }
                final Translog.Location location =
                    translog.add(new Translog.Index("test", "" + op, op, Integer.toString(++count).getBytes(Charset.forName("UTF-8"))));
                locations.add(location);
            }
            Collections.shuffle(locations, random());
            if (randomBoolean()) {
                assertTrue("at least one operation pending", translog.syncNeeded());
                assertTrue("this operation has not been synced", translog.ensureSynced(locations.stream()));
                assertFalse("the last call to ensureSycned synced all previous ops", translog.syncNeeded()); // we are the last location so everything should be synced
            } else if (rarely()) {
                rollAndCommit(translog);
                assertFalse("location is from a previous translog - already synced", translog.ensureSynced(locations.stream())); // not syncing now
                assertFalse("no sync needed since no operations in current translog", translog.syncNeeded());
            } else {
                translog.sync();
                assertFalse("translog has been synced already", translog.ensureSynced(locations.stream()));
            }
            for (Location location : locations) {
                assertFalse("all of the locations should be synced: " + location, translog.ensureSynced(location));
            }
        }
    }

    public void testLocationComparison() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        int count = 0;
        for (int op = 0; op < translogOperations; op++) {
            locations.add(
                translog.add(new Translog.Index("test", "" + op, op, Integer.toString(++count).getBytes(Charset.forName("UTF-8")))));
            if (rarely() && translogOperations > op + 1) {
                rollAndCommit(translog);
            }
        }
        Collections.shuffle(locations, random());
        Translog.Location max = locations.get(0);
        for (Translog.Location location : locations) {
            max = max(max, location);
        }

        assertEquals(max.generation, translog.currentFileGeneration());
        try (Translog.Snapshot snap = translog.newSnapshot()) {
            Translog.Operation next;
            Translog.Operation maxOp = null;
            while ((next = snap.next()) != null) {
                maxOp = next;
            }
            assertNotNull(maxOp);
            assertEquals(maxOp.getSource().source.utf8ToString(), Integer.toString(count));
        }
    }

    public static Translog.Location max(Translog.Location a, Translog.Location b) {
        if (a.compareTo(b) > 0) {
            return a;
        }
        return b;
    }


    public void testBasicCheckpoint() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        int lastSynced = -1;
        long lastSyncedGlobalCheckpoint = globalCheckpoint.get();
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (randomBoolean()) {
                globalCheckpoint.set(globalCheckpoint.get() + randomIntBetween(1, 16));
            }
            if (frequently()) {
                translog.sync();
                lastSynced = op;
                lastSyncedGlobalCheckpoint = globalCheckpoint.get();
            }
        }
        assertEquals(translogOperations, translog.totalOperations());
        translog.add(new Translog.Index(
            "test", "" + translogOperations, translogOperations, Integer.toString(translogOperations).getBytes(Charset.forName("UTF-8"))));

        final Checkpoint checkpoint = Checkpoint.read(translog.location().resolve(Translog.CHECKPOINT_FILE_NAME));
        try (TranslogReader reader = translog.openReader(translog.location().resolve(Translog.getFilename(translog.currentFileGeneration())), checkpoint)) {
            assertEquals(lastSynced + 1, reader.totalOperations());
            TranslogSnapshot snapshot = reader.newSnapshot();

            for (int op = 0; op < translogOperations; op++) {
                if (op <= lastSynced) {
                    final Translog.Operation read = snapshot.next();
                    assertEquals(Integer.toString(op), read.getSource().source.utf8ToString());
                } else {
                    Translog.Operation next = snapshot.next();
                    assertNull(next);
                }
            }
            Translog.Operation next = snapshot.next();
            assertNull(next);
        }
        assertEquals(translogOperations + 1, translog.totalOperations());
        assertThat(checkpoint.globalCheckpoint, equalTo(lastSyncedGlobalCheckpoint));
        translog.close();
    }

    public void testTranslogWriter() throws IOException {
        final TranslogWriter writer = translog.createWriter(translog.currentFileGeneration() + 1);
        final int numOps = randomIntBetween(8, 128);
        byte[] bytes = new byte[4];
        ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
        final Set<Long> seenSeqNos = new HashSet<>();
        boolean opsHaveValidSequenceNumbers = randomBoolean();
        for (int i = 0; i < numOps; i++) {
            out.reset(bytes);
            out.writeInt(i);
            long seqNo;
            do {
                seqNo = opsHaveValidSequenceNumbers ? randomNonNegativeLong() : SequenceNumbers.UNASSIGNED_SEQ_NO;
                opsHaveValidSequenceNumbers = opsHaveValidSequenceNumbers || !rarely();
            } while (seenSeqNos.contains(seqNo));
            if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                seenSeqNos.add(seqNo);
            }
            writer.add(new BytesArray(bytes), seqNo);
        }
        writer.sync();

        final BaseTranslogReader reader = randomBoolean() ? writer : translog.openReader(writer.path(), Checkpoint.read(translog.location().resolve(Translog.CHECKPOINT_FILE_NAME)));
        for (int i = 0; i < numOps; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            reader.readBytes(buffer, reader.getFirstOperationOffset() + 4 * i);
            buffer.flip();
            final int value = buffer.getInt();
            assertEquals(i, value);
        }
        final long minSeqNo = seenSeqNos.stream().min(Long::compareTo).orElse(SequenceNumbers.NO_OPS_PERFORMED);
        final long maxSeqNo = seenSeqNos.stream().max(Long::compareTo).orElse(SequenceNumbers.NO_OPS_PERFORMED);
        assertThat(reader.getCheckpoint().minSeqNo, equalTo(minSeqNo));
        assertThat(reader.getCheckpoint().maxSeqNo, equalTo(maxSeqNo));

        out.reset(bytes);
        out.writeInt(2048);
        writer.add(new BytesArray(bytes), randomNonNegativeLong());

        if (reader instanceof TranslogReader) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            try {
                reader.readBytes(buffer, reader.getFirstOperationOffset() + 4 * numOps);
                fail("read past EOF?");
            } catch (EOFException ex) {
                // expected
            }
            ((TranslogReader) reader).close();
        } else {
            // live reader!
            ByteBuffer buffer = ByteBuffer.allocate(4);
            final long pos = reader.getFirstOperationOffset() + 4 * numOps;
            reader.readBytes(buffer, pos);
            buffer.flip();
            final int value = buffer.getInt();
            assertEquals(2048, value);
        }
        IOUtils.close(writer);
    }

    public void testCloseIntoReader() throws IOException {
        try (TranslogWriter writer = translog.createWriter(translog.currentFileGeneration() + 1)) {
            final int numOps = randomIntBetween(8, 128);
            final byte[] bytes = new byte[4];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
            for (int i = 0; i < numOps; i++) {
                out.reset(bytes);
                out.writeInt(i);
                writer.add(new BytesArray(bytes), randomNonNegativeLong());
            }
            writer.sync();
            final Checkpoint writerCheckpoint = writer.getCheckpoint();
            TranslogReader reader = writer.closeIntoReader();
            try {
                if (randomBoolean()) {
                    reader.close();
                    reader = translog.openReader(reader.path(), writerCheckpoint);
                }
                for (int i = 0; i < numOps; i++) {
                    final ByteBuffer buffer = ByteBuffer.allocate(4);
                    reader.readBytes(buffer, reader.getFirstOperationOffset() + 4 * i);
                    buffer.flip();
                    final int value = buffer.getInt();
                    assertEquals(i, value);
                }
                final Checkpoint readerCheckpoint = reader.getCheckpoint();
                assertThat(readerCheckpoint, equalTo(writerCheckpoint));
            } finally {
                IOUtils.close(reader);
            }
        }
    }

    public void testBasicRecovery() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        Translog.TranslogGeneration translogGeneration = null;
        int minUncommittedOp = -1;
        final boolean commitOften = randomBoolean();
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            final boolean commit = commitOften ? frequently() : rarely();
            if (commit && op < translogOperations - 1) {
                rollAndCommit(translog);
                minUncommittedOp = op + 1;
                translogGeneration = translog.getGeneration();
            }
        }
        translog.sync();
        TranslogConfig config = translog.getConfig();

        translog.close();
        if (translogGeneration == null) {
            translog = createTranslog(config, null);
            assertEquals(0, translog.stats().estimatedNumberOfOperations());
            assertEquals(1, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            try(Translog.Snapshot snapshot = translog.newSnapshot()) {
                assertNull(snapshot.next());
            }
        } else {
            translog = new Translog(config, translogGeneration.translogUUID, translog.getDeletionPolicy(), () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
            assertEquals("lastCommitted must be 1 less than current", translogGeneration.translogFileGeneration + 1, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            try (Translog.Snapshot snapshot = translog.newSnapshotFromGen(translogGeneration.translogFileGeneration)) {
                for (int i = minUncommittedOp; i < translogOperations; i++) {
                    assertEquals("expected operation" + i + " to be in the previous translog but wasn't",
                        translog.currentFileGeneration() - 1, locations.get(i).generation);
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null", next);
                    assertEquals(i, Integer.parseInt(next.getSource().source.utf8ToString()));
                }
            }
        }
    }

    public void testRecoveryUncommitted() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        final int prepareOp = randomIntBetween(0, translogOperations - 1);
        Translog.TranslogGeneration translogGeneration = null;
        final boolean sync = randomBoolean();
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (op == prepareOp) {
                translogGeneration = translog.getGeneration();
                translog.rollGeneration();
                assertEquals("expected this to be the first commit", 1L, translogGeneration.translogFileGeneration);
                assertNotNull(translogGeneration.translogUUID);
            }
        }
        if (sync) {
            translog.sync();
        }
        // we intentionally don't close the tlog that is in the prepareCommit stage since we try to recovery the uncommitted
        // translog here as well.
        TranslogConfig config = translog.getConfig();
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        try (Translog translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO)) {
            assertNotNull(translogGeneration);
            assertEquals("lastCommitted must be 2 less than current - we never finished the commit", translogGeneration.translogFileGeneration + 2, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                int upTo = sync ? translogOperations : prepareOp;
                for (int i = 0; i < upTo; i++) {
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                    assertEquals("payload mismatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.utf8ToString()));
                }
            }
        }
        if (randomBoolean()) { // recover twice
            try (Translog translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO)) {
                assertNotNull(translogGeneration);
                assertEquals("lastCommitted must be 3 less than current - we never finished the commit and run recovery twice", translogGeneration.translogFileGeneration + 3, translog.currentFileGeneration());
                assertFalse(translog.syncNeeded());
                try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                    int upTo = sync ? translogOperations : prepareOp;
                    for (int i = 0; i < upTo; i++) {
                        Translog.Operation next = snapshot.next();
                        assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                        assertEquals("payload mismatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.utf8ToString()));
                    }
                }
            }
        }
    }


    public void testRecoveryUncommittedFileExists() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        final int prepareOp = randomIntBetween(0, translogOperations - 1);
        Translog.TranslogGeneration translogGeneration = null;
        final boolean sync = randomBoolean();
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (op == prepareOp) {
                translogGeneration = translog.getGeneration();
                translog.rollGeneration();
                assertEquals("expected this to be the first commit", 1L, translogGeneration.translogFileGeneration);
                assertNotNull(translogGeneration.translogUUID);
            }
        }
        if (sync) {
            translog.sync();
        }
        // we intentionally don't close the tlog that is in the prepareCommit stage since we try to recovery the uncommitted
        // translog here as well.
        TranslogConfig config = translog.getConfig();
        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        Files.copy(ckp, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)));

        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        try (Translog translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO)) {
            assertNotNull(translogGeneration);
            assertEquals("lastCommitted must be 2 less than current - we never finished the commit", translogGeneration.translogFileGeneration + 2, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                int upTo = sync ? translogOperations : prepareOp;
                for (int i = 0; i < upTo; i++) {
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                    assertEquals("payload mismatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.utf8ToString()));
                }
            }
        }

        if (randomBoolean()) { // recover twice
            try (Translog translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO)) {
                assertNotNull(translogGeneration);
                assertEquals("lastCommitted must be 3 less than current - we never finished the commit and run recovery twice", translogGeneration.translogFileGeneration + 3, translog.currentFileGeneration());
                assertFalse(translog.syncNeeded());
                try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                    int upTo = sync ? translogOperations : prepareOp;
                    for (int i = 0; i < upTo; i++) {
                        Translog.Operation next = snapshot.next();
                        assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                        assertEquals("payload mismatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.utf8ToString()));
                    }
                }
            }
        }
    }

    public void testRecoveryUncommittedCorruptedCheckpoint() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = 100;
        final int prepareOp = 44;
        Translog.TranslogGeneration translogGeneration = null;
        final boolean sync = randomBoolean();
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (op == prepareOp) {
                translogGeneration = translog.getGeneration();
                translog.rollGeneration();
                assertEquals("expected this to be the first commit", 1L, translogGeneration.translogFileGeneration);
                assertNotNull(translogGeneration.translogUUID);
            }
        }
        translog.sync();
        // we intentionally don't close the tlog that is in the prepareCommit stage since we try to recovery the uncommitted
        // translog here as well.
        TranslogConfig config = translog.getConfig();
        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        Checkpoint corrupted = Checkpoint.emptyTranslogCheckpoint(0, 0, SequenceNumbers.UNASSIGNED_SEQ_NO, 0);
        Checkpoint.write(FileChannel::open, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)), corrupted, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        try (Translog ignored = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO)) {
            fail("corrupted");
        } catch (IllegalStateException ex) {
            assertEquals("Checkpoint file translog-2.ckp already exists but has corrupted content expected: Checkpoint{offset=3123, " +
                "numOps=55, generation=2, minSeqNo=45, maxSeqNo=99, globalCheckpoint=-2, minTranslogGeneration=1} but got: Checkpoint{offset=0, numOps=0, " +
                "generation=0, minSeqNo=-1, maxSeqNo=-1, globalCheckpoint=-2, minTranslogGeneration=0}", ex.getMessage());
        }
        Checkpoint.write(FileChannel::open, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)), read, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        try (Translog translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO)) {
            assertNotNull(translogGeneration);
            assertEquals("lastCommitted must be 2 less than current - we never finished the commit", translogGeneration.translogFileGeneration + 2, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                int upTo = sync ? translogOperations : prepareOp;
                for (int i = 0; i < upTo; i++) {
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                    assertEquals("payload mismatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.utf8ToString()));
                }
            }
        }
    }

    public void testSnapshotFromStreamInput() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        List<Translog.Operation> ops = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations; op++) {
            Translog.Index test = new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8")));
            ops.add(test);
        }
        Translog.writeOperations(out, ops);
        final List<Translog.Operation> readOperations = Translog.readOperations(out.bytes().streamInput());
        assertEquals(ops.size(), readOperations.size());
        assertEquals(ops, readOperations);
    }

    public void testLocationHashCodeEquals() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        List<Translog.Location> locations2 = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        try (Translog translog2 = create(createTempDir())) {
            for (int op = 0; op < translogOperations; op++) {
                locations.add(translog.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
                locations2.add(translog2.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            }
            int iters = randomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                Translog.Location location = RandomPicks.randomFrom(random(), locations);
                for (Translog.Location loc : locations) {
                    if (loc == location) {
                        assertTrue(loc.equals(location));
                        assertEquals(loc.hashCode(), location.hashCode());
                    } else {
                        assertFalse(loc.equals(location));
                    }
                }
                for (int j = 0; j < translogOperations; j++) {
                    assertTrue(locations.get(j).equals(locations2.get(j)));
                    assertEquals(locations.get(j).hashCode(), locations2.get(j).hashCode());
                }
            }
        }
    }

    public void testOpenForeignTranslog() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(1, 10);
        int firstUncommitted = 0;
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (randomBoolean()) {
                rollAndCommit(translog);
                firstUncommitted = op + 1;
            }
        }
        final TranslogConfig config = translog.getConfig();
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        translog.close();

        final String foreignTranslog = randomRealisticUnicodeOfCodepointLengthBetween(1,
            translogGeneration.translogUUID.length());
        try {
            new Translog(config, foreignTranslog, createTranslogDeletionPolicy(), () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
            fail("translog doesn't belong to this UUID");
        } catch (TranslogCorruptedException ex) {

        }
        this.translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
        try (Translog.Snapshot snapshot = this.translog.newSnapshotFromGen(translogGeneration.translogFileGeneration)) {
            for (int i = firstUncommitted; i < translogOperations; i++) {
                Translog.Operation next = snapshot.next();
                assertNotNull("" + i, next);
                assertEquals(Integer.parseInt(next.getSource().source.utf8ToString()), i);
            }
            assertNull(snapshot.next());
        }
    }

    public void testFailOnClosedWrite() throws IOException {
        translog.add(new Translog.Index("test", "1", 0, Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
        translog.close();
        try {
            translog.add(new Translog.Index("test", "1", 0, Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
            fail("closed");
        } catch (AlreadyClosedException ex) {
            // all is well
        }
    }

    public void testCloseConcurrently() throws Throwable {
        final int opsPerThread = randomIntBetween(10, 200);
        int threadCount = 2 + randomInt(5);

        logger.info("testing with [{}] threads, each doing [{}] ops", threadCount, opsPerThread);
        final BlockingQueue<LocationOperation> writtenOperations = new ArrayBlockingQueue<>(threadCount * opsPerThread);

        Thread[] threads = new Thread[threadCount];
        final Exception[] threadExceptions = new Exception[threadCount];
        final CountDownLatch downLatch = new CountDownLatch(1);
        final AtomicLong seqNoGenerator = new AtomicLong();
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new TranslogThread(translog, downLatch, opsPerThread, threadId, writtenOperations, seqNoGenerator, threadExceptions);
            threads[i].setDaemon(true);
            threads[i].start();
        }

        downLatch.countDown();
        translog.close();

        for (int i = 0; i < threadCount; i++) {
            if (threadExceptions[i] != null) {
                if ((threadExceptions[i] instanceof AlreadyClosedException) == false) {
                    throw threadExceptions[i];
                }
            }
            threads[i].join(60 * 1000);
        }
    }

    private static class TranslogThread extends Thread {
        private final CountDownLatch downLatch;
        private final int opsPerThread;
        private final int threadId;
        private final Collection<LocationOperation> writtenOperations;
        private final Exception[] threadExceptions;
        private final Translog translog;
        private final AtomicLong seqNoGenerator;

        TranslogThread(Translog translog, CountDownLatch downLatch, int opsPerThread, int threadId,
                       Collection<LocationOperation> writtenOperations, AtomicLong seqNoGenerator, Exception[] threadExceptions) {
            this.translog = translog;
            this.downLatch = downLatch;
            this.opsPerThread = opsPerThread;
            this.threadId = threadId;
            this.writtenOperations = writtenOperations;
            this.seqNoGenerator = seqNoGenerator;
            this.threadExceptions = threadExceptions;
        }

        @Override
        public void run() {
            try {
                downLatch.await();
                for (int opCount = 0; opCount < opsPerThread; opCount++) {
                    Translog.Operation op;
                    final Translog.Operation.Type type = randomFrom(Translog.Operation.Type.values());
                    switch (type) {
                        case CREATE:
                        case INDEX:
                            op = new Translog.Index("test", threadId + "_" + opCount, seqNoGenerator.getAndIncrement(),
                                randomUnicodeOfLengthBetween(1, 20 * 1024).getBytes("UTF-8"));
                            break;
                        case DELETE:
                            op = new Translog.Delete(
                                "test", threadId + "_" + opCount,
                                new Term("_uid", threadId + "_" + opCount),
                                seqNoGenerator.getAndIncrement(),
                                0,
                                1 + randomInt(100000),
                                randomFrom(VersionType.values()));
                            break;
                        case NO_OP:
                            op = new Translog.NoOp(seqNoGenerator.getAndIncrement(), randomNonNegativeLong(), randomAlphaOfLength(16));
                            break;
                        default:
                            throw new AssertionError("unsupported operation type [" + type + "]");
                    }

                    Translog.Location loc = add(op);
                    writtenOperations.add(new LocationOperation(op, loc));
                    afterAdd();
                }
            } catch (Exception t) {
                threadExceptions[threadId] = t;
            }
        }

        protected Translog.Location add(Translog.Operation op) throws IOException {
            return translog.add(op);
        }

        protected void afterAdd() throws IOException {
        }
    }

    public void testFailFlush() throws IOException {
        Path tempDir = createTempDir();
        final FailSwitch fail = new FailSwitch();
        TranslogConfig config = getTranslogConfig(tempDir);
        Translog translog = getFailableTranslog(fail, config);

        List<Translog.Location> locations = new ArrayList<>();
        int opsSynced = 0;
        boolean failed = false;
        while (failed == false) {
            try {
                locations.add(translog.add(
                    new Translog.Index("test", "" + opsSynced, opsSynced, Integer.toString(opsSynced).getBytes(Charset.forName("UTF-8")))));
                translog.sync();
                opsSynced++;
            } catch (MockDirectoryWrapper.FakeIOException ex) {
                failed = true;
                assertFalse(translog.isOpen());
            } catch (IOException ex) {
                failed = true;
                assertFalse(translog.isOpen());
                assertEquals("__FAKE__ no space left on device", ex.getMessage());
            }
            if (randomBoolean()) {
                fail.failAlways();
            } else {
                fail.failNever();
            }
        }
        fail.failNever();
        if (randomBoolean()) {
            try {
                locations.add(translog.add(
                    new Translog.Index("test", "" + opsSynced, opsSynced, Integer.toString(opsSynced).getBytes(Charset.forName("UTF-8")))));
                fail("we are already closed");
            } catch (AlreadyClosedException ex) {
                assertNotNull(ex.getCause());
                if (ex.getCause() instanceof MockDirectoryWrapper.FakeIOException) {
                    assertNull(ex.getCause().getMessage());
                } else {
                    assertEquals(ex.getCause().getMessage(), "__FAKE__ no space left on device");
                }
            }

        }
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        try {
            translog.newSnapshot();
            fail("already closed");
        } catch (AlreadyClosedException ex) {
            // all is well
            assertNotNull(ex.getCause());
            assertSame(translog.getTragicException(), ex.getCause());
        }

        try {
            rollAndCommit(translog);
            fail("already closed");
        } catch (AlreadyClosedException ex) {
            assertNotNull(ex.getCause());
            assertSame(translog.getTragicException(), ex.getCause());
        }

        assertFalse(translog.isOpen());
        translog.close(); // we are closed
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        try (Translog tlog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO)) {
            assertEquals("lastCommitted must be 1 less than current", translogGeneration.translogFileGeneration + 1, tlog.currentFileGeneration());
            assertFalse(tlog.syncNeeded());

            try (Translog.Snapshot snapshot = tlog.newSnapshot()) {
                assertEquals(opsSynced, snapshot.totalOperations());
                for (int i = 0; i < opsSynced; i++) {
                    assertEquals("expected operation" + i + " to be in the previous translog but wasn't", tlog.currentFileGeneration() - 1,
                        locations.get(i).generation);
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null", next);
                    assertEquals(i, Integer.parseInt(next.getSource().source.utf8ToString()));
                }
            }
        }
    }

    public void testTranslogOpsCountIsCorrect() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int numOps = randomIntBetween(100, 200);
        LineFileDocs lineFileDocs = new LineFileDocs(random()); // writes pretty big docs so we cross buffer borders regularly
        for (int opsAdded = 0; opsAdded < numOps; opsAdded++) {
            locations.add(translog.add(
                new Translog.Index("test", "" + opsAdded, opsAdded, lineFileDocs.nextDoc().toString().getBytes(Charset.forName("UTF-8")))));
            try (Translog.Snapshot snapshot = this.translog.newSnapshot()) {
                assertEquals(opsAdded + 1, snapshot.totalOperations());
                for (int i = 0; i < opsAdded; i++) {
                    assertEquals("expected operation" + i + " to be in the current translog but wasn't", translog.currentFileGeneration(),
                        locations.get(i).generation);
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null", next);
                }
            }
        }
    }

    public void testTragicEventCanBeAnyException() throws IOException {
        Path tempDir = createTempDir();
        final FailSwitch fail = new FailSwitch();
        TranslogConfig config = getTranslogConfig(tempDir);
        Translog translog = getFailableTranslog(fail, config, false, true, null, createTranslogDeletionPolicy());
        LineFileDocs lineFileDocs = new LineFileDocs(random()); // writes pretty big docs so we cross buffer boarders regularly
        translog.add(new Translog.Index("test", "1", 0, lineFileDocs.nextDoc().toString().getBytes(Charset.forName("UTF-8"))));
        fail.failAlways();
        try {
            Translog.Location location = translog.add(
                new Translog.Index("test", "2", 1, lineFileDocs.nextDoc().toString().getBytes(Charset.forName("UTF-8"))));
            if (randomBoolean()) {
                translog.ensureSynced(location);
            } else {
                translog.sync();
            }
            //TODO once we have a mock FS that can simulate we can also fail on plain sync
            fail("WTF");
        } catch (UnknownException ex) {
            // w00t
        } catch (TranslogException ex) {
            assertTrue(ex.getCause() instanceof UnknownException);
        }
        assertFalse(translog.isOpen());
        assertTrue(translog.getTragicException() instanceof UnknownException);
    }

    public void testFatalIOExceptionsWhileWritingConcurrently() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        final FailSwitch fail = new FailSwitch();

        TranslogConfig config = getTranslogConfig(tempDir);
        Translog translog = getFailableTranslog(fail, config);
        final String translogUUID = translog.getTranslogUUID();

        final int threadCount = randomIntBetween(1, 5);
        Thread[] threads = new Thread[threadCount];
        final Exception[] threadExceptions = new Exception[threadCount];
        final CountDownLatch downLatch = new CountDownLatch(1);
        final CountDownLatch added = new CountDownLatch(randomIntBetween(10, 100));
        final AtomicLong seqNoGenerator = new AtomicLong();
        List<LocationOperation> writtenOperations = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new TranslogThread(translog, downLatch, 200, threadId, writtenOperations, seqNoGenerator, threadExceptions) {
                @Override
                protected Translog.Location add(Translog.Operation op) throws IOException {
                    Translog.Location add = super.add(op);
                    added.countDown();
                    return add;
                }

                @Override
                protected void afterAdd() throws IOException {
                    if (randomBoolean()) {
                        translog.sync();
                    }
                }
            };
            threads[i].setDaemon(true);
            threads[i].start();
        }
        downLatch.countDown();
        added.await();
        try (Closeable ignored = translog.acquireRetentionLock()) {
            // this holds a reference to the current tlog channel such that it's not closed
            // if we hit a tragic event. this is important to ensure that asserts inside the Translog#add doesn't trip
            // otherwise our assertions here are off by one sometimes.
            fail.failAlways();
            for (int i = 0; i < threadCount; i++) {
                threads[i].join();
            }
            boolean atLeastOneFailed = false;
            for (Throwable ex : threadExceptions) {
                if (ex != null) {
                    assertTrue(ex.toString(), ex instanceof IOException || ex instanceof AlreadyClosedException);
                    atLeastOneFailed = true;
                }
            }
            if (atLeastOneFailed == false) {
                try {
                    boolean syncNeeded = translog.syncNeeded();
                    translog.close();
                    assertFalse("should have failed if sync was needed", syncNeeded);
                } catch (IOException ex) {
                    // boom now we failed
                }
            }
            Collections.sort(writtenOperations, (a, b) -> a.location.compareTo(b.location));
            assertFalse(translog.isOpen());
            final Checkpoint checkpoint = Checkpoint.read(config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME));
            Iterator<LocationOperation> iterator = writtenOperations.iterator();
            while (iterator.hasNext()) {
                LocationOperation next = iterator.next();
                if (checkpoint.offset < (next.location.translogLocation + next.location.size)) {
                    // drop all that haven't been synced
                    iterator.remove();
                }
            }
            try (Translog tlog =
                     new Translog(config, translogUUID, createTranslogDeletionPolicy(), () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
                 Translog.Snapshot snapshot = tlog.newSnapshot()) {
                if (writtenOperations.size() != snapshot.totalOperations()) {
                    for (int i = 0; i < threadCount; i++) {
                        if (threadExceptions[i] != null) {
                            logger.info("Translog exception", threadExceptions[i]);
                        }
                    }
                }
                assertEquals(writtenOperations.size(), snapshot.totalOperations());
                for (int i = 0; i < writtenOperations.size(); i++) {
                    assertEquals("expected operation" + i + " to be in the previous translog but wasn't", tlog.currentFileGeneration() - 1, writtenOperations.get(i).location.generation);
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null", next);
                    assertEquals(next, writtenOperations.get(i).operation);
                }
            }
        }
    }

    /**
     * Tests the situation where the node crashes after a translog gen was committed to lucene, but before the translog had the chance
     * to clean up its files.
     */
    public void testRecoveryFromAFutureGenerationCleansUp() throws IOException {
        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations / 2; op++) {
            translog.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8"))));
            if (rarely()) {
                translog.rollGeneration();
            }
        }
        translog.rollGeneration();
        long comittedGeneration = randomLongBetween(2, translog.currentFileGeneration());
        for (int op = translogOperations / 2; op < translogOperations; op++) {
            translog.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8"))));
            if (rarely()) {
                translog.rollGeneration();
            }
        }
        // engine blows up, after committing the above generation
        translog.close();
        TranslogConfig config = translog.getConfig();
        final TranslogDeletionPolicy deletionPolicy = new TranslogDeletionPolicy(-1, -1);
        deletionPolicy.setMinTranslogGenerationForRecovery(comittedGeneration);
        translog = new Translog(config, translog.getTranslogUUID(), deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
        assertThat(translog.getMinFileGeneration(), equalTo(1L));
        // no trimming done yet, just recovered
        for (long gen = 1; gen < translog.currentFileGeneration(); gen++) {
            assertFileIsPresent(translog, gen);
        }
        translog.trimUnreferencedReaders();
        for (long gen = 1; gen < comittedGeneration; gen++) {
            assertFileDeleted(translog, gen);
        }
    }

    /**
     * Tests the situation where the node crashes after a translog gen was committed to lucene, but before the translog had the chance
     * to clean up its files.
     */
    public void testRecoveryFromFailureOnTrimming() throws IOException {
        Path tempDir = createTempDir();
        final FailSwitch fail = new FailSwitch();
        fail.failNever();
        final TranslogConfig config = getTranslogConfig(tempDir);
        final long comittedGeneration;
        final String translogUUID;
        try (Translog translog = getFailableTranslog(fail, config)) {
            final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
            // disable retention so we trim things
            deletionPolicy.setRetentionSizeInBytes(-1);
            deletionPolicy.setRetentionAgeInMillis(-1);
            translogUUID = translog.getTranslogUUID();
            int translogOperations = randomIntBetween(10, 100);
            for (int op = 0; op < translogOperations / 2; op++) {
                translog.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8"))));
                if (rarely()) {
                    translog.rollGeneration();
                }
            }
            translog.rollGeneration();
            comittedGeneration = randomLongBetween(2, translog.currentFileGeneration());
            for (int op = translogOperations / 2; op < translogOperations; op++) {
                translog.add(new Translog.Index("test", "" + op, op, Integer.toString(op).getBytes(Charset.forName("UTF-8"))));
                if (rarely()) {
                    translog.rollGeneration();
                }
            }
            deletionPolicy.setMinTranslogGenerationForRecovery(comittedGeneration);
            fail.failRandomly();
            try {
                translog.trimUnreferencedReaders();
            } catch (Exception e) {
                // expected...
            }
        }
        final TranslogDeletionPolicy deletionPolicy = new TranslogDeletionPolicy(-1, -1);
        deletionPolicy.setMinTranslogGenerationForRecovery(comittedGeneration);
        try (Translog translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO)) {
            // we don't know when things broke exactly
            assertThat(translog.getMinFileGeneration(), greaterThanOrEqualTo(1L));
            assertThat(translog.getMinFileGeneration(), lessThanOrEqualTo(comittedGeneration));
            assertFilePresences(translog);
            translog.trimUnreferencedReaders();
            assertThat(translog.getMinFileGeneration(), equalTo(comittedGeneration));
            assertFilePresences(translog);
        }
    }

    private Translog getFailableTranslog(FailSwitch fail, final TranslogConfig config) throws IOException {
        return getFailableTranslog(fail, config, randomBoolean(), false, null, createTranslogDeletionPolicy());
    }

    private static class FailSwitch {
        private volatile int failRate;
        private volatile boolean onceFailedFailAlways = false;

        public boolean fail() {
            boolean fail = randomIntBetween(1, 100) <= failRate;
            if (fail && onceFailedFailAlways) {
                failAlways();
            }
            return fail;
        }

        public void failNever() {
            failRate = 0;
        }

        public void failAlways() {
            failRate = 100;
        }

        public void failRandomly() {
            failRate = randomIntBetween(1, 100);
        }

        public void onceFailedFailAlways() {
            onceFailedFailAlways = true;
        }
    }


    private Translog getFailableTranslog(final FailSwitch fail, final TranslogConfig config, final boolean partialWrites,
                                         final boolean throwUnknownException, String translogUUID,
                                         final TranslogDeletionPolicy deletionPolicy) throws IOException {
        return new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO) {
            @Override
            ChannelFactory getChannelFactory() {
                final ChannelFactory factory = super.getChannelFactory();

                return (file, openOption) -> {
                    FileChannel channel = factory.open(file, openOption);
                    boolean success = false;
                    try {
                        final boolean isCkpFile = file.getFileName().toString().endsWith(".ckp"); // don't do partial writes for checkpoints we rely on the fact that the bytes are written as an atomic operation
                        ThrowingFileChannel throwingFileChannel = new ThrowingFileChannel(fail, isCkpFile ? false : partialWrites, throwUnknownException, channel);
                        success = true;
                        return throwingFileChannel;
                    } finally {
                        if (success == false) {
                            IOUtils.closeWhileHandlingException(channel);
                        }
                    }
                };
            }

            @Override
            void deleteReaderFiles(TranslogReader reader) {
                if (fail.fail()) {
                    // simulate going OOM and dieing just at the wrong moment.
                    throw new RuntimeException("simulated");
                } else {
                    super.deleteReaderFiles(reader);
                }
            }
        };
    }

    public static class ThrowingFileChannel extends FilterFileChannel {
        private final FailSwitch fail;
        private final boolean partialWrite;
        private final boolean throwUnknownException;

        public ThrowingFileChannel(FailSwitch fail, boolean partialWrite, boolean throwUnknownException, FileChannel delegate) throws MockDirectoryWrapper.FakeIOException {
            super(delegate);
            this.fail = fail;
            this.partialWrite = partialWrite;
            this.throwUnknownException = throwUnknownException;
            if (fail.fail()) {
                throw new MockDirectoryWrapper.FakeIOException();
            }
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (fail.fail()) {
                throw new MockDirectoryWrapper.FakeIOException();
            }
            return super.read(dst);
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            if (fail.fail()) {
                throw new MockDirectoryWrapper.FakeIOException();
            }
            return super.read(dsts, offset, length);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            throw new UnsupportedOperationException();
        }


        public int write(ByteBuffer src) throws IOException {
            if (fail.fail()) {
                if (partialWrite) {
                    if (src.hasRemaining()) {
                        final int pos = src.position();
                        final int limit = src.limit();
                        src.limit(randomIntBetween(pos, limit));
                        super.write(src);
                        src.limit(limit);
                        src.position(pos);
                        throw new IOException("__FAKE__ no space left on device");
                    }
                }
                if (throwUnknownException) {
                    throw new UnknownException();
                } else {
                    throw new MockDirectoryWrapper.FakeIOException();
                }
            }
            return super.write(src);
        }

        @Override
        public void force(boolean metaData) throws IOException {
            if (fail.fail()) {
                throw new MockDirectoryWrapper.FakeIOException();
            }
            super.force(metaData);
        }

        @Override
        public long position() throws IOException {
            if (fail.fail()) {
                throw new MockDirectoryWrapper.FakeIOException();
            }
            return super.position();
        }
    }

    private static final class UnknownException extends RuntimeException {

    }

    // see https://github.com/elastic/elasticsearch/issues/15754
    public void testFailWhileCreateWriteWithRecoveredTLogs() throws IOException {
        Path tempDir = createTempDir();
        TranslogConfig config = getTranslogConfig(tempDir);
        Translog translog = createTranslog(config, null);
        translog.add(new Translog.Index("test", "boom", 0, "boom".getBytes(Charset.forName("UTF-8"))));
        translog.close();
        try {
            new Translog(config, translog.getTranslogUUID(), createTranslogDeletionPolicy(), () -> SequenceNumbers.UNASSIGNED_SEQ_NO) {
                @Override
                protected TranslogWriter createWriter(long fileGeneration) throws IOException {
                    throw new MockDirectoryWrapper.FakeIOException();
                }
            };
            // if we have a LeakFS here we fail if not all resources are closed
            fail("should have been failed");
        } catch (MockDirectoryWrapper.FakeIOException ex) {
            // all is well
        }
    }

    public void testRecoverWithUnbackedNextGen() throws IOException {
        translog.add(new Translog.Index("test", "" + 0, 0, Integer.toString(0).getBytes(Charset.forName("UTF-8"))));
        translog.close();
        TranslogConfig config = translog.getConfig();

        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        Files.copy(ckp, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)));
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 1) + ".tlog"));
        try (Translog tlog = createTranslog(config, translog.getTranslogUUID());
             Translog.Snapshot snapshot = tlog.newSnapshot()) {
            assertFalse(tlog.syncNeeded());
            for (int i = 0; i < 1; i++) {
                Translog.Operation next = snapshot.next();
                assertNotNull("operation " + i + " must be non-null", next);
                assertEquals("payload missmatch", i, Integer.parseInt(next.getSource().source.utf8ToString()));
            }
            tlog.add(new Translog.Index("test", "" + 1, 1, Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
        }
        try (Translog tlog = createTranslog(config, translog.getTranslogUUID());
             Translog.Snapshot snapshot = tlog.newSnapshot()) {
            assertFalse(tlog.syncNeeded());
            for (int i = 0; i < 2; i++) {
                Translog.Operation next = snapshot.next();
                assertNotNull("operation " + i + " must be non-null", next);
                assertEquals("payload missmatch", i, Integer.parseInt(next.getSource().source.utf8ToString()));
            }
        }
    }

    public void testRecoverWithUnbackedNextGenInIllegalState() throws IOException {
        translog.add(new Translog.Index("test", "" + 0, 0, Integer.toString(0).getBytes(Charset.forName("UTF-8"))));
        translog.close();
        TranslogConfig config = translog.getConfig();
        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        // don't copy the new file
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 1) + ".tlog"));

        try {
            Translog tlog = new Translog(config, translog.getTranslogUUID(), translog.getDeletionPolicy(), () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
            fail("file already exists?");
        } catch (TranslogException ex) {
            // all is well
            assertEquals(ex.getMessage(), "failed to create new translog file");
            assertEquals(ex.getCause().getClass(), FileAlreadyExistsException.class);
        }
    }

    public void testRecoverWithUnbackedNextGenAndFutureFile() throws IOException {
        translog.add(new Translog.Index("test", "" + 0, 0, Integer.toString(0).getBytes(Charset.forName("UTF-8"))));
        translog.close();
        TranslogConfig config = translog.getConfig();
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();

        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        Files.copy(ckp, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)));
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 1) + ".tlog"));
        // we add N+1 and N+2 to ensure we only delete the N+1 file and never jump ahead and wipe without the right condition
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 2) + ".tlog"));
        try (Translog tlog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO)) {
            assertFalse(tlog.syncNeeded());
            try (Translog.Snapshot snapshot = tlog.newSnapshot()) {
                for (int i = 0; i < 1; i++) {
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null", next);
                    assertEquals("payload missmatch", i, Integer.parseInt(next.getSource().source.utf8ToString()));
                }
            }
            tlog.add(new Translog.Index("test", "" + 1, 1, Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
        }

        try {
            Translog tlog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
            fail("file already exists?");
        } catch (TranslogException ex) {
            // all is well
            assertEquals(ex.getMessage(), "failed to create new translog file");
            assertEquals(ex.getCause().getClass(), FileAlreadyExistsException.class);
        }
    }

    /**
     * This test adds operations to the translog which might randomly throw an IOException. The only thing this test verifies is
     * that we can, after we hit an exception, open and recover the translog successfully and retrieve all successfully synced operations
     * from the transaction log.
     */
    public void testWithRandomException() throws IOException {
        final int runs = randomIntBetween(5, 10);
        for (int run = 0; run < runs; run++) {
            Path tempDir = createTempDir();
            final FailSwitch fail = new FailSwitch();
            fail.failRandomly();
            TranslogConfig config = getTranslogConfig(tempDir);
            final int numOps = randomIntBetween(100, 200);
            long minGenForRecovery = 1;
            List<String> syncedDocs = new ArrayList<>();
            List<String> unsynced = new ArrayList<>();
            if (randomBoolean()) {
                fail.onceFailedFailAlways();
            }
            String generationUUID = null;
            try {
                boolean committing = false;
                final Translog failableTLog = getFailableTranslog(fail, config, randomBoolean(), false, generationUUID, createTranslogDeletionPolicy());
                try {
                    LineFileDocs lineFileDocs = new LineFileDocs(random()); //writes pretty big docs so we cross buffer boarders regularly
                    for (int opsAdded = 0; opsAdded < numOps; opsAdded++) {
                        String doc = lineFileDocs.nextDoc().toString();
                        failableTLog.add(new Translog.Index("test", "" + opsAdded, opsAdded, doc.getBytes(Charset.forName("UTF-8"))));
                        unsynced.add(doc);
                        if (randomBoolean()) {
                            failableTLog.sync();
                            syncedDocs.addAll(unsynced);
                            unsynced.clear();
                        }
                        if (randomFloat() < 0.1) {
                            failableTLog.sync(); // we have to sync here first otherwise we don't know if the sync succeeded if the commit fails
                            syncedDocs.addAll(unsynced);
                            unsynced.clear();
                            failableTLog.rollGeneration();
                            committing = true;
                            failableTLog.getDeletionPolicy().setMinTranslogGenerationForRecovery(failableTLog.currentFileGeneration());
                            failableTLog.trimUnreferencedReaders();
                            committing = false;
                            syncedDocs.clear();
                        }
                    }
                    // we survived all the randomness!!!
                    // lets close the translog and if it succeeds we are all synced again. If we don't do this we will close
                    // it in the finally block but miss to copy over unsynced docs to syncedDocs and fail the assertion down the road...
                    failableTLog.close();
                    syncedDocs.addAll(unsynced);
                    unsynced.clear();
                } catch (TranslogException | MockDirectoryWrapper.FakeIOException ex) {
                    // fair enough
                } catch (IOException ex) {
                    assertEquals(ex.getMessage(), "__FAKE__ no space left on device");
                } catch (RuntimeException ex) {
                    assertEquals(ex.getMessage(), "simulated");
                } finally {
                    Checkpoint checkpoint = Translog.readCheckpoint(config.getTranslogPath());
                    if (checkpoint.numOps == unsynced.size() + syncedDocs.size()) {
                        syncedDocs.addAll(unsynced); // failed in fsync but got fully written
                        unsynced.clear();
                    }
                    if (committing && checkpoint.minTranslogGeneration == checkpoint.generation) {
                        // we were committing and blew up in one of the syncs, but they made it through
                        syncedDocs.clear();
                        assertThat(unsynced, empty());
                    }
                    generationUUID = failableTLog.getTranslogUUID();
                    minGenForRecovery = failableTLog.getDeletionPolicy().getMinTranslogGenerationForRecovery();
                    IOUtils.closeWhileHandlingException(failableTLog);
                }
            } catch (TranslogException | MockDirectoryWrapper.FakeIOException ex) {
                // failed - that's ok, we didn't even create it
            } catch (IOException ex) {
                assertEquals(ex.getMessage(), "__FAKE__ no space left on device");
            }
            // now randomly open this failing tlog again just to make sure we can also recover from failing during recovery
            if (randomBoolean()) {
                try {
                    TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy();
                    deletionPolicy.setMinTranslogGenerationForRecovery(minGenForRecovery);
                    IOUtils.close(getFailableTranslog(fail, config, randomBoolean(), false, generationUUID, deletionPolicy));
                } catch (TranslogException | MockDirectoryWrapper.FakeIOException ex) {
                    // failed - that's ok, we didn't even create it
                } catch (IOException ex) {
                    assertEquals(ex.getMessage(), "__FAKE__ no space left on device");
                }
            }

            fail.failNever(); // we don't wanna fail here but we might since we write a new checkpoint and create a new tlog file
            TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy();
            deletionPolicy.setMinTranslogGenerationForRecovery(minGenForRecovery);
            try (Translog translog = new Translog(config, generationUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
                 Translog.Snapshot snapshot = translog.newSnapshotFromGen(minGenForRecovery)) {
                assertEquals(syncedDocs.size(), snapshot.totalOperations());
                for (int i = 0; i < syncedDocs.size(); i++) {
                    Translog.Operation next = snapshot.next();
                    assertEquals(syncedDocs.get(i), next.getSource().source.utf8ToString());
                    assertNotNull("operation " + i + " must be non-null", next);
                }
            }
        }
    }

    private Checkpoint randomCheckpoint() {
        final long a = randomNonNegativeLong();
        final long b = randomNonNegativeLong();
        final long minSeqNo;
        final long maxSeqNo;
        if (a <= b) {
            minSeqNo = a;
            maxSeqNo = b;
        } else {
            minSeqNo = b;
            maxSeqNo = a;
        }
        final long generation = randomNonNegativeLong();
        return new Checkpoint(randomLong(), randomInt(), generation, minSeqNo, maxSeqNo, randomNonNegativeLong(),
            randomLongBetween(1, generation));
    }

    public void testCheckpointOnDiskFull() throws IOException {
        final Checkpoint checkpoint = randomCheckpoint();
        Path tempDir = createTempDir();
        Checkpoint.write(FileChannel::open, tempDir.resolve("foo.cpk"), checkpoint, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        final Checkpoint checkpoint2 = randomCheckpoint();
        try {
            Checkpoint.write((p, o) -> {
                if (randomBoolean()) {
                    throw new MockDirectoryWrapper.FakeIOException();
                }
                FileChannel open = FileChannel.open(p, o);
                FailSwitch failSwitch = new FailSwitch();
                failSwitch.failNever(); // don't fail in the ctor
                ThrowingFileChannel channel = new ThrowingFileChannel(failSwitch, false, false, open);
                failSwitch.failAlways();
                return channel;

            }, tempDir.resolve("foo.cpk"), checkpoint2, StandardOpenOption.WRITE);
            fail("should have failed earlier");
        } catch (MockDirectoryWrapper.FakeIOException ex) {
            //fine
        }
        Checkpoint read = Checkpoint.read(tempDir.resolve("foo.cpk"));
        assertEquals(read, checkpoint);
    }

    /**
     * Tests that closing views after the translog is fine and we can reopen the translog
     */
    public void testPendingDelete() throws IOException {
        translog.add(new Translog.Index("test", "1", 0, new byte[]{1}));
        translog.rollGeneration();
        TranslogConfig config = translog.getConfig();
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy(config.getIndexSettings());
        translog.close();
        translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
        translog.add(new Translog.Index("test", "2", 1, new byte[]{2}));
        translog.rollGeneration();
        Closeable lock = translog.acquireRetentionLock();
        translog.add(new Translog.Index("test", "3", 2, new byte[]{3}));
        translog.close();
        IOUtils.close(lock);
        translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
    }

    public static Translog.Location randomTranslogLocation() {
        return new Translog.Location(randomLong(), randomLong(), randomInt());
    }

    public void testTranslogOpSerialization() throws Exception {
        BytesReference B_1 = new BytesArray(new byte[]{1});
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        assert Version.CURRENT.major <= 6 : "Using UNASSIGNED_SEQ_NO can be removed in 7.0, because 6.0+ nodes have actual sequence numbers";
        long randomSeqNum = randomBoolean() ? SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong();
        long primaryTerm = randomSeqNum == SequenceNumbers.UNASSIGNED_SEQ_NO ? 0 : randomIntBetween(1, 16);
        long randomPrimaryTerm = randomBoolean() ? 0 : randomNonNegativeLong();
        seqID.seqNo.setLongValue(randomSeqNum);
        seqID.seqNoDocValue.setLongValue(randomSeqNum);
        seqID.primaryTerm.setLongValue(randomPrimaryTerm);
        Field uidField = new Field("_uid", Uid.createUid("test", "1"), UidFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 1);
        Document document = new Document();
        document.add(new TextField("value", "test", Field.Store.YES));
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        ParsedDocument doc = new ParsedDocument(versionField, seqID, "1", "type", null, Arrays.asList(document), B_1, XContentType.JSON,
            null);

        Engine.Index eIndex = new Engine.Index(newUid(doc), doc, randomSeqNum, randomPrimaryTerm,
                1, VersionType.INTERNAL, Origin.PRIMARY, 0, 0, false);
        Engine.IndexResult eIndexResult = new Engine.IndexResult(1, randomSeqNum, true);
        Translog.Index index = new Translog.Index(eIndex, eIndexResult);

        BytesStreamOutput out = new BytesStreamOutput();
        Translog.Operation.writeOperation(out, index);
        StreamInput in = out.bytes().streamInput();
        Translog.Index serializedIndex = (Translog.Index) Translog.Operation.readOperation(in);
        assertEquals(index, serializedIndex);

        Engine.Delete eDelete = new Engine.Delete(doc.type(), doc.id(), newUid(doc), randomSeqNum, randomPrimaryTerm,
                2, VersionType.INTERNAL, Origin.PRIMARY, 0);
        Engine.DeleteResult eDeleteResult = new Engine.DeleteResult(2, randomSeqNum, true);
        Translog.Delete delete = new Translog.Delete(eDelete, eDeleteResult);

        out = new BytesStreamOutput();
        Translog.Operation.writeOperation(out, delete);
        in = out.bytes().streamInput();
        Translog.Delete serializedDelete = (Translog.Delete) Translog.Operation.readOperation(in);
        assertEquals(delete, serializedDelete);

        // simulate legacy delete serialization
        out = new BytesStreamOutput();
        out.writeByte(Translog.Operation.Type.DELETE.id());
        out.writeVInt(Translog.Delete.FORMAT_5_0);
        out.writeString(UidFieldMapper.NAME);
        out.writeString("my_type#my_id");
        out.writeLong(3); // version
        out.writeByte(VersionType.INTERNAL.getValue());
        out.writeLong(2); // seq no
        out.writeLong(0); // primary term
        in = out.bytes().streamInput();
        serializedDelete = (Translog.Delete) Translog.Operation.readOperation(in);
        assertEquals("my_type", serializedDelete.type());
        assertEquals("my_id", serializedDelete.id());
    }

    public void testRollGeneration() throws Exception {
        // make sure we keep some files around
        final boolean longRetention = randomBoolean();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        if (longRetention) {
            deletionPolicy.setRetentionAgeInMillis(3600 * 1000);
        } else {
            deletionPolicy.setRetentionAgeInMillis(-1);
        }
        // we control retention via time, disable size based calculations for simplicity
        deletionPolicy.setRetentionSizeInBytes(-1);
        final long generation = translog.currentFileGeneration();
        final int rolls = randomIntBetween(1, 16);
        int totalOperations = 0;
        int seqNo = 0;
        for (int i = 0; i < rolls; i++) {
            final int operations = randomIntBetween(1, 128);
            for (int j = 0; j < operations; j++) {
                translog.add(new Translog.NoOp(seqNo++, 0, "test"));
                totalOperations++;
            }
            try (ReleasableLock ignored = translog.writeLock.acquire()) {
                translog.rollGeneration();
            }
            assertThat(translog.currentFileGeneration(), equalTo(generation + i + 1));
            assertThat(translog.totalOperations(), equalTo(totalOperations));
        }
        for (int i = 0; i <= rolls; i++) {
            assertFileIsPresent(translog, generation + i);
        }
        commit(translog, generation + rolls);
        assertThat(translog.currentFileGeneration(), equalTo(generation + rolls ));
        assertThat(translog.uncommittedOperations(), equalTo(0));
        if (longRetention) {
            for (int i = 0; i <= rolls; i++) {
                assertFileIsPresent(translog, generation + i);
            }
            deletionPolicy.setRetentionAgeInMillis(randomBoolean() ? 100 : -1);
            assertBusy(() -> {
                translog.trimUnreferencedReaders();
                for (int i = 0; i < rolls; i++) {
                    assertFileDeleted(translog, generation + i);
                }
            });
        } else {
            // immediate cleanup
            for (int i = 0; i < rolls; i++) {
                assertFileDeleted(translog, generation + i);
            }
        }
        assertFileIsPresent(translog, generation + rolls);
    }

    public void testMinSeqNoBasedAPI() throws IOException {
        final int operations = randomIntBetween(1, 512);
        final List<Long> shuffledSeqNos = LongStream.range(0, operations).boxed().collect(Collectors.toList());
        Randomness.shuffle(shuffledSeqNos);
        final List<Tuple<Long, Long>> seqNos = new ArrayList<>();
        final Map<Long, Long> terms = new HashMap<>();
        for (final Long seqNo : shuffledSeqNos) {
            seqNos.add(Tuple.tuple(seqNo, terms.computeIfAbsent(seqNo, k -> 0L)));
            Long repeatingTermSeqNo = randomFrom(seqNos.stream().map(Tuple::v1).collect(Collectors.toList()));
            seqNos.add(Tuple.tuple(repeatingTermSeqNo, terms.get(repeatingTermSeqNo)));
        }

        for (final Tuple<Long, Long> tuple : seqNos) {
            translog.add(new Translog.NoOp(tuple.v1(), tuple.v2(), "test"));
            if (rarely()) {
                translog.rollGeneration();
            }
        }

        final Map<Long, Set<Tuple<Long, Long>>> seqNoPerGeneration = new HashMap<>();
        final Map<Long, Integer> opCountPerGeneration = new HashMap<>();
        // one extra roll to make sure that all ops so far are available via a reader and a translog-{gen}.ckp
        // file in a consistent way, in order to simplify checking code.
        translog.rollGeneration();
        for (long seqNo = 0; seqNo < operations; seqNo++) {
            final Set<Tuple<Long, Long>> seenSeqNos = new HashSet<>();
            final long generation = translog.getMinGenerationForSeqNo(seqNo).translogFileGeneration;
            int expectedSnapshotOps = 0;
            for (long g = generation; g < translog.currentFileGeneration(); g++) {
                if (!seqNoPerGeneration.containsKey(g)) {
                    final Set<Tuple<Long, Long>> generationSeenSeqNos = new HashSet<>();
                    int opCount = 0;
                    final Checkpoint checkpoint = Checkpoint.read(translog.location().resolve(Translog.getCommitCheckpointFileName(g)));
                    try (TranslogReader reader = translog.openReader(translog.location().resolve(Translog.getFilename(g)), checkpoint)) {
                        TranslogSnapshot snapshot = reader.newSnapshot();
                        Translog.Operation operation;
                        while ((operation = snapshot.next()) != null) {
                            generationSeenSeqNos.add(Tuple.tuple(operation.seqNo(), operation.primaryTerm()));
                            opCount++;
                        }
                        assertThat(opCount, equalTo(reader.totalOperations()));
                        assertThat(opCount, equalTo(checkpoint.numOps));
                    }
                    opCountPerGeneration.put(g, opCount);
                    seqNoPerGeneration.put(g, generationSeenSeqNos);
                }
                final Set<Tuple<Long, Long>> generationSeqNo = seqNoPerGeneration.get(g);
                if (generationSeqNo.stream().map(Tuple::v1).max(Long::compareTo).orElse(Long.MIN_VALUE) >= seqNo) {
                    expectedSnapshotOps += opCountPerGeneration.get(g);
                }
                seenSeqNos.addAll(generationSeqNo);
            }
            assertThat(translog.estimateTotalOperationsFromMinSeq(seqNo), equalTo(expectedSnapshotOps));
            int readFromSnapshot = 0;
            try (Translog.Snapshot snapshot = translog.newSnapshotFromMinSeqNo(seqNo)) {
                assertThat(snapshot.totalOperations(), equalTo(expectedSnapshotOps));
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    assertThat(Tuple.tuple(op.seqNo(), op.primaryTerm()), isIn(seenSeqNos));
                    readFromSnapshot++;
                }
            }
            assertThat(readFromSnapshot, equalTo(expectedSnapshotOps));
            final long seqNoLowerBound = seqNo;
            final Set<Tuple<Long, Long>> expected = seqNos.stream().filter(t -> t.v1() >= seqNoLowerBound).collect(Collectors.toSet());
            seenSeqNos.retainAll(expected);
            assertThat(seenSeqNos, equalTo(expected));
        }
    }

    public void testSimpleCommit() throws IOException {
        final int operations = randomIntBetween(1, 4096);
        long seqNo = 0;
        for (int i = 0; i < operations; i++) {
            translog.add(new Translog.NoOp(seqNo++, 0, "test'"));
            if (rarely()) {
                translog.rollGeneration();
            }
        }

        final long generation =
                randomIntBetween(1, Math.toIntExact(translog.currentFileGeneration()));
        commit(translog, generation);
    }

    public void testAcquiredLockIsPassedToDeletionPolicy() throws IOException {
        final int operations = randomIntBetween(1, 4096);
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        for (int i = 0; i < operations; i++) {
            translog.add(new Translog.NoOp(i, 0, "test"));
            if (rarely()) {
                translog.rollGeneration();
            }
            if (rarely()) {
                commit(translog, randomLongBetween(deletionPolicy.getMinTranslogGenerationForRecovery(), translog.currentFileGeneration()));
            }
            if (frequently()) {
                long minGen;
                try (Closeable ignored = translog.acquireRetentionLock()) {
                    minGen = translog.getMinFileGeneration();
                    assertThat(deletionPolicy.getTranslogRefCount(minGen), equalTo(1L));
                }
                assertThat(deletionPolicy.getTranslogRefCount(minGen), equalTo(0L));
            }
        }
    }
}
