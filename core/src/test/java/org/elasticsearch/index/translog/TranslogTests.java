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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.mockfile.FilterFileChannel;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

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
import java.util.Collection;
import java.util.Collections;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 *
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class TranslogTests extends ESTestCase {

    protected final ShardId shardId = new ShardId("index", "_na_", 1);

    protected Translog translog;
    protected Path translogDir;

    @Override
    protected void afterIfSuccessful() throws Exception {
        super.afterIfSuccessful();

        if (translog.isOpen()) {
            if (translog.currentFileGeneration() > 1) {
                translog.commit();
                assertFileDeleted(translog, translog.currentFileGeneration() - 1);
            }
            translog.close();
        }
        assertFileIsPresent(translog, translog.currentFileGeneration());
        IOUtils.rm(translog.location()); // delete all the locations

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
            assertEquals("there are still open views", 0, translog.getNumOpenViews());
            translog.close();
        } finally {
            super.tearDown();
        }
    }

    private Translog create(Path path) throws IOException {
        return new Translog(getTranslogConfig(path), null);
    }

    private TranslogConfig getTranslogConfig(Path path) {
        Settings build = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT)
            .build();
        ByteSizeValue bufferSize = randomBoolean() ? TranslogConfig.DEFAULT_BUFFER_SIZE : new ByteSizeValue(10 + randomInt(128 * 1024), ByteSizeUnit.BYTES);
        return new TranslogConfig(shardId, path, IndexSettingsModule.newIndexSettings(shardId.getIndex(), build), BigArrays.NON_RECYCLING_INSTANCE, bufferSize);
    }

    protected void addToTranslogAndList(Translog translog, ArrayList<Translog.Operation> list, Translog.Operation op) throws IOException {
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

    public void testRead() throws IOException {
        Location loc0 = translog.getLastWriteLocation();
        assertNotNull(loc0);

        Translog.Location loc1 = translog.add(new Translog.Index("test", "1", new byte[]{1}));
        assertThat(loc1, greaterThan(loc0));
        assertThat(translog.getLastWriteLocation(), greaterThan(loc1));
        Translog.Location loc2 = translog.add(new Translog.Index("test", "2", new byte[]{2}));
        assertThat(loc2, greaterThan(loc1));
        assertThat(translog.getLastWriteLocation(), greaterThan(loc2));
        assertThat(translog.read(loc1).getSource().source, equalTo(new BytesArray(new byte[]{1})));
        assertThat(translog.read(loc2).getSource().source, equalTo(new BytesArray(new byte[]{2})));

        Translog.Location lastLocBeforeSync = translog.getLastWriteLocation();
        translog.sync();
        assertEquals(lastLocBeforeSync, translog.getLastWriteLocation());
        assertThat(translog.read(loc1).getSource().source, equalTo(new BytesArray(new byte[]{1})));
        assertThat(translog.read(loc2).getSource().source, equalTo(new BytesArray(new byte[]{2})));

        Translog.Location loc3 = translog.add(new Translog.Index("test", "2", new byte[]{3}));
        assertThat(loc3, greaterThan(loc2));
        assertThat(translog.getLastWriteLocation(), greaterThan(loc3));
        assertThat(translog.read(loc3).getSource().source, equalTo(new BytesArray(new byte[]{3})));

        lastLocBeforeSync = translog.getLastWriteLocation();
        translog.sync();
        assertEquals(lastLocBeforeSync, translog.getLastWriteLocation());
        assertThat(translog.read(loc3).getSource().source, equalTo(new BytesArray(new byte[]{3})));
        translog.prepareCommit();
        /*
         * The commit adds to the lastWriteLocation even though is isn't really a write. This is just an implementation artifact but it can
         * safely be ignored because the lastWriteLocation continues to be greater than the Location returned from the last write operation
         * and less than the location of the next write operation.
         */
        assertThat(translog.getLastWriteLocation(), greaterThan(lastLocBeforeSync));
        assertThat(translog.read(loc3).getSource().source, equalTo(new BytesArray(new byte[]{3})));
        translog.commit();
        assertNull(translog.read(loc1));
        assertNull(translog.read(loc2));
        assertNull(translog.read(loc3));
        try {
            translog.read(new Translog.Location(translog.currentFileGeneration() + 1, 17, 35));
            fail("generation is greater than the current");
        } catch (IllegalStateException ex) {
            // expected
        }
    }

    public void testSimpleOperations() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        Translog.Snapshot snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.size(0));

        addToTranslogAndList(translog, ops, new Translog.Index("test", "1", new byte[]{1}));
        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.totalOperations(), equalTo(ops.size()));

        addToTranslogAndList(translog, ops, new Translog.Delete(newUid("2")));
        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.totalOperations(), equalTo(ops.size()));

        snapshot = translog.newSnapshot();

        Translog.Index index = (Translog.Index) snapshot.next();
        assertThat(index != null, equalTo(true));
        assertThat(BytesReference.toBytes(index.source()), equalTo(new byte[]{1}));

        Translog.Delete delete = (Translog.Delete) snapshot.next();
        assertThat(delete != null, equalTo(true));
        assertThat(delete.uid(), equalTo(newUid("2")));

        assertThat(snapshot.next(), equalTo(null));

        long firstId = translog.currentFileGeneration();
        translog.prepareCommit();
        assertThat(translog.currentFileGeneration(), Matchers.not(equalTo(firstId)));

        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.totalOperations(), equalTo(ops.size()));

        translog.commit();
        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.size(0));
        assertThat(snapshot.totalOperations(), equalTo(0));
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
        final long firstOperationPosition = translog.getFirstOperationPosition();
        TranslogStats stats = stats();
        assertThat(stats.estimatedNumberOfOperations(), equalTo(0L));
        long lastSize = stats.getTranslogSizeInBytes();
        assertThat((int) firstOperationPosition, greaterThan(CodecUtil.headerLength(TranslogWriter.TRANSLOG_CODEC)));
        assertThat(lastSize, equalTo(firstOperationPosition));
        TranslogStats total = new TranslogStats();
        translog.add(new Translog.Index("test", "1", new byte[]{1}));
        stats = stats();
        total.add(stats);
        assertThat(stats.estimatedNumberOfOperations(), equalTo(1L));
        assertThat(stats.getTranslogSizeInBytes(), greaterThan(lastSize));
        lastSize = stats.getTranslogSizeInBytes();

        translog.add(new Translog.Delete(newUid("2")));
        stats = stats();
        total.add(stats);
        assertThat(stats.estimatedNumberOfOperations(), equalTo(2L));
        assertThat(stats.getTranslogSizeInBytes(), greaterThan(lastSize));
        lastSize = stats.getTranslogSizeInBytes();

        translog.add(new Translog.Delete(newUid("3")));
        translog.prepareCommit();
        stats = stats();
        total.add(stats);
        assertThat(stats.estimatedNumberOfOperations(), equalTo(3L));
        assertThat(stats.getTranslogSizeInBytes(), greaterThan(lastSize));

        translog.commit();
        stats = stats();
        total.add(stats);
        assertThat(stats.estimatedNumberOfOperations(), equalTo(0L));
        assertThat(stats.getTranslogSizeInBytes(), equalTo(firstOperationPosition));
        assertEquals(6, total.estimatedNumberOfOperations());
        assertEquals(431, total.getTranslogSizeInBytes());

        BytesStreamOutput out = new BytesStreamOutput();
        total.writeTo(out);
        TranslogStats copy = new TranslogStats();
        copy.readFrom(out.bytes().streamInput());

        assertEquals(6, copy.estimatedNumberOfOperations());
        assertEquals(431, copy.getTranslogSizeInBytes());

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            copy.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            assertEquals("{\"translog\":{\"operations\":6,\"size_in_bytes\":431}}", builder.string());
        }

        try {
            new TranslogStats(1, -1);
            fail("must be positive");
        } catch (IllegalArgumentException ex) {
            //all well
        }
        try {
            new TranslogStats(-1, 1);
            fail("must be positive");
        } catch (IllegalArgumentException ex) {
            //all well
        }
    }

    public void testSnapshot() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        Translog.Snapshot snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.size(0));

        addToTranslogAndList(translog, ops, new Translog.Index("test", "1", new byte[]{1}));

        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.totalOperations(), equalTo(1));

        snapshot = translog.newSnapshot();
        Translog.Snapshot snapshot1 = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.totalOperations(), equalTo(1));

        assertThat(snapshot1, SnapshotMatchers.size(1));
        assertThat(snapshot1.totalOperations(), equalTo(1));
    }

    public void testSnapshotWithNewTranslog() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        Translog.Snapshot snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.size(0));

        addToTranslogAndList(translog, ops, new Translog.Index("test", "1", new byte[]{1}));
        Translog.Snapshot snapshot1 = translog.newSnapshot();

        addToTranslogAndList(translog, ops, new Translog.Index("test", "2", new byte[]{2}));

        assertThat(snapshot1, SnapshotMatchers.equalsTo(ops.get(0)));

        translog.prepareCommit();
        addToTranslogAndList(translog, ops, new Translog.Index("test", "3", new byte[]{3}));

        try (Translog.View view = translog.newView()) {
            Translog.Snapshot snapshot2 = translog.newSnapshot();
            translog.commit();
            assertThat(snapshot2, SnapshotMatchers.equalsTo(ops));
            assertThat(snapshot2.totalOperations(), equalTo(ops.size()));
        }
    }

    public void testSnapshotOnClosedTranslog() throws IOException {
        assertTrue(Files.exists(translogDir.resolve(Translog.getFilename(1))));
        translog.add(new Translog.Index("test", "1", new byte[]{1}));
        translog.close();
        try {
            Translog.Snapshot snapshot = translog.newSnapshot();
            fail("translog is closed");
        } catch (AlreadyClosedException ex) {
            assertEquals(ex.getMessage(), "translog is already closed");
        }
    }

    public void assertFileIsPresent(Translog translog, long id) {
        if (Files.exists(translogDir.resolve(Translog.getFilename(id)))) {
            return;
        }
        fail(Translog.getFilename(id) + " is not present in any location: " + translog.location());
    }

    public void assertFileDeleted(Translog translog, long id) {
        assertFalse("translog [" + id + "] still exists", Files.exists(translog.location().resolve(Translog.getFilename(id))));
    }

    static class LocationOperation {
        final Translog.Operation operation;
        final Translog.Location location;

        public LocationOperation(Translog.Operation operation, Translog.Location location) {
            this.operation = operation;
            this.location = location;
        }

    }

    public void testConcurrentWritesWithVaryingSize() throws Throwable {
        final int opsPerThread = randomIntBetween(10, 200);
        int threadCount = 2 + randomInt(5);

        logger.info("testing with [{}] threads, each doing [{}] ops", threadCount, opsPerThread);
        final BlockingQueue<LocationOperation> writtenOperations = new ArrayBlockingQueue<>(threadCount * opsPerThread);

        Thread[] threads = new Thread[threadCount];
        final Exception[] threadExceptions = new Exception[threadCount];
        final CountDownLatch downLatch = new CountDownLatch(1);
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new TranslogThread(translog, downLatch, opsPerThread, threadId, writtenOperations, threadExceptions);
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

        for (LocationOperation locationOperation : writtenOperations) {
            Translog.Operation op = translog.read(locationOperation.location);
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
                default:
                    throw new ElasticsearchException("unsupported opType");
            }

        }

    }

    public void testTranslogChecksums() throws Exception {
        List<Translog.Location> locations = new ArrayList<>();

        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations; op++) {
            String ascii = randomAsciiOfLengthBetween(1, 50);
            locations.add(translog.add(new Translog.Index("test", "" + op, ascii.getBytes("UTF-8"))));
        }
        translog.sync();

        corruptTranslogs(translogDir);

        AtomicInteger corruptionsCaught = new AtomicInteger(0);
        for (Translog.Location location : locations) {
            try {
                translog.read(location);
            } catch (TranslogCorruptedException e) {
                corruptionsCaught.incrementAndGet();
            }
        }
        assertThat("at least one corruption was caused and caught", corruptionsCaught.get(), greaterThanOrEqualTo(1));
    }

    public void testTruncatedTranslogs() throws Exception {
        List<Translog.Location> locations = new ArrayList<>();

        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations; op++) {
            String ascii = randomAsciiOfLengthBetween(1, 50);
            locations.add(translog.add(new Translog.Index("test", "" + op, ascii.getBytes("UTF-8"))));
        }
        translog.sync();

        truncateTranslogs(translogDir);

        AtomicInteger truncations = new AtomicInteger(0);
        for (Translog.Location location : locations) {
            try {
                translog.read(location);
            } catch (ElasticsearchException e) {
                if (e.getCause() instanceof EOFException) {
                    truncations.incrementAndGet();
                } else {
                    throw e;
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

    private Term newUid(String id) {
        return new Term("_uid", id);
    }

    public void testVerifyTranslogIsNotDeleted() throws IOException {
        assertFileIsPresent(translog, 1);
        translog.add(new Translog.Index("test", "1", new byte[]{1}));
        Translog.Snapshot snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.size(1));
        assertFileIsPresent(translog, 1);
        assertThat(snapshot.totalOperations(), equalTo(1));
        translog.close();

        assertFileIsPresent(translog, 1);
    }

    /**
     * Tests that concurrent readers and writes maintain view and snapshot semantics
     */
    public void testConcurrentWriteViewsAndSnapshot() throws Throwable {
        final Thread[] writers = new Thread[randomIntBetween(1, 10)];
        final Thread[] readers = new Thread[randomIntBetween(1, 10)];
        final int flushEveryOps = randomIntBetween(5, 100);
        // used to notify main thread that so many operations have been written so it can simulate a flush
        final AtomicReference<CountDownLatch> writtenOpsLatch = new AtomicReference<>(new CountDownLatch(0));
        final AtomicLong idGenerator = new AtomicLong();
        final CyclicBarrier barrier = new CyclicBarrier(writers.length + readers.length + 1);

        // a map of all written ops and their returned location.
        final Map<Translog.Operation, Translog.Location> writtenOps = ConcurrentCollections.newConcurrentMap();

        // a signal for all threads to stop
        final AtomicBoolean run = new AtomicBoolean(true);

        // any errors on threads
        final List<Exception> errors = new CopyOnWriteArrayList<>();
        logger.debug("using [{}] readers. [{}] writers. flushing every ~[{}] ops.", readers.length, writers.length, flushEveryOps);
        for (int i = 0; i < writers.length; i++) {
            final String threadName = "writer_" + i;
            final int threadId = i;
            writers[i] = new Thread(new AbstractRunnable() {
                @Override
                public void doRun() throws BrokenBarrierException, InterruptedException, IOException {
                    barrier.await();
                    int counter = 0;
                    while (run.get()) {
                        long id = idGenerator.incrementAndGet();
                        final Translog.Operation op;
                        switch (Translog.Operation.Type.values()[((int) (id % Translog.Operation.Type.values().length))]) {
                            case CREATE:
                            case INDEX:
                                op = new Translog.Index("type", "" + id, new byte[]{(byte) id});
                                break;
                            case DELETE:
                                op = new Translog.Delete(newUid("" + id));
                                break;
                            default:
                                throw new ElasticsearchException("unknown type");
                        }
                        Translog.Location location = translog.add(op);
                        Translog.Location existing = writtenOps.put(op, location);
                        if (existing != null) {
                            fail("duplicate op [" + op + "], old entry at " + location);
                        }
                        if (id % writers.length == threadId) {
                            translog.ensureSynced(location);
                        }
                        writtenOpsLatch.get().countDown();
                        counter++;
                    }
                    logger.debug("--> [{}] done. wrote [{}] ops.", threadName, counter);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("--> writer [{}] had an error", e, threadName);
                    errors.add(e);
                }
            }, threadName);
            writers[i].start();
        }

        for (int i = 0; i < readers.length; i++) {
            final String threadId = "reader_" + i;
            readers[i] = new Thread(new AbstractRunnable() {
                Translog.View view = null;
                Set<Translog.Operation> writtenOpsAtView;

                @Override
                public void onFailure(Exception e) {
                    logger.error("--> reader [{}] had an error", e, threadId);
                    errors.add(e);
                    try {
                        closeView();
                    } catch (IOException inner) {
                        inner.addSuppressed(e);
                        logger.error("unexpected error while closing view, after failure", inner);
                    }
                }

                void closeView() throws IOException {
                    if (view != null) {
                        view.close();
                    }
                }

                void newView() throws IOException {
                    closeView();
                    view = translog.newView();
                    // captures the currently written ops so we know what to expect from the view
                    writtenOpsAtView = new HashSet<>(writtenOps.keySet());
                    logger.debug("--> [{}] opened view from [{}]", threadId, view.minTranslogGeneration());
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    int iter = 0;
                    while (run.get()) {
                        if (iter++ % 10 == 0) {
                            newView();
                        }

                        // captures al views that are written since the view was created (with a small caveat see bellow)
                        // these are what we expect the snapshot to return (and potentially some more).
                        Set<Translog.Operation> expectedOps = new HashSet<>(writtenOps.keySet());
                        expectedOps.removeAll(writtenOpsAtView);
                        Translog.Snapshot snapshot = view.snapshot();
                        Translog.Operation op;
                        while ((op = snapshot.next()) != null) {
                            expectedOps.remove(op);
                        }
                        if (expectedOps.isEmpty() == false) {
                            StringBuilder missed = new StringBuilder("missed ").append(expectedOps.size()).append(" operations");
                            boolean failed = false;
                            for (Translog.Operation expectedOp : expectedOps) {
                                final Translog.Location loc = writtenOps.get(expectedOp);
                                if (loc.generation < view.minTranslogGeneration()) {
                                    // writtenOps is only updated after the op was written to the translog. This mean
                                    // that ops written to the translog before the view was taken (and will be missing from the view)
                                    // may yet be available in writtenOpsAtView, meaning we will erroneously expect them
                                    continue;
                                }
                                failed = true;
                                missed.append("\n --> [").append(expectedOp).append("] written at ").append(loc);
                            }
                            if (failed) {
                                fail(missed.toString());
                            }
                        }
                        // slow down things a bit and spread out testing..
                        writtenOpsLatch.get().await(200, TimeUnit.MILLISECONDS);
                    }
                    closeView();
                    logger.debug("--> [{}] done. tested [{}] snapshots", threadId, iter);
                }
            }, threadId);
            readers[i].start();
        }

        barrier.await();
        try {
            for (int iterations = scaledRandomIntBetween(10, 200); iterations > 0 && errors.isEmpty(); iterations--) {
                writtenOpsLatch.set(new CountDownLatch(flushEveryOps));
                while (writtenOpsLatch.get().await(200, TimeUnit.MILLISECONDS) == false) {
                    if (errors.size() > 0) {
                        break;
                    }
                }
                translog.commit();
            }
        } finally {
            run.set(false);
            logger.debug("--> waiting for threads to stop");
            for (Thread thread : writers) {
                thread.join();
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
    }

    public void testSyncUpTo() throws IOException {
        int translogOperations = randomIntBetween(10, 100);
        int count = 0;
        for (int op = 0; op < translogOperations; op++) {
            final Translog.Location location = translog.add(new Translog.Index("test", "" + op, Integer.toString(++count).getBytes(Charset.forName("UTF-8"))));
            if (randomBoolean()) {
                assertTrue("at least one operation pending", translog.syncNeeded());
                assertTrue("this operation has not been synced", translog.ensureSynced(location));
                assertFalse("the last call to ensureSycned synced all previous ops", translog.syncNeeded()); // we are the last location so everything should be synced
                translog.add(new Translog.Index("test", "" + op, Integer.toString(++count).getBytes(Charset.forName("UTF-8"))));
                assertTrue("one pending operation", translog.syncNeeded());
                assertFalse("this op has been synced before", translog.ensureSynced(location)); // not syncing now
                assertTrue("we only synced a previous operation yet", translog.syncNeeded());
            }
            if (rarely()) {
                translog.commit();
                assertFalse("location is from a previous translog - already synced", translog.ensureSynced(location)); // not syncing now
                assertFalse("no sync needed since no operations in current translog", translog.syncNeeded());
            }

            if (randomBoolean()) {
                translog.sync();
                assertFalse("translog has been synced already", translog.ensureSynced(location));
            }
        }
    }

    public void testLocationComparison() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        int count = 0;
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("test", "" + op, Integer.toString(++count).getBytes(Charset.forName("UTF-8")))));
            if (rarely() && translogOperations > op + 1) {
                translog.commit();
            }
        }
        Collections.shuffle(locations, random());
        Translog.Location max = locations.get(0);
        for (Translog.Location location : locations) {
            max = max(max, location);
        }

        assertEquals(max.generation, translog.currentFileGeneration());
        final Translog.Operation read = translog.read(max);
        assertEquals(read.getSource().source.utf8ToString(), Integer.toString(count));
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
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (frequently()) {
                translog.sync();
                lastSynced = op;
            }
        }
        assertEquals(translogOperations, translog.totalOperations());
        final Translog.Location lastLocation = translog.add(new Translog.Index("test", "" + translogOperations, Integer.toString(translogOperations).getBytes(Charset.forName("UTF-8"))));

        final Checkpoint checkpoint = Checkpoint.read(translog.location().resolve(Translog.CHECKPOINT_FILE_NAME));
        try (final TranslogReader reader = translog.openReader(translog.location().resolve(Translog.getFilename(translog.currentFileGeneration())), checkpoint)) {
            assertEquals(lastSynced + 1, reader.totalOperations());
            for (int op = 0; op < translogOperations; op++) {
                Translog.Location location = locations.get(op);
                if (op <= lastSynced) {
                    final Translog.Operation read = reader.read(location);
                    assertEquals(Integer.toString(op), read.getSource().source.utf8ToString());
                } else {
                    try {
                        reader.read(location);
                        fail("read past checkpoint");
                    } catch (EOFException ex) {

                    }
                }
            }
            try {
                reader.read(lastLocation);
                fail("read past checkpoint");
            } catch (EOFException ex) {
            }
        }
        assertEquals(translogOperations + 1, translog.totalOperations());
        translog.close();
    }

    public void testTranslogWriter() throws IOException {
        final TranslogWriter writer = translog.createWriter(0);
        final int numOps = randomIntBetween(10, 100);
        byte[] bytes = new byte[4];
        ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
        for (int i = 0; i < numOps; i++) {
            out.reset(bytes);
            out.writeInt(i);
            writer.add(new BytesArray(bytes));
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

        out.reset(bytes);
        out.writeInt(2048);
        writer.add(new BytesArray(bytes));

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

    public void testFailWriterWhileClosing() throws IOException {
        Path tempDir = createTempDir();
        final FailSwitch fail = new FailSwitch();
        fail.failNever();
        TranslogConfig config = getTranslogConfig(tempDir);
        try (Translog translog = getFailableTranslog(fail, config)) {
            final TranslogWriter writer = translog.createWriter(0);
            final int numOps = randomIntBetween(10, 100);
            byte[] bytes = new byte[4];
            ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
            for (int i = 0; i < numOps; i++) {
                out.reset(bytes);
                out.writeInt(i);
                writer.add(new BytesArray(bytes));
            }
            writer.sync();
            try {
                fail.failAlways();
                writer.closeIntoReader();
                fail();
            } catch (MockDirectoryWrapper.FakeIOException ex) {
            }
            try (TranslogReader reader = translog.openReader(writer.path(), Checkpoint.read(translog.location().resolve(Translog.CHECKPOINT_FILE_NAME)))) {
                for (int i = 0; i < numOps; i++) {
                    ByteBuffer buffer = ByteBuffer.allocate(4);
                    reader.readBytes(buffer, reader.getFirstOperationOffset() + 4 * i);
                    buffer.flip();
                    final int value = buffer.getInt();
                    assertEquals(i, value);
                }
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
            locations.add(translog.add(new Translog.Index("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            final boolean commit = commitOften ? frequently() : rarely();
            if (commit && op < translogOperations - 1) {
                translog.commit();
                minUncommittedOp = op + 1;
                translogGeneration = translog.getGeneration();
            }
        }
        translog.sync();
        TranslogConfig config = translog.getConfig();

        translog.close();
        translog = new Translog(config, translogGeneration);
        if (translogGeneration == null) {
            assertEquals(0, translog.stats().estimatedNumberOfOperations());
            assertEquals(1, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            Translog.Snapshot snapshot = translog.newSnapshot();
            assertNull(snapshot.next());
        } else {
            assertEquals("lastCommitted must be 1 less than current", translogGeneration.translogFileGeneration + 1, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            Translog.Snapshot snapshot = translog.newSnapshot();
            for (int i = minUncommittedOp; i < translogOperations; i++) {
                assertEquals("expected operation" + i + " to be in the previous translog but wasn't", translog.currentFileGeneration() - 1, locations.get(i).generation);
                Translog.Operation next = snapshot.next();
                assertNotNull("operation " + i + " must be non-null", next);
                assertEquals(i, Integer.parseInt(next.getSource().source.utf8ToString()));
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
            locations.add(translog.add(new Translog.Index("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (op == prepareOp) {
                translogGeneration = translog.getGeneration();
                translog.prepareCommit();
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
        try (Translog translog = new Translog(config, translogGeneration)) {
            assertNotNull(translogGeneration);
            assertEquals("lastCommitted must be 2 less than current - we never finished the commit", translogGeneration.translogFileGeneration + 2, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            Translog.Snapshot snapshot = translog.newSnapshot();
            int upTo = sync ? translogOperations : prepareOp;
            for (int i = 0; i < upTo; i++) {
                Translog.Operation next = snapshot.next();
                assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                assertEquals("payload missmatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.utf8ToString()));
            }
        }
        if (randomBoolean()) { // recover twice
            try (Translog translog = new Translog(config, translogGeneration)) {
                assertNotNull(translogGeneration);
                assertEquals("lastCommitted must be 3 less than current - we never finished the commit and run recovery twice", translogGeneration.translogFileGeneration + 3, translog.currentFileGeneration());
                assertFalse(translog.syncNeeded());
                Translog.Snapshot snapshot = translog.newSnapshot();
                int upTo = sync ? translogOperations : prepareOp;
                for (int i = 0; i < upTo; i++) {
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                    assertEquals("payload missmatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.utf8ToString()));
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
            locations.add(translog.add(new Translog.Index("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (op == prepareOp) {
                translogGeneration = translog.getGeneration();
                translog.prepareCommit();
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

        try (Translog translog = new Translog(config, translogGeneration)) {
            assertNotNull(translogGeneration);
            assertEquals("lastCommitted must be 2 less than current - we never finished the commit", translogGeneration.translogFileGeneration + 2, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            Translog.Snapshot snapshot = translog.newSnapshot();
            int upTo = sync ? translogOperations : prepareOp;
            for (int i = 0; i < upTo; i++) {
                Translog.Operation next = snapshot.next();
                assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                assertEquals("payload missmatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.utf8ToString()));
            }

        }

        if (randomBoolean()) { // recover twice
            try (Translog translog = new Translog(config, translogGeneration)) {
                assertNotNull(translogGeneration);
                assertEquals("lastCommitted must be 3 less than current - we never finished the commit and run recovery twice", translogGeneration.translogFileGeneration + 3, translog.currentFileGeneration());
                assertFalse(translog.syncNeeded());
                Translog.Snapshot snapshot = translog.newSnapshot();
                int upTo = sync ? translogOperations : prepareOp;
                for (int i = 0; i < upTo; i++) {
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                    assertEquals("payload missmatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.utf8ToString()));
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
            locations.add(translog.add(new Translog.Index("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (op == prepareOp) {
                translogGeneration = translog.getGeneration();
                translog.prepareCommit();
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
        Checkpoint corrupted = new Checkpoint(0, 0, 0);
        Checkpoint.write(FileChannel::open, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)), corrupted, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        try (Translog translog = new Translog(config, translogGeneration)) {
            fail("corrupted");
        } catch (IllegalStateException ex) {
            assertEquals(ex.getMessage(), "Checkpoint file translog-2.ckp already exists but has corrupted content expected: Checkpoint{offset=2683, numOps=55, translogFileGeneration= 2} but got: Checkpoint{offset=0, numOps=0, translogFileGeneration= 0}");
        }
        Checkpoint.write(FileChannel::open, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)), read, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        try (Translog translog = new Translog(config, translogGeneration)) {
            assertNotNull(translogGeneration);
            assertEquals("lastCommitted must be 2 less than current - we never finished the commit", translogGeneration.translogFileGeneration + 2, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            Translog.Snapshot snapshot = translog.newSnapshot();
            int upTo = sync ? translogOperations : prepareOp;
            for (int i = 0; i < upTo; i++) {
                Translog.Operation next = snapshot.next();
                assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                assertEquals("payload missmatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.utf8ToString()));
            }
        }
    }

    public void testSnapshotFromStreamInput() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        List<Translog.Operation> ops = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations; op++) {
            Translog.Index test = new Translog.Index("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")));
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
                locations.add(translog.add(new Translog.Index("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
                locations2.add(translog2.add(new Translog.Index("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
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
            locations.add(translog.add(new Translog.Index("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (randomBoolean()) {
                translog.commit();
                firstUncommitted = op + 1;
            }
        }
        TranslogConfig config = translog.getConfig();
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        translog.close();

        Translog.TranslogGeneration generation = new Translog.TranslogGeneration(randomRealisticUnicodeOfCodepointLengthBetween(1,
            translogGeneration.translogUUID.length()), translogGeneration.translogFileGeneration);
        try {
            new Translog(config, generation);
            fail("translog doesn't belong to this UUID");
        } catch (TranslogCorruptedException ex) {

        }
        this.translog = new Translog(config, translogGeneration);
        Translog.Snapshot snapshot = this.translog.newSnapshot();
        for (int i = firstUncommitted; i < translogOperations; i++) {
            Translog.Operation next = snapshot.next();
            assertNotNull("" + i, next);
            assertEquals(Integer.parseInt(next.getSource().source.utf8ToString()), i);
        }
        assertNull(snapshot.next());
    }

    public void testFailOnClosedWrite() throws IOException {
        translog.add(new Translog.Index("test", "1", Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
        translog.close();
        try {
            translog.add(new Translog.Index("test", "1", Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
            fail("closed");
        } catch (AlreadyClosedException ex) {
            // all is welll
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
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new TranslogThread(translog, downLatch, opsPerThread, threadId, writtenOperations, threadExceptions);
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

        public TranslogThread(Translog translog, CountDownLatch downLatch, int opsPerThread, int threadId, Collection<LocationOperation> writtenOperations, Exception[] threadExceptions) {
            this.translog = translog;
            this.downLatch = downLatch;
            this.opsPerThread = opsPerThread;
            this.threadId = threadId;
            this.writtenOperations = writtenOperations;
            this.threadExceptions = threadExceptions;
        }

        @Override
        public void run() {
            try {
                downLatch.await();
                for (int opCount = 0; opCount < opsPerThread; opCount++) {
                    Translog.Operation op;
                    switch (randomFrom(Translog.Operation.Type.values())) {
                        case CREATE:
                        case INDEX:
                            op = new Translog.Index("test", threadId + "_" + opCount,
                                randomUnicodeOfLengthBetween(1, 20 * 1024).getBytes("UTF-8"));
                            break;
                        case DELETE:
                            op = new Translog.Delete(new Term("_uid", threadId + "_" + opCount),
                                1 + randomInt(100000),
                                randomFrom(VersionType.values()));
                            break;
                        default:
                            throw new ElasticsearchException("not supported op type");
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
                locations.add(translog.add(new Translog.Index("test", "" + opsSynced, Integer.toString(opsSynced).getBytes(Charset.forName("UTF-8")))));
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
                locations.add(translog.add(new Translog.Index("test", "" + opsSynced, Integer.toString(opsSynced).getBytes(Charset.forName("UTF-8")))));
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
            translog.commit();
            fail("already closed");
        } catch (AlreadyClosedException ex) {
            assertNotNull(ex.getCause());
            assertSame(translog.getTragicException(), ex.getCause());
        }

        assertFalse(translog.isOpen());
        translog.close(); // we are closed
        try (Translog tlog = new Translog(config, translogGeneration)) {
            assertEquals("lastCommitted must be 1 less than current", translogGeneration.translogFileGeneration + 1, tlog.currentFileGeneration());
            assertFalse(tlog.syncNeeded());

            Translog.Snapshot snapshot = tlog.newSnapshot();
            assertEquals(opsSynced, snapshot.totalOperations());
            for (int i = 0; i < opsSynced; i++) {
                assertEquals("expected operation" + i + " to be in the previous translog but wasn't", tlog.currentFileGeneration() - 1, locations.get(i).generation);
                Translog.Operation next = snapshot.next();
                assertNotNull("operation " + i + " must be non-null", next);
                assertEquals(i, Integer.parseInt(next.getSource().source.utf8ToString()));
            }
        }
    }

    public void testTranslogOpsCountIsCorrect() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int numOps = randomIntBetween(100, 200);
        LineFileDocs lineFileDocs = new LineFileDocs(random()); // writes pretty big docs so we cross buffer boarders regularly
        for (int opsAdded = 0; opsAdded < numOps; opsAdded++) {
            locations.add(translog.add(new Translog.Index("test", "" + opsAdded, lineFileDocs.nextDoc().toString().getBytes(Charset.forName("UTF-8")))));
            Translog.Snapshot snapshot = this.translog.newSnapshot();
            assertEquals(opsAdded + 1, snapshot.totalOperations());
            for (int i = 0; i < opsAdded; i++) {
                assertEquals("expected operation" + i + " to be in the current translog but wasn't", translog.currentFileGeneration(), locations.get(i).generation);
                Translog.Operation next = snapshot.next();
                assertNotNull("operation " + i + " must be non-null", next);
            }
        }
    }

    public void testTragicEventCanBeAnyException() throws IOException {
        Path tempDir = createTempDir();
        final FailSwitch fail = new FailSwitch();
        TranslogConfig config = getTranslogConfig(tempDir);
        Translog translog = getFailableTranslog(fail, config, false, true, null);
        LineFileDocs lineFileDocs = new LineFileDocs(random()); // writes pretty big docs so we cross buffer boarders regularly
        translog.add(new Translog.Index("test", "1", lineFileDocs.nextDoc().toString().getBytes(Charset.forName("UTF-8"))));
        fail.failAlways();
        try {
            Translog.Location location = translog.add(new Translog.Index("test", "2", lineFileDocs.nextDoc().toString().getBytes(Charset.forName("UTF-8"))));
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

        final int threadCount = randomIntBetween(1, 5);
        Thread[] threads = new Thread[threadCount];
        final Exception[] threadExceptions = new Exception[threadCount];
        final CountDownLatch downLatch = new CountDownLatch(1);
        final CountDownLatch added = new CountDownLatch(randomIntBetween(10, 100));
        List<LocationOperation> writtenOperations = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new TranslogThread(translog, downLatch, 200, threadId, writtenOperations, threadExceptions) {
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
        try (Translog.View view = translog.newView()) {
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
            try (Translog tlog = new Translog(config, translog.getGeneration())) {
                Translog.Snapshot snapshot = tlog.newSnapshot();
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

    private Translog getFailableTranslog(FailSwitch fail, final TranslogConfig config) throws IOException {
        return getFailableTranslog(fail, config, randomBoolean(), false, null);
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


    private Translog getFailableTranslog(final FailSwitch fail, final TranslogConfig config, final boolean paritalWrites, final boolean throwUnknownException, Translog.TranslogGeneration generation) throws IOException {
        return new Translog(config, generation) {
            @Override
            ChannelFactory getChannelFactory() {
                final ChannelFactory factory = super.getChannelFactory();

                return (file, openOption) -> {
                    FileChannel channel = factory.open(file, openOption);
                    boolean success = false;
                    try {
                        final boolean isCkpFile = file.getFileName().toString().endsWith(".ckp"); // don't do partial writes for checkpoints we rely on the fact that the 20bytes are written as an atomic operation
                        ThrowingFileChannel throwingFileChannel = new ThrowingFileChannel(fail, isCkpFile ? false : paritalWrites, throwUnknownException, channel);
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
            protected boolean assertBytesAtLocation(Location location, BytesReference expectedBytes) throws IOException {
                return true; // we don't wanna fail in the assert
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
        Translog translog = new Translog(config, null);
        translog.add(new Translog.Index("test", "boom", "boom".getBytes(Charset.forName("UTF-8"))));
        Translog.TranslogGeneration generation = translog.getGeneration();
        translog.close();
        try {
            new Translog(config, generation) {
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
        translog.add(new Translog.Index("test", "" + 0, Integer.toString(0).getBytes(Charset.forName("UTF-8"))));
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        translog.close();
        TranslogConfig config = translog.getConfig();

        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        Files.copy(ckp, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)));
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 1) + ".tlog"));
        try (Translog tlog = new Translog(config, translogGeneration)) {
            assertNotNull(translogGeneration);
            assertFalse(tlog.syncNeeded());
            Translog.Snapshot snapshot = tlog.newSnapshot();
            for (int i = 0; i < 1; i++) {
                Translog.Operation next = snapshot.next();
                assertNotNull("operation " + i + " must be non-null", next);
                assertEquals("payload missmatch", i, Integer.parseInt(next.getSource().source.utf8ToString()));
            }
            tlog.add(new Translog.Index("test", "" + 1, Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
        }
        try (Translog tlog = new Translog(config, translogGeneration)) {
            assertNotNull(translogGeneration);
            assertFalse(tlog.syncNeeded());
            Translog.Snapshot snapshot = tlog.newSnapshot();
            for (int i = 0; i < 2; i++) {
                Translog.Operation next = snapshot.next();
                assertNotNull("operation " + i + " must be non-null", next);
                assertEquals("payload missmatch", i, Integer.parseInt(next.getSource().source.utf8ToString()));
            }
        }
    }

    public void testRecoverWithUnbackedNextGenInIllegalState() throws IOException {
        translog.add(new Translog.Index("test", "" + 0, Integer.toString(0).getBytes(Charset.forName("UTF-8"))));
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        translog.close();
        TranslogConfig config = translog.getConfig();
        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        // don't copy the new file
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 1) + ".tlog"));

        try {
            Translog tlog = new Translog(config, translogGeneration);
            fail("file already exists?");
        } catch (TranslogException ex) {
            // all is well
            assertEquals(ex.getMessage(), "failed to create new translog file");
            assertEquals(ex.getCause().getClass(), FileAlreadyExistsException.class);
        }
    }

    public void testRecoverWithUnbackedNextGenAndFutureFile() throws IOException {
        translog.add(new Translog.Index("test", "" + 0, Integer.toString(0).getBytes(Charset.forName("UTF-8"))));
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        translog.close();
        TranslogConfig config = translog.getConfig();

        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        Files.copy(ckp, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)));
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 1) + ".tlog"));
        // we add N+1 and N+2 to ensure we only delete the N+1 file and never jump ahead and wipe without the right condition
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 2) + ".tlog"));
        try (Translog tlog = new Translog(config, translogGeneration)) {
            assertNotNull(translogGeneration);
            assertFalse(tlog.syncNeeded());
            Translog.Snapshot snapshot = tlog.newSnapshot();
            for (int i = 0; i < 1; i++) {
                Translog.Operation next = snapshot.next();
                assertNotNull("operation " + i + " must be non-null", next);
                assertEquals("payload missmatch", i, Integer.parseInt(next.getSource().source.utf8ToString()));
            }
            tlog.add(new Translog.Index("test", "" + 1, Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
        }

        try {
            Translog tlog = new Translog(config, translogGeneration);
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
            List<String> syncedDocs = new ArrayList<>();
            List<String> unsynced = new ArrayList<>();
            if (randomBoolean()) {
                fail.onceFailedFailAlways();
            }
            Translog.TranslogGeneration generation = null;
            try {
                final Translog failableTLog = getFailableTranslog(fail, config, randomBoolean(), false, generation);
                try {
                    LineFileDocs lineFileDocs = new LineFileDocs(random()); //writes pretty big docs so we cross buffer boarders regularly
                    for (int opsAdded = 0; opsAdded < numOps; opsAdded++) {
                        String doc = lineFileDocs.nextDoc().toString();
                        failableTLog.add(new Translog.Index("test", "" + opsAdded, doc.getBytes(Charset.forName("UTF-8"))));
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
                            if (randomBoolean()) {
                                failableTLog.prepareCommit();
                            }
                            failableTLog.commit();
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
                } finally {
                    Checkpoint checkpoint = failableTLog.readCheckpoint();
                    if (checkpoint.numOps == unsynced.size() + syncedDocs.size()) {
                        syncedDocs.addAll(unsynced); // failed in fsync but got fully written
                        unsynced.clear();
                    }
                    generation = failableTLog.getGeneration();
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
                    IOUtils.close(getFailableTranslog(fail, config, randomBoolean(), false, generation));
                } catch (TranslogException | MockDirectoryWrapper.FakeIOException ex) {
                    // failed - that's ok, we didn't even create it
                } catch (IOException ex) {
                    assertEquals(ex.getMessage(), "__FAKE__ no space left on device");
                }
            }

            fail.failNever(); // we don't wanna fail here but we might since we write a new checkpoint and create a new tlog file
            try (Translog translog = new Translog(config, generation)) {
                Translog.Snapshot snapshot = translog.newSnapshot();
                assertEquals(syncedDocs.size(), snapshot.totalOperations());
                for (int i = 0; i < syncedDocs.size(); i++) {
                    Translog.Operation next = snapshot.next();
                    assertEquals(syncedDocs.get(i), next.getSource().source.utf8ToString());
                    assertNotNull("operation " + i + " must be non-null", next);
                }
            }
        }
    }

    public void testCheckpointOnDiskFull() throws IOException {
        Checkpoint checkpoint = new Checkpoint(randomLong(), randomInt(), randomLong());
        Path tempDir = createTempDir();
        Checkpoint.write(FileChannel::open, tempDir.resolve("foo.cpk"), checkpoint, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        Checkpoint checkpoint2 = new Checkpoint(randomLong(), randomInt(), randomLong());
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
        translog.add(new Translog.Index("test", "1", new byte[]{1}));
        translog.prepareCommit();
        Translog.TranslogGeneration generation = translog.getGeneration();
        TranslogConfig config = translog.getConfig();
        translog.close();
        translog = new Translog(config, generation);
        translog.add(new Translog.Index("test", "2", new byte[]{2}));
        translog.prepareCommit();
        Translog.View view = translog.newView();
        translog.add(new Translog.Index("test", "3", new byte[]{3}));
        translog.close();
        IOUtils.close(view);
        translog = new Translog(config, generation);
    }
}
