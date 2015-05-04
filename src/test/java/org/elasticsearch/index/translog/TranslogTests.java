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

import org.apache.lucene.index.Term;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class TranslogTests extends ElasticsearchTestCase {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    protected Translog translog;
    protected Path translogDir;

    @Override
    protected void afterIfSuccessful() throws Exception {
        super.afterIfSuccessful();

        if (translog.isOpen()) {
            if (translog.currentId() > 1) {
                translog.markCommitted(translog.currentId());
                assertFileDeleted(translog, translog.currentId() - 1);
            }
            translog.close();
        }
        assertFileIsPresent(translog, translog.currentId());
        IOUtils.rm(translog.location()); // delete all the locations

    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // if a previous test failed we clean up things here
        translogDir = createTempDir();
        translog = create();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            translog.close();
        } finally {
            super.tearDown();
        }
    }

    protected Translog create() throws IOException {
        return new Translog(shardId,
                ImmutableSettings.settingsBuilder().put("index.translog.fs.type", TranslogFile.Type.SIMPLE.name()).build(),
                BigArrays.NON_RECYCLING_INSTANCE, translogDir);
    }

    protected void addToTranslogAndList(Translog translog, ArrayList<Translog.Operation> list, Translog.Operation op) {
        list.add(op);
        translog.add(op);
    }


    public void testIdParsingFromFile() {
        long id = randomIntBetween(0, Integer.MAX_VALUE);
        Path file = translogDir.resolve(Translog.TRANSLOG_FILE_PREFIX + id);
        assertThat(Translog.parseIdFromFileName(file), equalTo(id));

        file = translogDir.resolve(Translog.TRANSLOG_FILE_PREFIX + id + ".recovering");
        assertThat(Translog.parseIdFromFileName(file), equalTo(id));

        file = translogDir.resolve(Translog.TRANSLOG_FILE_PREFIX + randomNonTranslogPatternString(1, 10) + id);
        assertThat(Translog.parseIdFromFileName(file), equalTo(-1l));

        file = translogDir.resolve(randomNonTranslogPatternString(1, Translog.TRANSLOG_FILE_PREFIX.length() - 1));
        assertThat(Translog.parseIdFromFileName(file), equalTo(-1l));
    }

    private static String randomNonTranslogPatternString(int min, int max) {
       String string;
        do {
            string = randomRealisticUnicodeOfCodepointLength(randomIntBetween(min, max));
        } while (Translog.PARSE_ID_PATTERN.matcher(string).matches());
        return string;
    }

    @Test
    public void testRead() throws IOException {
        Translog.Location loc1 = translog.add(new Translog.Create("test", "1", new byte[]{1}));
        Translog.Location loc2 = translog.add(new Translog.Create("test", "2", new byte[]{2}));
        assertThat(translog.read(loc1).getSource().source.toBytesArray(), equalTo(new BytesArray(new byte[]{1})));
        assertThat(translog.read(loc2).getSource().source.toBytesArray(), equalTo(new BytesArray(new byte[]{2})));
        translog.sync();
        assertThat(translog.read(loc1).getSource().source.toBytesArray(), equalTo(new BytesArray(new byte[]{1})));
        assertThat(translog.read(loc2).getSource().source.toBytesArray(), equalTo(new BytesArray(new byte[]{2})));
        Translog.Location loc3 = translog.add(new Translog.Create("test", "2", new byte[]{3}));
        assertThat(translog.read(loc3).getSource().source.toBytesArray(), equalTo(new BytesArray(new byte[]{3})));
        translog.sync();
        assertThat(translog.read(loc3).getSource().source.toBytesArray(), equalTo(new BytesArray(new byte[]{3})));
    }

    @Test
    public void testSimpleOperations() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        Translog.Snapshot snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.size(0));
        snapshot.close();

        addToTranslogAndList(translog, ops, new Translog.Create("test", "1", new byte[]{1}));
        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.close();

        addToTranslogAndList(translog, ops, new Translog.Index("test", "2", new byte[]{2}));
        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(ops.size()));
        snapshot.close();

        addToTranslogAndList(translog, ops, new Translog.Delete(newUid("3")));
        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(ops.size()));
        snapshot.close();

        snapshot = translog.newSnapshot();

        Translog.Create create = (Translog.Create) snapshot.next();
        assertThat(create != null, equalTo(true));
        assertThat(create.source().toBytes(), equalTo(new byte[]{1}));

        Translog.Index index = (Translog.Index) snapshot.next();
        assertThat(index != null, equalTo(true));
        assertThat(index.source().toBytes(), equalTo(new byte[]{2}));

        Translog.Delete delete = (Translog.Delete) snapshot.next();
        assertThat(delete != null, equalTo(true));
        assertThat(delete.uid(), equalTo(newUid("3")));

        assertThat(snapshot.next(), equalTo(null));

        snapshot.close();

        long firstId = translog.currentId();
        translog.newTranslog();
        assertThat(translog.currentId(), Matchers.not(equalTo(firstId)));

        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(ops.size()));
        snapshot.close();

        translog.markCommitted(translog.currentId());
        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.size(0));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(0));
        snapshot.close();
    }

    protected TranslogStats stats() throws IOException {
        // force flushing and updating of stats
        translog.sync();
        TranslogStats stats = translog.stats();
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            stats.writeTo(out);
            BytesStreamInput in = new BytesStreamInput(out.bytes());
            stats = new TranslogStats();
            stats.readFrom(in);
        }
        return stats;
    }

    @Test
    public void testStats() throws IOException {
        TranslogStats stats = stats();
        assertThat(stats.estimatedNumberOfOperations(), equalTo(0l));
        long lastSize = stats.translogSizeInBytes().bytes();
        assertThat(lastSize, equalTo(17l));

        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        stats = stats();
        assertThat(stats.estimatedNumberOfOperations(), equalTo(1l));
        assertThat(stats.translogSizeInBytes().bytes(), greaterThan(lastSize));
        lastSize = stats.translogSizeInBytes().bytes();

        translog.add(new Translog.Index("test", "2", new byte[]{2}));
        stats = stats();
        assertThat(stats.estimatedNumberOfOperations(), equalTo(2l));
        assertThat(stats.translogSizeInBytes().bytes(), greaterThan(lastSize));
        lastSize = stats.translogSizeInBytes().bytes();

        translog.add(new Translog.Delete(newUid("3")));
        stats = stats();
        assertThat(stats.estimatedNumberOfOperations(), equalTo(3l));
        assertThat(stats.translogSizeInBytes().bytes(), greaterThan(lastSize));
        lastSize = stats.translogSizeInBytes().bytes();

        translog.add(new Translog.Delete(newUid("4")));
        translog.newTranslog();
        stats = stats();
        assertThat(stats.estimatedNumberOfOperations(), equalTo(4l));
        assertThat(stats.translogSizeInBytes().bytes(), greaterThan(lastSize));

        translog.markCommitted(2);
        stats = stats();
        assertThat(stats.estimatedNumberOfOperations(), equalTo(0l));
        assertThat(stats.translogSizeInBytes().bytes(), equalTo(17l));
    }

    @Test
    public void testSnapshot() {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        Translog.Snapshot snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.size(0));
        snapshot.close();

        addToTranslogAndList(translog, ops, new Translog.Create("test", "1", new byte[]{1}));

        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.close();

        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));

        // snapshot while another is open
        Translog.Snapshot snapshot1 = translog.newSnapshot();
        assertThat(snapshot1, SnapshotMatchers.size(1));
        assertThat(snapshot1.estimatedTotalOperations(), equalTo(1));

        snapshot.close();
        snapshot1.close();
    }

    @Test
    public void testSnapshotWithNewTranslog() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        Translog.Snapshot snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.size(0));
        snapshot.close();

        addToTranslogAndList(translog, ops, new Translog.Create("test", "1", new byte[]{1}));
        Translog.Snapshot snapshot1 = translog.newSnapshot();

        addToTranslogAndList(translog, ops, new Translog.Index("test", "2", new byte[]{2}));

        translog.newTranslog();

        addToTranslogAndList(translog, ops, new Translog.Index("test", "3", new byte[]{3}));

        Translog.Snapshot snapshot2 = translog.newSnapshot();
        assertThat(snapshot2, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot2.estimatedTotalOperations(), equalTo(ops.size()));


        assertThat(snapshot1, SnapshotMatchers.equalsTo(ops.get(0)));
        snapshot1.close();
        snapshot2.close();
    }

    public void testSnapshotOnClosedTranslog() throws IOException {
        assertTrue(Files.exists(translogDir.resolve("translog-1")));
        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        translog.close();
        try {
            Translog.Snapshot snapshot = translog.newSnapshot();
            fail("translog is closed");
        } catch (TranslogException ex) {
            assertThat(ex.getMessage(), containsString("can't increment channel"));
        }
    }

    @Test
    public void deleteOnSnapshotRelease() throws Exception {
        ArrayList<Translog.Operation> firstOps = new ArrayList<>();
        addToTranslogAndList(translog, firstOps, new Translog.Create("test", "1", new byte[]{1}));

        Translog.Snapshot firstSnapshot = translog.newSnapshot();
        assertThat(firstSnapshot.estimatedTotalOperations(), equalTo(1));
        translog.newTranslog();
        translog.markCommitted(translog.currentId());
        assertFileIsPresent(translog, 1);


        ArrayList<Translog.Operation> secOps = new ArrayList<>();
        addToTranslogAndList(translog, secOps, new Translog.Index("test", "2", new byte[]{2}));
        assertThat(firstSnapshot.estimatedTotalOperations(), equalTo(1));

        Translog.Snapshot secondSnapshot = translog.newSnapshot();
        translog.add(new Translog.Index("test", "3", new byte[]{3}));
        assertThat(secondSnapshot, SnapshotMatchers.equalsTo(secOps));
        assertThat(secondSnapshot.estimatedTotalOperations(), equalTo(1));
        assertFileIsPresent(translog, 1);
        assertFileIsPresent(translog, 2);

        firstSnapshot.close();
        assertFileDeleted(translog, 1);
        assertFileIsPresent(translog, 2);
        secondSnapshot.close();
        assertFileIsPresent(translog, 2); // it's the current nothing should be deleted
        translog.newTranslog();
        translog.markCommitted(translog.currentId());
        assertFileIsPresent(translog, 3); // it's the current nothing should be deleted
        assertFileDeleted(translog, 2);

    }


    public void assertFileIsPresent(Translog translog, long id) {
        if (Files.exists(translogDir.resolve(translog.getFilename(id)))) {
            return;
        }
        fail(translog.getFilename(id) + " is not present in any location: " + translog.location());
    }

    public void assertFileDeleted(Translog translog, long id) {
        assertFalse("translog [" + id + "] still exists", Files.exists(translog.location().resolve(translog.getFilename(id))));
    }

    static class LocationOperation {
        final Translog.Operation operation;
        final Translog.Location location;

        public LocationOperation(Translog.Operation operation, Translog.Location location) {
            this.operation = operation;
            this.location = location;
        }

    }

    @Test
    public void testConcurrentWritesWithVaryingSize() throws Throwable {
        final int opsPerThread = randomIntBetween(10, 200);
        int threadCount = 2 + randomInt(5);

        logger.info("testing with [{}] threads, each doing [{}] ops", threadCount, opsPerThread);
        final BlockingQueue<LocationOperation> writtenOperations = new ArrayBlockingQueue<>(threadCount * opsPerThread);

        Thread[] threads = new Thread[threadCount];
        final Throwable[] threadExceptions = new Throwable[threadCount];
        final CountDownLatch downLatch = new CountDownLatch(1);
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        downLatch.await();
                        for (int opCount = 0; opCount < opsPerThread; opCount++) {
                            Translog.Operation op;
                            switch (randomFrom(Translog.Operation.Type.values())) {
                                case CREATE:
                                    op = new Translog.Create("test", threadId + "_" + opCount,
                                            randomUnicodeOfLengthBetween(1, 20 * 1024).getBytes("UTF-8"));
                                    break;
                                case SAVE:
                                    op = new Translog.Index("test", threadId + "_" + opCount,
                                            randomUnicodeOfLengthBetween(1, 20 * 1024).getBytes("UTF-8"));
                                    break;
                                case DELETE:
                                    op = new Translog.Delete(new Term("_uid", threadId + "_" + opCount),
                                            1 + randomInt(100000),
                                            randomFrom(VersionType.values()));
                                    break;
                                case DELETE_BY_QUERY:
                                    // deprecated
                                    continue;
                                default:
                                    throw new ElasticsearchException("not supported op type");
                            }

                            Translog.Location loc = translog.add(op);
                            writtenOperations.add(new LocationOperation(op, loc));
                        }
                    } catch (Throwable t) {
                        threadExceptions[threadId] = t;
                    }
                }
            });
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
                case SAVE:
                    Translog.Index indexOp = (Translog.Index) op;
                    Translog.Index expIndexOp = (Translog.Index) expectedOp;
                    assertEquals(expIndexOp.id(), indexOp.id());
                    assertEquals(expIndexOp.routing(), indexOp.routing());
                    assertEquals(expIndexOp.type(), indexOp.type());
                    assertEquals(expIndexOp.source(), indexOp.source());
                    assertEquals(expIndexOp.version(), indexOp.version());
                    assertEquals(expIndexOp.versionType(), indexOp.versionType());
                    break;
                case CREATE:
                    Translog.Create createOp = (Translog.Create) op;
                    Translog.Create expCreateOp = (Translog.Create) expectedOp;
                    assertEquals(expCreateOp.id(), createOp.id());
                    assertEquals(expCreateOp.routing(), createOp.routing());
                    assertEquals(expCreateOp.type(), createOp.type());
                    assertEquals(expCreateOp.source(), createOp.source());
                    assertEquals(expCreateOp.version(), createOp.version());
                    assertEquals(expCreateOp.versionType(), createOp.versionType());
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

    @Test
    @LuceneTestCase.BadApple(bugUrl = "corrupting size can cause OOME")
    public void testTranslogChecksums() throws Exception {
        List<Translog.Location> locations = newArrayList();

        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations; op++) {
            String ascii = randomAsciiOfLengthBetween(1, 50);
            locations.add(translog.add(new Translog.Create("test", "" + op, ascii.getBytes("UTF-8"))));
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

    @Test
    public void testTruncatedTranslogs() throws Exception {
        List<Translog.Location> locations = newArrayList();

        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations; op++) {
            String ascii = randomAsciiOfLengthBetween(1, 50);
            locations.add(translog.add(new Translog.Create("test", "" + op, ascii.getBytes("UTF-8"))));
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


    @Test
    public void testVerifyTranslogIsNotDeleted() throws IOException {
        assertFileIsPresent(translog, 1);
        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        Translog.Snapshot snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.size(1));
        assertFileIsPresent(translog, 1);
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        if (randomBoolean()) {
            translog.close();
            snapshot.close();
        } else {
            snapshot.close();
            translog.close();
        }

        assertFileIsPresent(translog, 1);
    }

    /** Tests that concurrent readers and writes maintain view and snapshot semantics */
    @Test
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
        final List<Throwable> errors = new CopyOnWriteArrayList<>();
        logger.debug("using [{}] readers. [{}] writers. flushing every ~[{}] ops.", readers.length, writers.length, flushEveryOps);
        for (int i = 0; i < writers.length; i++) {
            final String threadId = "writer_" + i;
            writers[i] = new Thread(new AbstractRunnable() {
                @Override
                public void doRun() throws BrokenBarrierException, InterruptedException {
                    barrier.await();
                    int counter = 0;
                    while (run.get()) {
                        long id = idGenerator.incrementAndGet();
                        final Translog.Operation op;
                        switch (Translog.Operation.Type.values()[((int) (id % Translog.Operation.Type.values().length))]) {
                            case CREATE:
                                op = new Translog.Create("type", "" + id, new byte[]{(byte) id});
                                break;
                            case SAVE:
                                op = new Translog.Index("type", "" + id, new byte[]{(byte) id});
                                break;
                            case DELETE:
                                op = new Translog.Delete(newUid("" + id));
                                break;
                            case DELETE_BY_QUERY:
                                // deprecated
                                continue;
                            default:
                                throw new ElasticsearchException("unknown type");
                        }
                        Translog.Location location = translog.add(op);
                        Translog.Location existing = writtenOps.put(op, location);
                        if (existing != null) {
                            fail("duplicate op [" + op + "], old entry at " + location);
                        }
                        writtenOpsLatch.get().countDown();
                        counter++;
                    }
                    logger.debug("--> [{}] done. wrote [{}] ops.", threadId, counter);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("--> writer [{}] had an error", t, threadId);
                    errors.add(t);
                }
            }, threadId);
            writers[i].start();
        }

        for (int i = 0; i < readers.length; i++) {
            final String threadId = "reader_" + i;
            readers[i] = new Thread(new AbstractRunnable() {
                Translog.View view = null;
                Set<Translog.Operation> writtenOpsAtView;

                @Override
                public void onFailure(Throwable t) {
                    logger.error("--> reader [{}] had an error", t, threadId);
                    errors.add(t);
                    closeView();
                }

                void closeView() {
                    if (view != null) {
                        view.close();
                    }
                }

                void newView() {
                    closeView();
                    view = translog.newView();
                    // captures the currently written ops so we know what to expect from the view
                    writtenOpsAtView = new HashSet<>(writtenOps.keySet());
                    logger.debug("--> [{}] opened view from [{}]", threadId, view.minTranslogId());
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
                        try (Translog.Snapshot snapshot = view.snapshot()) {
                            Translog.Operation op;
                            while ((op = snapshot.next()) != null) {
                                expectedOps.remove(op);
                            }
                        }
                        if (expectedOps.isEmpty() == false) {
                            StringBuilder missed = new StringBuilder("missed ").append(expectedOps.size()).append(" operations");
                            boolean failed = false;
                            for (Translog.Operation op : expectedOps) {
                                final Translog.Location loc = writtenOps.get(op);
                                if (loc.translogId < view.minTranslogId()) {
                                    // writtenOps is only updated after the op was written to the translog. This mean
                                    // that ops written to the translog before the view was taken (and will be missing from the view)
                                    // may yet be available in writtenOpsAtView, meaning we will erroneously expect them
                                    continue;
                                }
                                failed = true;
                                missed.append("\n --> [").append(op).append("] written at ").append(loc);
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
            long previousId = translog.currentId();
            for (int iterations = scaledRandomIntBetween(10, 200); iterations > 0 && errors.isEmpty(); iterations--) {
                writtenOpsLatch.set(new CountDownLatch(flushEveryOps));
                while (writtenOpsLatch.get().await(200, TimeUnit.MILLISECONDS) == false) {
                    if (errors.size() > 0) {
                        break;
                    }
                }
                long newId = translog.newTranslog();
                translog.markCommitted(previousId);
                previousId = newId;
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
            final Translog.Location location = translog.add(new Translog.Create("test", "" + op, Integer.toString(++count).getBytes(Charset.forName("UTF-8"))));
            if (randomBoolean()) {
                assertTrue("at least one operation pending", translog.syncNeeded());
                assertTrue("this operation has not been synced", translog.ensureSynced(location));
                assertFalse("the last call to ensureSycned synced all previous ops", translog.syncNeeded()); // we are the last location so everything should be synced
                translog.add(new Translog.Create("test", "" + op, Integer.toString(++count).getBytes(Charset.forName("UTF-8"))));
                assertTrue("one pending operation", translog.syncNeeded());
                assertFalse("this op has been synced before", translog.ensureSynced(location)); // not syncing now
                assertTrue("we only synced a previous operation yet", translog.syncNeeded());
            }
            if (rarely()) {
                translog.newTranslog();
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
        List<Translog.Location> locations = newArrayList();
        int translogOperations = randomIntBetween(10, 100);
        int count = 0;
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Create("test", "" + op, Integer.toString(++count).getBytes(Charset.forName("UTF-8")))));
            if (rarely()) {
                translog.newTranslog();
            }
        }
        Collections.shuffle(locations, random());
        Translog.Location max = locations.get(0);
        for (Translog.Location location : locations) {
            max = max(max, location);
        }

        assertEquals(max.translogId, translog.currentId());
        final Translog.Operation read = translog.read(max);
        assertEquals(read.getSource().source.toUtf8(), Integer.toString(count));
    }

    public static Translog.Location max(Translog.Location a, Translog.Location b) {
        if (a.compareTo(b) > 0) {
            return a;
        }
        return b;
    }
}
