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
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.bwcompat.OldIndexBackwardsCompatibilityIT;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.*;

/**
 *
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class TranslogTests extends ESTestCase {

    private static final Pattern PARSE_LEGACY_ID_PATTERN = Pattern.compile("^" + Translog.TRANSLOG_FILE_PREFIX + "(\\d+)((\\.recovering))?$");

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

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

    protected Translog create(Path path) throws IOException {
        Settings build = Settings.settingsBuilder()
                .put(TranslogConfig.INDEX_TRANSLOG_FS_TYPE, TranslogWriter.Type.SIMPLE.name())
                .build();
        TranslogConfig translogConfig = new TranslogConfig(shardId, path, build, Translog.Durabilty.REQUEST, BigArrays.NON_RECYCLING_INSTANCE, null);
        return new Translog(translogConfig);
    }

    protected void addToTranslogAndList(Translog translog, ArrayList<Translog.Operation> list, Translog.Operation op) {
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
                translogDir.resolve(string);
                validPathString = true;
            } catch (InvalidPathException ex) {
                // some FS don't like our random file names -- let's just skip these random choices
            }
        } while (Translog.PARSE_STRICT_ID_PATTERN.matcher(string).matches() || validPathString == false);
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
        translog.prepareCommit();
        assertThat(translog.read(loc3).getSource().source.toBytesArray(), equalTo(new BytesArray(new byte[]{3})));
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

        long firstId = translog.currentFileGeneration();
        translog.prepareCommit();
        assertThat(translog.currentFileGeneration(), Matchers.not(equalTo(firstId)));

        snapshot = translog.newSnapshot();
        assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(ops.size()));
        snapshot.close();

        translog.commit();
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
            StreamInput in = StreamInput.wrap(out.bytes());
            stats = new TranslogStats();
            stats.readFrom(in);
        }
        return stats;
    }

    @Test
    public void testStats() throws IOException {
        final long firstOperationPosition = translog.getFirstOperationPosition();
        TranslogStats stats = stats();
        assertThat(stats.estimatedNumberOfOperations(), equalTo(0l));
        long lastSize = stats.translogSizeInBytes().bytes();
        assertThat((int) firstOperationPosition, greaterThan(CodecUtil.headerLength(TranslogWriter.TRANSLOG_CODEC)));
        assertThat(lastSize, equalTo(firstOperationPosition));

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
        translog.prepareCommit();
        stats = stats();
        assertThat(stats.estimatedNumberOfOperations(), equalTo(4l));
        assertThat(stats.translogSizeInBytes().bytes(), greaterThan(lastSize));

        translog.commit();
        stats = stats();
        assertThat(stats.estimatedNumberOfOperations(), equalTo(0l));
        assertThat(stats.translogSizeInBytes().bytes(), equalTo(firstOperationPosition));
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

        translog.prepareCommit();
        addToTranslogAndList(translog, ops, new Translog.Index("test", "3", new byte[]{3}));

        Translog.Snapshot snapshot2 = translog.newSnapshot();
        translog.commit();
        assertThat(snapshot2, SnapshotMatchers.equalsTo(ops));
        assertThat(snapshot2.estimatedTotalOperations(), equalTo(ops.size()));


        assertThat(snapshot1, SnapshotMatchers.equalsTo(ops.get(0)));
        snapshot1.close();
        snapshot2.close();
    }

    public void testSnapshotOnClosedTranslog() throws IOException {
        assertTrue(Files.exists(translogDir.resolve(Translog.getFilename(1))));
        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        translog.close();
        try {
            Translog.Snapshot snapshot = translog.newSnapshot();
            fail("translog is closed");
        } catch (AlreadyClosedException ex) {
            assertThat(ex.getMessage(), containsString("translog-1.tlog is already closed can't increment"));
        }
    }

    @Test
    public void deleteOnSnapshotRelease() throws Exception {
        ArrayList<Translog.Operation> firstOps = new ArrayList<>();
        addToTranslogAndList(translog, firstOps, new Translog.Create("test", "1", new byte[]{1}));

        Translog.Snapshot firstSnapshot = translog.newSnapshot();
        assertThat(firstSnapshot.estimatedTotalOperations(), equalTo(1));
        translog.commit();
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
        translog.commit();
        assertFileIsPresent(translog, 3); // it's the current nothing should be deleted
        assertFileDeleted(translog, 2);

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
    public void testTranslogChecksums() throws Exception {
        List<Translog.Location> locations = new ArrayList<>();

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
        List<Translog.Location> locations = new ArrayList<>();

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
                                if (loc.generation < view.minTranslogGeneration()) {
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
            locations.add(translog.add(new Translog.Create("test", "" + op, Integer.toString(++count).getBytes(Charset.forName("UTF-8")))));
            if (rarely() && translogOperations > op+1) {
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
        assertEquals(read.getSource().source.toUtf8(), Integer.toString(count));
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
            locations.add(translog.add(new Translog.Create("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (frequently()) {
                translog.sync();
                lastSynced = op;
            }
        }
        assertEquals(translogOperations, translog.totalOperations());
        final Translog.Location lastLocation = translog.add(new Translog.Create("test", "" + translogOperations, Integer.toString(translogOperations).getBytes(Charset.forName("UTF-8"))));

        final Checkpoint checkpoint = Checkpoint.read(translog.location().resolve(Translog.CHECKPOINT_FILE_NAME));
        try (final ImmutableTranslogReader reader = translog.openReader(translog.location().resolve(Translog.getFilename(translog.currentFileGeneration())), checkpoint)) {
            assertEquals(lastSynced + 1, reader.totalOperations());
            for (int op = 0; op < translogOperations; op++) {
                Translog.Location location = locations.get(op);
                if (op <= lastSynced) {
                    final Translog.Operation read = reader.read(location);
                    assertEquals(Integer.toString(op), read.getSource().source.toUtf8());
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

        final TranslogReader reader = randomBoolean() ? writer : translog.openReader(writer.path(), Checkpoint.read(translog.location().resolve(Translog.CHECKPOINT_FILE_NAME)));
        for (int i = 0; i < numOps; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            reader.readBytes(buffer, reader.getFirstOperationOffset() + 4*i);
            buffer.flip();
            final int value = buffer.getInt();
            assertEquals(i, value);
        }

        out.reset(bytes);
        out.writeInt(2048);
        writer.add(new BytesArray(bytes));

        if (reader instanceof ImmutableTranslogReader) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            try {
                reader.readBytes(buffer, reader.getFirstOperationOffset() + 4 * numOps);
                fail("read past EOF?");
            } catch (EOFException ex) {
                // expected
            }
        } else {
            // live reader!
            ByteBuffer buffer = ByteBuffer.allocate(4);
            final long pos = reader.getFirstOperationOffset() + 4 * numOps;
            reader.readBytes(buffer, pos);
            buffer.flip();
            final int value = buffer.getInt();
            assertEquals(2048, value);
        }
        IOUtils.close(writer, reader);
    }

    public void testBasicRecovery() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        Translog.TranslogGeneration translogGeneration = null;
        int minUncommittedOp = -1;
        final boolean commitOften = randomBoolean();
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Create("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            final boolean commit = commitOften ? frequently() : rarely();
            if (commit && op < translogOperations-1) {
                translog.commit();
                minUncommittedOp = op+1;
                translogGeneration = translog.getGeneration();
            }
        }
        translog.sync();
        TranslogConfig config = translog.getConfig();

        translog.close();
        config.setTranslogGeneration(translogGeneration);
        translog = new Translog(config);
        if (translogGeneration == null) {
            assertEquals(0, translog.stats().estimatedNumberOfOperations());
            assertEquals(1, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                assertNull(snapshot.next());
            }
        } else {
            assertEquals("lastCommitted must be 1 less than current", translogGeneration.translogFileGeneration + 1, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                for (int i = minUncommittedOp; i < translogOperations; i++) {
                    assertEquals("expected operation" + i + " to be in the previous translog but wasn't", translog.currentFileGeneration() - 1, locations.get(i).generation);
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null", next);
                    assertEquals(i, Integer.parseInt(next.getSource().source.toUtf8()));
                }
            }
        }
    }

    public void testRecoveryUncommitted() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        final int prepareOp = randomIntBetween(0, translogOperations-1);
        Translog.TranslogGeneration translogGeneration = null;
        final boolean sync = randomBoolean();
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Create("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (op == prepareOp) {
                translogGeneration = translog.getGeneration();
                translog.prepareCommit();
                assertEquals("expected this to be the first commit", 1l, translogGeneration.translogFileGeneration);
                assertNotNull(translogGeneration.translogUUID);
            }
        }
        if (sync) {
            translog.sync();
        }
        // we intentionally don't close the tlog that is in the prepareCommit stage since we try to recovery the uncommitted
        // translog here as well.
        TranslogConfig config = translog.getConfig();
        config.setTranslogGeneration(translogGeneration);
        try (Translog translog = new Translog(config)) {
            assertNotNull(translogGeneration);
            assertEquals("lastCommitted must be 2 less than current - we never finished the commit", translogGeneration.translogFileGeneration + 2, translog.currentFileGeneration());
            assertFalse(translog.syncNeeded());
            try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                int upTo = sync ? translogOperations : prepareOp;
                for (int i = 0; i < upTo; i++) {
                    Translog.Operation next = snapshot.next();
                    assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                    assertEquals("payload missmatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.toUtf8()));
                }
            }
        }
        if (randomBoolean()) { // recover twice
            try (Translog translog = new Translog(config)) {
                assertNotNull(translogGeneration);
                assertEquals("lastCommitted must be 3 less than current - we never finished the commit and run recovery twice", translogGeneration.translogFileGeneration + 3, translog.currentFileGeneration());
                assertFalse(translog.syncNeeded());
                try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                    int upTo = sync ? translogOperations : prepareOp;
                    for (int i = 0; i < upTo; i++) {
                        Translog.Operation next = snapshot.next();
                        assertNotNull("operation " + i + " must be non-null synced: " + sync, next);
                        assertEquals("payload missmatch, synced: " + sync, i, Integer.parseInt(next.getSource().source.toUtf8()));
                    }
                }
            }
        }

    }

    public void testSnapshotFromStreamInput() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        List<Translog.Operation> ops = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations; op++) {
            Translog.Create test = new Translog.Create("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")));
            ops.add(test);
        }
        Translog.writeOperations(out, ops);
        final List<Translog.Operation> readOperations = Translog.readOperations(StreamInput.wrap(out.bytes()));
        assertEquals(ops.size(), readOperations.size());
        assertEquals(ops, readOperations);
    }

    public void testLocationHashCodeEquals() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        List<Translog.Location> locations2 = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        try(Translog translog2 = create(createTempDir())) {
            for (int op = 0; op < translogOperations; op++) {
                locations.add(translog.add(new Translog.Create("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
                locations2.add(translog2.add(new Translog.Create("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
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
            locations.add(translog.add(new Translog.Create("test", "" + op, Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (randomBoolean()) {
                translog.commit();
                firstUncommitted = op + 1;
            }
        }
        TranslogConfig config = translog.getConfig();
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        translog.close();

        config.setTranslogGeneration(new Translog.TranslogGeneration(randomRealisticUnicodeOfCodepointLengthBetween(1, translogGeneration.translogUUID.length()),translogGeneration.translogFileGeneration));
        try {
            new Translog(config);
            fail("translog doesn't belong to this UUID");
        } catch (TranslogCorruptedException ex) {

        }
        config.setTranslogGeneration(translogGeneration);
        this.translog = new Translog(config);
        try (Translog.Snapshot snapshot = this.translog.newSnapshot()) {
            for (int i = firstUncommitted; i < translogOperations; i++) {
                Translog.Operation next = snapshot.next();
                assertNotNull("" + i, next);
                assertEquals(Integer.parseInt(next.getSource().source.toUtf8()), i);
            }
            assertNull(snapshot.next());
        }
    }

    public void testUpgradeOldTranslogFiles() throws IOException {
        List<Path> indexes = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(getBwcIndicesPath(), "index-*.zip")) {
            for (Path path : stream) {
                indexes.add(path);
            }
        }
        TranslogConfig config = this.translog.getConfig();
        Translog.TranslogGeneration gen = translog.getGeneration();
        this.translog.close();
        try {
            Translog.upgradeLegacyTranslog(logger, translog.getConfig());
            fail("no generation set");
        } catch (IllegalArgumentException ex) {

        }
        translog.getConfig().setTranslogGeneration(gen);
        try {
            Translog.upgradeLegacyTranslog(logger, translog.getConfig());
            fail("already upgraded generation set");
        } catch (IllegalArgumentException ex) {

        }

        for (Path indexFile : indexes) {
            final String indexName = indexFile.getFileName().toString().replace(".zip", "").toLowerCase(Locale.ROOT);
            Version version = Version.fromString(indexName.replace("index-", ""));
            if (version.onOrAfter(Version.V_2_0_0_beta1)) {
                continue;
            }
            Path unzipDir = createTempDir();
            Path unzipDataDir = unzipDir.resolve("data");
            // decompress the index
            try (InputStream stream = Files.newInputStream(indexFile)) {
                TestUtil.unzip(stream, unzipDir);
            }
            // check it is unique
            assertTrue(Files.exists(unzipDataDir));
            Path[] list = FileSystemUtils.files(unzipDataDir);
            if (list.length != 1) {
                throw new IllegalStateException("Backwards index must contain exactly one cluster but was " + list.length);
            }
            // the bwc scripts packs the indices under this path
            Path src = list[0].resolve("nodes/0/indices/" + indexName);
            Path translog = list[0].resolve("nodes/0/indices/" + indexName).resolve("0").resolve("translog");

            assertTrue("[" + indexFile + "] missing index dir: " + src.toString(), Files.exists(src));
            assertTrue("[" + indexFile + "] missing translog dir: " + translog.toString(), Files.exists(translog));
            Path[] tlogFiles =  FileSystemUtils.files(translog);
            assertEquals(tlogFiles.length, 1);
            final long size = Files.size(tlogFiles[0]);

            final long generation = parseLegacyTranslogFile(tlogFiles[0]);
            assertTrue(generation >= 1);
            logger.info("upgrading index {} file: {} size: {}", indexName, tlogFiles[0].getFileName(), size);
            TranslogConfig upgradeConfig = new TranslogConfig(config.getShardId(), translog, config.getIndexSettings(), config.getDurabilty(), config.getBigArrays(), config.getThreadPool());
            upgradeConfig.setTranslogGeneration(new Translog.TranslogGeneration(null, generation));
            Translog.upgradeLegacyTranslog(logger, upgradeConfig);
            try (Translog upgraded = new Translog(upgradeConfig)) {
                assertEquals(generation + 1, upgraded.getGeneration().translogFileGeneration);
                assertEquals(upgraded.getRecoveredReaders().size(), 1);
                final long headerSize;
                if (version.before(Version.V_1_4_0_Beta1)) {
                    assertTrue(upgraded.getRecoveredReaders().get(0).getClass().toString(), upgraded.getRecoveredReaders().get(0).getClass() == LegacyTranslogReader.class);
                   headerSize = 0;
                } else {
                    assertTrue(upgraded.getRecoveredReaders().get(0).getClass().toString(), upgraded.getRecoveredReaders().get(0).getClass() == LegacyTranslogReaderBase.class);
                    headerSize = CodecUtil.headerLength(TranslogWriter.TRANSLOG_CODEC);
                }
                List<Translog.Operation> operations = new ArrayList<>();
                try (Translog.Snapshot snapshot = upgraded.newSnapshot()) {
                    Translog.Operation op = null;
                    while ((op = snapshot.next()) != null) {
                        operations.add(op);
                    }
                }
                if (size > headerSize) {
                    assertFalse(operations.toString(), operations.isEmpty());
                } else {
                    assertTrue(operations.toString(), operations.isEmpty());
                }
            }
        }
    }

    /**
     * this tests a set of files that has some of the operations flushed with a buffered translog such that tlogs are truncated.
     * 3 of the 6 files are created with ES 1.3 and the rest is created wiht ES 1.4 such that both the checksummed as well as the
     * super old version of the translog without a header is tested.
     */
    public void testOpenAndReadTruncatedLegacyTranslogs() throws IOException {
        Path zip = getDataPath("/org/elasticsearch/index/translog/legacy_translogs.zip");
        Path unzipDir = createTempDir();
        try (InputStream stream = Files.newInputStream(zip)) {
            TestUtil.unzip(stream, unzipDir);
        }
        TranslogConfig config = this.translog.getConfig();
        int count = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(unzipDir)) {

            for (Path legacyTranslog : stream) {
                logger.debug("upgrading {} ", legacyTranslog.getFileName());
                Path directory = legacyTranslog.resolveSibling("translog_" + count++);
                Files.createDirectories(directory);
                Files.copy(legacyTranslog, directory.resolve(legacyTranslog.getFileName()));
                TranslogConfig upgradeConfig = new TranslogConfig(config.getShardId(), directory, config.getIndexSettings(), config.getDurabilty(), config.getBigArrays(), config.getThreadPool());
                try {
                    Translog.upgradeLegacyTranslog(logger, upgradeConfig);
                    fail("no generation set");
                } catch (IllegalArgumentException ex) {
                    // expected
                }
                long generation = parseLegacyTranslogFile(legacyTranslog);
                upgradeConfig.setTranslogGeneration(new Translog.TranslogGeneration(null, generation));
                Translog.upgradeLegacyTranslog(logger, upgradeConfig);
                try (Translog tlog = new Translog(upgradeConfig)) {
                    List<Translog.Operation> operations = new ArrayList<>();
                    try (Translog.Snapshot snapshot = tlog.newSnapshot()) {
                        Translog.Operation op = null;
                        while ((op = snapshot.next()) != null) {
                            operations.add(op);
                        }
                    }
                    logger.debug("num ops recovered: {} for file {} ", operations.size(), legacyTranslog.getFileName());
                    assertFalse(operations.isEmpty());
                }
            }
        }
    }

    public static long parseLegacyTranslogFile(Path translogFile) {
        final String fileName = translogFile.getFileName().toString();
        final Matcher matcher = PARSE_LEGACY_ID_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            try {
                return Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                throw new IllegalStateException("number formatting issue in a file that passed PARSE_STRICT_ID_PATTERN: " + fileName + "]", e);
            }
        }
        throw new IllegalArgumentException("can't parse id from file: " + fileName);
    }
}
