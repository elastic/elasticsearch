/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.spill.spi.SpillScope;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * The framed run over the memory store in every completion mode: records cross chunk boundaries at every offset and
 * come back in order per partition; corruption of a chunk, a trailer, the header, the index or the footer is caught
 * before anything is decoded; an oversized record, a cancellation during encoding or decoding and a page handed over
 * leave the breaker at baseline; pages round-trip through the page run.
 */
public class FramedRunTests extends ComputeTestCase {

    private static final String CODEC = "spill_codec_test";
    private static final String IO = "spill_io_test";
    private static final long DEADLINE_SECONDS = 30;

    private TestThreadPool threadPool;
    private MemorySpillStore store;
    private CircuitBreaker breaker;
    private BlockFactory factory;
    private int chunkSize;
    private int depth;

    @Before
    public void setUpStore() {
        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, CODEC, between(1, 2), 64, "spill", EsExecutors.TaskTrackingConfig.DEFAULT),
            new FixedExecutorBuilder(Settings.EMPTY, IO, between(1, 2), 64, "spill", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        factory = blockFactory();
        breaker = factory.breaker();
        store = new MemorySpillStore(
            randomFrom(MemorySpillStore.Completion.values()),
            threadPool.executor(IO),
            Long.MAX_VALUE,
            "test.limit",
            new Random(random().nextLong())
        );
        store.setShortReads(randomBoolean());
        chunkSize = between(1, 256);
        depth = between(1, 3);
    }

    @After
    public void tearDownStore() throws Exception {
        try {
            await(store::shutdown);
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private Executor codec() {
        return threadPool.executor(CODEC);
    }

    private FramedRunWriter.Options options(FramedRunMaxima maxima) {
        return new FramedRunWriter.Options(FramedRunLayout.KIND_PAGE, chunkSize, depth, maxima);
    }

    private <T> T awaitFuture(PlainActionFuture<T> future) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DEADLINE_SECONDS);
        while (future.isDone() == false) {
            store.releaseRandom();
            Thread.onSpinWait();
            if (System.nanoTime() > deadline) {
                fail("operation did not complete; held [" + store.heldOperations() + "] pending [" + store.pendingOperations() + "]");
            }
        }
        return future.actionGet();
    }

    private <T> T await(Consumer<ActionListener<T>> operation) {
        PlainActionFuture<T> future = new PlainActionFuture<>();
        operation.accept(future);
        return awaitFuture(future);
    }

    private <T> Throwable awaitFailure(Consumer<ActionListener<T>> operation) {
        PlainActionFuture<T> future = new PlainActionFuture<>();
        operation.accept(future);
        Throwable cause = expectThrows(Exception.class, () -> awaitFuture(future));
        while ((cause instanceof UncategorizedExecutionException || cause instanceof ExecutionException) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return ExceptionsHelper.unwrapCause(cause);
    }

    private FramedRunWriter newWriter(SpillScope scope, FramedRunMaxima maxima) {
        return this.<FramedRunWriter>await(l -> FramedRunWriter.create(scope, "run", breaker, codec(), options(maxima), l));
    }

    private FramedRunReader open(FramedRun run, FramedRunMaxima maxima) {
        return this.<FramedRunReader>await(l -> FramedRunReader.open(run.name(), run.object(), breaker, maxima, between(1, 4096), l));
    }

    private long append(FramedRunWriter writer, int partition, byte[] bytes) {
        return this.<Long>await(l -> writer.append(partition, out -> out.writeBytes(bytes, 0, bytes.length), null, l));
    }

    private byte[] readRecord(FramedRunReader.RecordCursor cursor) {
        FramedRunReader.Record record = this.<FramedRunReader.Record>await(cursor::next);
        if (record == null) {
            return null;
        }
        try (record) {
            return BytesReference.toBytes(record.payload());
        }
    }

    private byte[] randomRecord(int maxBytes) {
        byte[] bytes = new byte[between(0, maxBytes)];
        random().nextBytes(bytes);
        return bytes;
    }

    public void testRecordsCrossChunkBoundariesAndComeBackPerPartition() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRunWriter writer = newWriter(scope, FramedRunMaxima.DEFAULT);
        int partitions = between(1, 4);
        int count = between(1, 200);
        List<List<byte[]>> expected = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
            expected.add(new ArrayList<>());
        }
        long lastOffset = -1;
        for (int i = 0; i < count; i++) {
            int partition = between(0, partitions - 1);
            byte[] bytes = randomRecord(3 * chunkSize + 1);
            long offset = append(writer, partition, bytes);
            assertThat("offsets grow", offset, greaterThan(lastOffset));
            lastOffset = offset;
            expected.get(partition).add(bytes);
        }
        FramedRun run = await(writer::finish);
        assertThat(run.records(), equalTo((long) count));
        assertThat(run.header().chunkSize(), equalTo(chunkSize));
        assertThat("the object stays outside the breaker", breaker.getUsed(), equalTo(0L));
        try (FramedRunReader reader = open(run, FramedRunMaxima.DEFAULT)) {
            assertThat(reader.partitions(), equalTo(partitionsWritten(expected)));
            for (int p = 0; p < reader.partitions(); p++) {
                assertThat(reader.records(p), equalTo((long) expected.get(p).size()));
                try (FramedRunReader.RecordCursor cursor = reader.cursor(p)) {
                    for (byte[] bytes : expected.get(p)) {
                        assertArrayEquals(bytes, readRecord(cursor));
                    }
                    assertThat(readRecord(cursor), nullValue());
                    assertThat("the end is sticky", readRecord(cursor), nullValue());
                }
            }
            assertThat(reader.records(reader.partitions() + 1), equalTo(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    private static int partitionsWritten(List<List<byte[]>> expected) {
        int last = -1;
        for (int p = 0; p < expected.size(); p++) {
            if (expected.get(p).isEmpty() == false) {
                last = p;
            }
        }
        return last + 1;
    }

    /**
     * A reader that holds the records' positions, as a value run's rows do, reads them back at their offsets without
     * the index, in any order, and a read outside the run fails before it allocates.
     */
    public void testRecordsReadBackAtTheirOffsets() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRunWriter writer = newWriter(scope, FramedRunMaxima.DEFAULT);
        int count = between(1, 100);
        List<byte[]> records = new ArrayList<>();
        List<Long> offsets = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] bytes = randomRecord(3 * chunkSize + 1);
            offsets.add(append(writer, 0, bytes));
            records.add(bytes);
        }
        FramedRun run = await(writer::finish);
        try (FramedRunReader reader = open(run, FramedRunMaxima.DEFAULT); FramedRunReader.RecordCursor cursor = reader.cursor(0)) {
            List<Integer> order = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                order.add(i);
            }
            if (randomBoolean()) {
                Collections.shuffle(order, random());
            }
            for (int i : order) {
                PlainActionFuture<ReleasableBytesReference> future = new PlainActionFuture<>();
                cursor.readAt(offsets.get(i), records.get(i).length, future);
                try (ReleasableBytesReference read = awaitFuture(future)) {
                    assertArrayEquals(records.get(i), BytesReference.toBytes(read));
                }
            }
            PlainActionFuture<ReleasableBytesReference> outside = new PlainActionFuture<>();
            cursor.readAt(run.footer().logicalBytes() - 1, 2, outside);
            Exception e = expectThrows(Exception.class, () -> awaitFuture(outside));
            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(IllegalArgumentException.class));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    public void testEmptyRun() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRunWriter writer = newWriter(scope, FramedRunMaxima.DEFAULT);
        FramedRun run = await(writer::finish);
        assertThat(run.records(), equalTo(0L));
        assertThat(run.partitions(), equalTo(0));
        try (FramedRunReader reader = open(run, FramedRunMaxima.DEFAULT); FramedRunReader.RecordCursor cursor = reader.cursor(0)) {
            assertThat(readRecord(cursor), nullValue());
        }
        await(scope::close);
    }

    public void testOversizedRecordPoisonsTheWriter() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRunMaxima maxima = new FramedRunMaxima(
            FramedRunMaxima.DEFAULT.chunkBytes(),
            between(1, 50),
            FramedRunMaxima.DEFAULT.indexEntries()
        );
        FramedRunWriter writer = newWriter(scope, maxima);
        byte[] small = randomRecord((int) maxima.recordBytes());
        append(writer, 0, small);
        byte[] large = new byte[(int) maxima.recordBytes() + between(1, 100)];
        Throwable e = this.<Long>awaitFailure(l -> writer.append(0, out -> out.writeBytes(large, 0, large.length), null, l));
        assertThat(e, instanceOf(IllegalStateException.class));
        assertThat(e.getMessage(), containsString("exceeds the maximum"));
        assertThat(
            this.<Long>awaitFailure(l -> writer.append(0, out -> out.writeBytes(small, 0, small.length), null, l)),
            instanceOf(IllegalStateException.class)
        );
        assertThat(this.<FramedRun>awaitFailure(writer::finish), instanceOf(IllegalStateException.class));
        await(writer::abort);
        assertThat("the aborted run returned its bytes", scope.bytesInUse(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    public void testTooManyRecordsPoisonsTheWriter() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRunMaxima maxima = new FramedRunMaxima(
            FramedRunMaxima.DEFAULT.chunkBytes(),
            FramedRunMaxima.DEFAULT.recordBytes(),
            between(1, 5)
        );
        FramedRunWriter writer = newWriter(scope, maxima);
        for (int i = 0; i < maxima.indexEntries(); i++) {
            append(writer, 0, randomRecord(10));
        }
        assertThat(
            this.<Long>awaitFailure(l -> writer.append(0, out -> out.writeByte((byte) 1), null, l)).getMessage(),
            containsString("the maximum")
        );
        await(writer::abort);
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    private FramedRun writeRun(SpillScope scope, List<byte[]> records) {
        FramedRunWriter writer = newWriter(scope, FramedRunMaxima.DEFAULT);
        for (byte[] bytes : records) {
            append(writer, 0, bytes);
        }
        return await(writer::finish);
    }

    private List<byte[]> someRecords() {
        List<byte[]> records = new ArrayList<>();
        int count = between(2, 20);
        for (int i = 0; i < count; i++) {
            records.add(randomRecord(2 * chunkSize + 1));
        }
        records.set(0, new byte[between(1, 2 * chunkSize + 1)]);
        return records;
    }

    private void assertReadFails(FramedRun run, String message) {
        try (FramedRunReader reader = open(run, FramedRunMaxima.DEFAULT); FramedRunReader.RecordCursor cursor = reader.cursor(0)) {
            Throwable e = null;
            while (e == null && cursor.hasNext()) {
                e = readFailure(cursor);
            }
            assertThat("a corrupt chunk is caught", e, instanceOf(FramedRunCorruptionException.class));
            assertThat(e.getMessage(), containsString(message));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private Throwable readFailure(FramedRunReader.RecordCursor cursor) {
        PlainActionFuture<FramedRunReader.Record> future = new PlainActionFuture<>();
        cursor.next(future);
        try {
            FramedRunReader.Record record = awaitFuture(future);
            if (record != null) {
                record.close();
            }
            return null;
        } catch (Exception e) {
            Throwable cause = e;
            while ((cause instanceof UncategorizedExecutionException || cause instanceof ExecutionException) && cause.getCause() != null) {
                cause = cause.getCause();
            }
            return ExceptionsHelper.unwrapCause(cause);
        }
    }

    public void testACorruptChunkPayloadIsCaughtByItsChecksum() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRun run = writeRun(scope, someRecords());
        // a byte of the first chunk's payload
        store.corrupt(
            run.object(),
            FramedRunLayout.HEADER_BYTES + between(0, (int) Math.min(chunkSize, run.footer().logicalBytes()) - 1),
            (byte) 0x5a
        );
        assertReadFails(run, "checksum");
        await(scope::close);
    }

    public void testACorruptChunkTrailerIsCaught() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRun run = writeRun(scope, someRecords());
        int firstPayload = (int) Math.min(chunkSize, run.footer().logicalBytes());
        // the length field of the first chunk's trailer
        store.corrupt(run.object(), FramedRunLayout.HEADER_BYTES + firstPayload + between(0, 3), (byte) 0x7f);
        assertReadFails(run, "length");
        await(scope::close);
    }

    public void testACorruptHeaderIsCaughtAtOpen() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRun run = writeRun(scope, someRecords());
        int at = randomBoolean() ? between(0, 7) : 8;
        store.corrupt(run.object(), at, (byte) 0x55);
        Throwable e = this.<FramedRunReader>awaitFailure(
            l -> FramedRunReader.open(run.name(), run.object(), breaker, FramedRunMaxima.DEFAULT, 1024, l)
        );
        assertThat(e, instanceOf(FramedRunCorruptionException.class));
        assertThat(e.getMessage(), containsString(at < 8 ? "magic" : "format version"));
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    public void testAnUnreadableTransportVersionIsCaughtAtOpen() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRun run = writeRun(scope, someRecords());
        store.corrupt(run.object(), 19, (byte) 0x7f);
        Throwable e = this.<FramedRunReader>awaitFailure(
            l -> FramedRunReader.open(run.name(), run.object(), breaker, FramedRunMaxima.DEFAULT, 1024, l)
        );
        assertThat(e, instanceOf(FramedRunCorruptionException.class));
        assertThat(e.getMessage(), containsString("transport version"));
        await(scope::close);
    }

    public void testATruncatedRunIsCaughtAtOpen() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRun run = writeRun(scope, someRecords());
        long size = run.sizeInBytes();
        store.truncate(run.object(), size - between(1, (int) Math.min(size - 1, 100)));
        Throwable e = this.<FramedRunReader>awaitFailure(
            l -> FramedRunReader.open(run.name(), run.object(), breaker, FramedRunMaxima.DEFAULT, 1024, l)
        );
        assertThat(e, instanceOf(FramedRunCorruptionException.class));
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    public void testACorruptIndexIsCaughtAtOpen() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRun run = writeRun(scope, someRecords());
        long entries = run.footer().indexOffset() + (long) run.partitions() * FramedRunLayout.DIRECTORY_ENTRY_BYTES;
        long at = entries + randomLongBetween(0, run.records() * FramedRunLayout.INDEX_ENTRY_BYTES - 1);
        store.corrupt(run.object(), at, (byte) 0x33);
        Throwable e = this.<FramedRunReader>awaitFailure(
            l -> FramedRunReader.open(run.name(), run.object(), breaker, FramedRunMaxima.DEFAULT, 1024, l)
        );
        assertThat(e, instanceOf(FramedRunCorruptionException.class));
        assertThat(e.getMessage(), containsString("index checksum"));
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    public void testARecordLargerThanTheReaderMaximumIsRejectedBeforeAllocation() {
        SpillScope scope = store.newScope("q", breaker);
        List<byte[]> records = someRecords();
        records.add(new byte[between(200, 400)]);
        FramedRun run = writeRun(scope, records);
        FramedRunMaxima tight = new FramedRunMaxima(FramedRunMaxima.DEFAULT.chunkBytes(), 100, FramedRunMaxima.DEFAULT.indexEntries());
        // the index entry is checked when its block is loaded, before the record's buffer exists: a breaker that fails the
        // test on any charge for a record proves it
        breaker = new GuardingBreaker(breaker, "spill run record");
        try (FramedRunReader reader = open(run, tight); FramedRunReader.RecordCursor cursor = reader.cursor(0)) {
            Throwable e = readFailure(cursor);
            assertThat(e, instanceOf(FramedRunCorruptionException.class));
            assertThat(e.getMessage(), containsString("exceeds the maximum"));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    public void testIndexBlocksAreEvictedBehindTheBoundedCache() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRunWriter writer = newWriter(scope, FramedRunMaxima.DEFAULT);
        int partitions = 3;
        int count = 4096 * 5 + between(1, 100);
        for (int i = 0; i < count; i++) {
            final int value = i;
            this.<Long>await(l -> writer.append(value % partitions, out -> out.writeVInt(value), null, l));
        }
        FramedRun run = await(writer::finish);
        try (FramedRunReader reader = open(run, FramedRunMaxima.DEFAULT)) {
            for (int p = 0; p < partitions; p++) {
                try (FramedRunReader.RecordCursor cursor = reader.cursor(p)) {
                    int expected = p;
                    while (true) {
                        byte[] bytes = readRecord(cursor);
                        if (bytes == null) {
                            break;
                        }
                        assertThat(new BytesArray(bytes).streamInput().readVInt(), equalTo(expected));
                        assertThat("the cache stays bounded", reader.cachedIndexBlocks(), lessThanOrEqualTo(4));
                        expected += partitions;
                    }
                    assertThat(expected, greaterThan(count - partitions - 1));
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    public void testCancellationDuringEncodingReleasesEverything() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRunWriter writer = newWriter(scope, FramedRunMaxima.DEFAULT);
        List<PlainActionFuture<Long>> appends = new ArrayList<>();
        int count = between(2, 10);
        for (int i = 0; i < count; i++) {
            byte[] bytes = randomRecord(3 * chunkSize + 1);
            PlainActionFuture<Long> future = new PlainActionFuture<>();
            writer.append(0, out -> out.writeBytes(bytes, 0, bytes.length), null, future);
            appends.add(future);
        }
        PlainActionFuture<Void> aborted = new PlainActionFuture<>();
        writer.abort(aborted);
        awaitFuture(aborted);
        for (PlainActionFuture<Long> future : appends) {
            try {
                awaitFuture(future);
            } catch (Exception e) {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                assertTrue(cause.toString(), cause instanceof TaskCancelledException || cause instanceof IllegalStateException);
            }
        }
        assertThat(this.<FramedRun>awaitFailure(writer::finish), instanceOf(IllegalStateException.class));
        assertThat(scope.bytesInUse(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    public void testCancellationDuringDecodingReleasesEverything() {
        SpillScope scope = store.newScope("q", breaker);
        FramedRun run = writeRun(scope, someRecords());
        FramedRunReader reader = open(run, FramedRunMaxima.DEFAULT);
        FramedRunReader.RecordCursor cursor = reader.cursor(0);
        PlainActionFuture<FramedRunReader.Record> pending = new PlainActionFuture<>();
        cursor.next(pending);
        cursor.close();
        reader.close();
        try {
            FramedRunReader.Record record = awaitFuture(pending);
            if (record != null) {
                record.close();
            }
        } catch (Exception e) {
            // a read that was in flight may fail instead; either way nothing is leaked
        }
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    private Page randomPage(int positions) {
        try (
            LongBlock.Builder longs = factory.newLongBlockBuilder(positions);
            BytesRefBlock.Builder keywords = factory.newBytesRefBlockBuilder(positions)
        ) {
            for (int p = 0; p < positions; p++) {
                if (randomInt(10) == 0) {
                    longs.appendNull();
                } else {
                    longs.appendLong(randomLong());
                }
                keywords.appendBytesRef(new BytesRef(randomAlphaOfLengthBetween(1, 20)));
            }
            return new Page(longs.build(), keywords.build());
        }
    }

    private static byte[] serialize(Page page) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(org.elasticsearch.TransportVersion.current());
            page.writeTo(out);
            return BytesReference.toBytes(out.bytes());
        }
    }

    public void testPagesRoundTripThroughThePageRun() throws IOException {
        SpillScope scope = store.newScope("q", breaker);
        PageRunWriter writer = new PageRunWriter(newWriter(scope, FramedRunMaxima.DEFAULT));
        int partitions = between(1, 3);
        List<List<byte[]>> expected = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
            expected.add(new ArrayList<>());
        }
        int count = between(1, 30);
        for (int i = 0; i < count; i++) {
            int partition = between(0, partitions - 1);
            Page page = randomPage(between(1, 200));
            expected.get(partition).add(serialize(page));
            this.<Long>await(l -> writer.append(partition, page, l));
            page.releaseBlocks();
        }
        FramedRun run = await(writer::finish);
        assertThat(breaker.getUsed(), equalTo(0L));
        try (FramedRunReader reader = open(run, FramedRunMaxima.DEFAULT)) {
            for (int p = 0; p < reader.partitions(); p++) {
                try (PageRunReader pages = new PageRunReader(reader, p, factory, breaker)) {
                    for (byte[] bytes : expected.get(p)) {
                        Page page = this.<Page>await(pages::next);
                        assertArrayEquals(bytes, serialize(page));
                        page.releaseBlocks();
                    }
                    assertThat(this.<Page>await(pages::next), nullValue());
                }
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    private MemorySpillStore deferredStore() {
        // the harness store goes first; the deferred one takes its place and the teardown shuts that one down
        MemorySpillStore previous = store;
        PlainActionFuture<Void> down = new PlainActionFuture<>();
        previous.shutdown(down);
        while (down.isDone() == false) {
            previous.releaseRandom();
        }
        store = new MemorySpillStore(
            MemorySpillStore.Completion.DEFERRED,
            null,
            Long.MAX_VALUE,
            "test.limit",
            new Random(random().nextLong())
        );
        return store;
    }

    public void testAPageIsHeldUntilItsBytesAreInTheStream() {
        MemorySpillStore deferred = deferredStore();
        chunkSize = between(1, 64);
        SpillScope scope = deferred.newScope("q", breaker);
        PageRunWriter writer = new PageRunWriter(newWriter(scope, FramedRunMaxima.DEFAULT));
        Page page = randomPage(between(500, 2000));
        long pageBytes = page.ramBytesUsedByBlocks();
        PlainActionFuture<Long> appended = new PlainActionFuture<>();
        writer.append(0, page, appended);
        // the operator drops its reference at once; the writer's copy keeps the page until its bytes are in the stream
        page.releaseBlocks();
        assertBusyHeld(deferred);
        assertThat("the page is still accounted while its chunks wait", breaker.getUsed(), greaterThan(pageBytes - 1));
        awaitFuture(appended);
        assertBusyReleased(pageBytes);
        FramedRun run = await(writer::finish);
        assertThat(run.records(), equalTo(1L));
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    /**
     * The in-flight bound of gate G0 on the run writer: with completions withheld, the breaker holds the source, the
     * writer's buffers and its index and nothing that grows with the bytes already handed to the store.
     */
    public void testSerializedBytesInFlightAreBoundedByTheBuffers() {
        MemorySpillStore deferred = deferredStore();
        chunkSize = between(16, 128);
        depth = between(1, 3);
        SpillScope scope = deferred.newScope("q", breaker);
        FramedRunWriter writer = newWriter(scope, FramedRunMaxima.DEFAULT);
        long baseline = breaker.getUsed();
        byte[] big = new byte[chunkSize * 40];
        PlainActionFuture<Long> appended = new PlainActionFuture<>();
        writer.append(0, out -> out.writeBytes(big, 0, big.length), null, appended);
        long bound = baseline + (long) depth * (chunkSize + FramedRunLayout.CHUNK_TRAILER_BYTES) + 64 * FramedRunLayout.INDEX_ENTRY_BYTES;
        int released = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DEADLINE_SECONDS);
        while (appended.isDone() == false) {
            assertThat("nothing beyond the buffers is in flight", breaker.getUsed(), lessThanOrEqualTo(bound));
            released += deferred.release(1);
            if (System.nanoTime() > deadline) {
                fail("stalled after " + released + " releases");
            }
        }
        awaitFuture(appended);
        assertThat("the store took the chunks one at a time", released, greaterThan(0));
        await(writer::finish);
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    /**
     * The page comes from a driver's allocation in a query scope and the writer releases its copy on the codec thread after
     * the driver has closed; query and request accounts return to baseline.
     */
    public void testAPageFromADriverAllocationIsReleasedAfterTheDriverClosed() {
        QueryScope queryScope = QueryScope.create("q", factory, ByteSizeValue.ofMb(64).getBytes(), 1024, null, ignored -> {});
        QueryScope.Lease lease = queryScope.tryAcquire();
        QueryScope.DriverAllocation allocation = queryScope.newDriverAllocation(new LocalCircuitBreaker.SizeSettings(1024, 4096));
        MemorySpillStore deferred = deferredStore();
        SpillScope scope = deferred.newScope("q", queryScope.rootView());
        PageRunWriter writer = new PageRunWriter(
            this.<FramedRunWriter>await(
                l -> FramedRunWriter.create(scope, "run", queryScope.rootView(), codec(), options(FramedRunMaxima.DEFAULT), l)
            )
        );
        Page page;
        try (LongBlock.Builder longs = allocation.blockFactory().newLongBlockBuilder(2000)) {
            for (int i = 0; i < 2000; i++) {
                longs.appendLong(i);
            }
            page = new Page(longs.build());
        }
        PlainActionFuture<Long> appended = new PlainActionFuture<>();
        writer.append(0, page, appended);
        page.releaseBlocks();
        allocation.close();
        assertThat("the writer's copy keeps the page accounted to the query", queryScope.breaker().used(), greaterThan(0L));
        awaitFuture(appended);
        await(writer::finish);
        assertThat(queryScope.breaker().used(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
        lease.close();
    }

    /**
     * Fails the test on a charge at least {@code forbidden} bytes large, which is how a record must never be allocated.
     */
    private static final class GuardingBreaker implements CircuitBreaker {
        private final CircuitBreaker delegate;
        private final String forbiddenLabel;

        GuardingBreaker(CircuitBreaker delegate, String forbiddenLabel) {
            this.delegate = delegate;
            this.forbiddenLabel = forbiddenLabel;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            delegate.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            assertThat("no allocation for a record", label, not(equalTo(forbiddenLabel)));
            delegate.addEstimateBytesAndMaybeBreak(bytes, label);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            delegate.addWithoutBreaking(bytes);
        }

        @Override
        public long getUsed() {
            return delegate.getUsed();
        }

        @Override
        public long getLimit() {
            return delegate.getLimit();
        }

        @Override
        public double getOverhead() {
            return delegate.getOverhead();
        }

        @Override
        public long getTrippedCount() {
            return delegate.getTrippedCount();
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public Durability getDurability() {
            return delegate.getDurability();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            delegate.setLimitAndOverhead(limit, overhead);
        }
    }

    private void assertBusyHeld(MemorySpillStore deferred) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DEADLINE_SECONDS);
        while (deferred.heldOperations() == 0) {
            Thread.onSpinWait();
            if (System.nanoTime() > deadline) {
                fail("no append reached the store");
            }
        }
    }

    private void assertBusyReleased(long pageBytes) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DEADLINE_SECONDS);
        while (breaker.getUsed() >= pageBytes) {
            Thread.onSpinWait();
            if (System.nanoTime() > deadline) {
                fail("the page copy was not released: " + breaker.getUsed());
            }
        }
    }
}
