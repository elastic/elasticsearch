/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.lucene.read.DelegatingBlockLoaderFactory;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.parquet.ParquetTestFiles;
import org.elasticsearch.xpack.parquet.engine.ExternalParquetCodecReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Many iterators over one segment at once, and the memory that they hold. The segment shares its page cache, its scan registry and its
 * reader between the iterators of all threads, and a Lucene iterator has no close hook, so the segment's close is what releases what the
 * abandoned ones hold. The circuit breaker that accounts the blocks and buffers is checked by the test framework after each test: it must
 * be back to zero.
 */
public class ParquetConcurrencyTests extends ESTestCase {

    private static final String LOCATION = "memory://f.parquet";
    private static final int ROWS = 10_000;

    private record Docs(int[] docs) implements BlockLoader.Docs {
        @Override
        public int count() {
            return docs.length;
        }

        @Override
        public int get(int i) {
            return docs[i];
        }

        @Override
        public boolean mayContainDuplicates() {
            return false;
        }
    }

    private static final class Segment {
        final ParquetDocValuesProducer producer;
        final org.apache.lucene.index.FieldInfos fieldInfos;
        final BlockFactory blocks;
        final BlockLoader.BlockFactory loaderBlocks;

        Segment(byte[] file, BlockFactory blocks) throws IOException {
            List<ColumnBinding> bindings = List.of(
                new ColumnBinding("id", 0, "id", DocValuesType.NUMERIC),
                new ColumnBinding("n", 1, "n", DocValuesType.NUMERIC),
                new ColumnBinding("name", 2, "name", DocValuesType.BINARY)
            );
            ParquetFileInfo info = ParquetFileInfo.read(LOCATION, ParquetTestFiles.storageObject(file));
            ExternalFilePiece piece = ExternalFilePiece.of(info, 0, info.rowGroups().size(), 0, bindings);
            this.blocks = blocks;
            this.producer = new ParquetDocValuesProducer(
                bindings,
                piece,
                (location, length) -> ParquetTestFiles.storageObject(file),
                512,
                blocks
            );
            producer.resolveStorage();
            this.fieldInfos = new ExternalParquetCodecReader(bindings, piece).getFieldInfos();
            this.loaderBlocks = new DelegatingBlockLoaderFactory(blocks) {
                @Override
                public BlockLoader.Block constantNulls(int count) {
                    return blocks.newConstantNullBlock(count);
                }
            };
        }

        NumericDocValues numeric(String field) throws IOException {
            return producer.getNumeric(fieldInfos.fieldInfo(field));
        }

        BinaryDocValues binary(String field) throws IOException {
            return producer.getBinary(fieldInfos.fieldInfo(field));
        }
    }

    private BlockFactory accountedBlocks(CircuitBreaker breaker) {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
    }

    /** One thread's work: sequential runs, random reads and block reads, each checked against the value that the file holds. */
    private static void work(Segment segment, Random random, int rounds) throws IOException {
        for (int round = 0; round < rounds; round++) {
            NumericDocValues ids = segment.numeric("id");
            NumericDocValues ns = segment.numeric("n");
            BinaryDocValues names = segment.binary("name");
            // a sequential run from a random start, as a slice of a query reads
            int start = random.nextInt(ROWS - 2_000);
            int end = start + 500 + random.nextInt(1_500);
            for (int doc = start; doc < end; doc++) {
                assertTrue(ids.advanceExact(doc));
                assertThat(ids.longValue(), equalTo(doc * 7L));
                assertThat(ns.advanceExact(doc), equalTo(doc % 3 != 0));
                if (doc % 3 != 0) {
                    assertThat(ns.longValue(), equalTo((long) doc));
                }
                assertThat(names.advanceExact(doc), equalTo(doc % 5 != 0));
                if (doc % 5 != 0) {
                    assertThat(names.binaryValue().utf8ToString(), equalTo("name-" + doc));
                }
            }
            // a few random reads, which go backward and forward
            for (int i = 0; i < 30; i++) {
                int doc = random.nextInt(ROWS);
                assertTrue(ids.advanceExact(doc));
                assertThat(ids.longValue(), equalTo(doc * 7L));
            }
            // a block read of a range, which is what ES|QL asks for
            int from = random.nextInt(ROWS - 1_500);
            int[] docs = new int[1_000];
            for (int i = 0; i < docs.length; i++) {
                docs[i] = from + i;
            }
            BlockLoader.OptionalColumnAtATimeReader reader = (BlockLoader.OptionalColumnAtATimeReader) segment.numeric("id");
            try (Block block = (Block) reader.tryRead(segment.loaderBlocks, new Docs(docs), 0, true, null, false, false)) {
                LongBlock longs = (LongBlock) block;
                for (int i = 0; i < docs.length; i++) {
                    assertThat(longs.getLong(longs.getFirstValueIndex(i)), equalTo(docs[i] * 7L));
                }
            }
            BlockLoader.OptionalColumnAtATimeReader nameReader = (BlockLoader.OptionalColumnAtATimeReader) segment.binary("name");
            try (Block block = (Block) nameReader.tryRead(segment.loaderBlocks, new Docs(docs), 0, false, null, false, false)) {
                BytesRefBlock strings = (BytesRefBlock) block;
                BytesRef scratch = new BytesRef();
                for (int i = 0; i < docs.length; i += 13) {
                    int doc = docs[i];
                    assertThat(strings.isNull(i), equalTo(doc % 5 == 0));
                    if (doc % 5 != 0) {
                        assertThat(strings.getBytesRef(strings.getFirstValueIndex(i), scratch).utf8ToString(), equalTo("name-" + doc));
                    }
                }
            }
        }
    }

    public void testManyThreadsReadOneSegmentAtOnce() throws Exception {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
        Segment segment = new Segment(ParquetTestFiles.threeColumnFile(ROWS), accountedBlocks(breaker));
        int threads = between(4, 12);
        AtomicInteger finished = new AtomicInteger();
        long seed = randomLong();
        startInParallel(threads, t -> {
            try {
                work(segment, new Random(seed + t), 12);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            finished.incrementAndGet();
        });
        assertThat(finished.get(), equalTo(threads));
        assertThat("the scans did run, and share their pages", segment.producer.scansOpened(), greaterThan(0L));
        assertThat("the threads found each other's pages", segment.producer.pageCacheHits(), greaterThan(0L));
        segment.producer.close();
    }

    public void testAbandonedIteratorsHoldNothingOnceTheSegmentIsClosed() throws Exception {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
        Segment segment = new Segment(ParquetTestFiles.threeColumnFile(ROWS), accountedBlocks(breaker));
        // read a column to the middle of a row group through many iterators, and abandon them with their scans open
        for (int i = 0; i < 40; i++) {
            BinaryDocValues names = segment.binary("name");
            for (int doc = 0; doc < 1_300; doc++) {
                names.advanceExact(doc);
            }
        }
        assertThat("scans are open", segment.producer.scansOpened(), greaterThan(0L));
        long heldWhileOpen = breaker.getUsed();
        assertThat(
            "the open scans hold memory that the breaker sees, so that the check below means something",
            heldWhileOpen,
            greaterThan(0L)
        );
        segment.producer.close();
        assertThat(
            "closing the segment releases what the abandoned iterators held (held " + heldWhileOpen + " bytes)",
            breaker.getUsed(),
            equalTo(0L)
        );
    }

    /** A node that stops reading must not keep the row group chunks of its last scans: the periodic sweep closes the idle ones. */
    public void testTheSweepClosesIdleScansWithoutTheSegmentBeingClosed() throws Exception {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
        Segment segment = new Segment(ParquetTestFiles.threeColumnFile(ROWS), accountedBlocks(breaker));
        BinaryDocValues names = segment.binary("name");
        for (int doc = 0; doc < 1_300; doc++) {
            names.advanceExact(doc);
        }
        assertThat("an open scan holds memory", breaker.getUsed(), greaterThan(0L));
        // the scan is idle after two seconds, and the sweep then closes it
        assertBusy(() -> {
            ScanRegistry.INSTANCE.sweep();
            assertThat(breaker.getUsed(), equalTo(0L));
        }, 10, java.util.concurrent.TimeUnit.SECONDS);
        // and the iterator that lost its scan reads on
        for (int doc = 1_300; doc < 1_600; doc++) {
            assertThat(names.advanceExact(doc), equalTo(doc % 5 != 0));
            if (doc % 5 != 0) {
                assertThat(names.binaryValue().utf8ToString(), equalTo("name-" + doc));
            }
        }
        segment.producer.close();
    }

    public void testAFailedReadDoesNotLeakAndTheSegmentStaysUsable() throws Exception {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
        byte[] file = ParquetTestFiles.threeColumnFile(ROWS);
        List<ColumnBinding> bindings = List.of(new ColumnBinding("id", 0, "id", DocValuesType.NUMERIC));
        ParquetFileInfo info = ParquetFileInfo.read(LOCATION, ParquetTestFiles.storageObject(file));
        ExternalFilePiece piece = ExternalFilePiece.of(info, 0, info.rowGroups().size(), 0, bindings);
        // a file that fails to read every other stream, as a repository does when a connection drops
        AtomicInteger streams = new AtomicInteger();
        var flaky = ParquetTestFiles.storageObject(file);
        var failing = new org.elasticsearch.xpack.esql.datasources.spi.StorageObject() {
            @Override
            public org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity storageIdentity() {
                return flaky.storageIdentity();
            }

            @Override
            public void attachMetrics(org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics metrics, String scheme) {}

            @Override
            public java.io.InputStream newStream() throws IOException {
                return flaky.newStream();
            }

            @Override
            public java.io.InputStream newStream(long position, long length) throws IOException {
                if (streams.incrementAndGet() % 2 == 0) {
                    throw new IOException("connection dropped");
                }
                return flaky.newStream(position, length);
            }

            @Override
            public long length() throws IOException {
                return flaky.length();
            }

            @Override
            public java.time.Instant lastModified() throws IOException {
                return flaky.lastModified();
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public org.elasticsearch.xpack.esql.datasources.spi.StoragePath path() {
                return flaky.path();
            }

            @Override
            public String version() {
                return "1";
            }
        };
        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
            bindings,
            piece,
            (location, length) -> failing,
            512,
            accountedBlocks(breaker)
        );
        producer.resolveStorage();
        var field = new ExternalParquetCodecReader(bindings, piece).getFieldInfos().fieldInfo("id");
        int failures = 0;
        int successes = 0;
        for (int attempt = 0; attempt < 40; attempt++) {
            NumericDocValues ids = producer.getNumeric(field);
            try {
                int doc = (attempt * 997) % ROWS;
                assertTrue(ids.advanceExact(doc));
                assertThat(ids.longValue(), equalTo(doc * 7L));
                successes++;
            } catch (Exception e) {
                failures++;
            }
        }
        assertThat("some reads failed", failures, greaterThan(0));
        assertThat("and some succeeded: a failure does not poison the segment", successes, greaterThan(0));
        producer.close();
    }

    public void testIteratorsThatReachTheEndOfTheDataReleaseTheirScans() throws Exception {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
        Segment segment = new Segment(ParquetTestFiles.threeColumnFile(ROWS), accountedBlocks(breaker));
        NumericDocValues ids = segment.numeric("id");
        int count = 0;
        for (int doc = ids.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = ids.nextDoc()) {
            count++;
        }
        assertThat(count, equalTo(ROWS));
        // a scan closes itself at the end of its row group, so nothing is held after a full pass
        assertThat(breaker.getUsed(), equalTo(0L));
        segment.producer.close();
    }
}
