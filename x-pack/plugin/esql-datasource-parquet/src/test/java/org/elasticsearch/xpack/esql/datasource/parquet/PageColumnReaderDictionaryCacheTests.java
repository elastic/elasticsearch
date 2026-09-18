/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

/**
 * Verifies that {@link PageColumnReader} caches the dictionary
 * {@link org.elasticsearch.common.util.BytesRefArray} once per chunk and shares it across
 * emitted batches via atomic ref-count, while still being safe to drive across drivers
 * (producer keeps decoding while a consumer thread closes already-emitted pages).
 *
 * <p>Sharing is verified indirectly via the breaker: with {@link MockBigArrays#withCircuitBreaking()}
 * the underlying {@code BigArrays} bytes flow through the breaker. If the dictionary were
 * deep-copied per batch (the bug the cache fixes), peak breaker usage while all batches are alive
 * would scale with the batch count; with sharing it stays bounded near a single dictionary's
 * footprint.
 */
public class PageColumnReaderDictionaryCacheTests extends ESTestCase {

    /**
     * Many small batches over a single dictionary-encoded chunk share one underlying
     * {@code BytesRefArray}. We hold every emitted page alive simultaneously, then assert that
     * peak breaker usage is far below what a per-batch deep-copy would have produced. Pages are
     * then closed in random order to exercise the atomic ref-count.
     */
    public void testDictionaryArrayCachedAcrossBatches() throws IOException {
        int numRows = 5_000;
        int batchSize = 256;
        int dictSize = 64;
        IntFunction<String> valueFor = row -> "v_" + (row % dictSize);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();

        byte[] data = writeStringFile(numRows, false, valueFor);

        List<Page> pages = new ArrayList<>();
        long usedAfterFirstPage = -1;
        try (CloseableIterator<Page> iter = openIterator(blockFactory, data, batchSize)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
                if (usedAfterFirstPage < 0) {
                    usedAfterFirstPage = breaker.getUsed();
                }
            }
        }
        try {
            assertTrue("expected many batches per chunk: got " + pages.size(), pages.size() >= 20);

            int totalRows = 0;
            int ordinalPagesSeen = 0;
            for (Page page : pages) {
                BytesRefBlock block = page.getBlock(0);
                OrdinalBytesRefBlock ordinal = block.asOrdinals();
                if (ordinal != null) {
                    ordinalPagesSeen++;
                }
                BytesRef scratch = new BytesRef();
                for (int p = 0; p < page.getPositionCount(); p++) {
                    String expected = valueFor.apply(totalRows);
                    assertEquals("row " + totalRows, new BytesRef(expected), block.getBytesRef(p, scratch));
                    totalRows++;
                }
            }
            assertEquals(numRows, totalRows);
            assertEquals("every page should have hit the ordinal fast path", pages.size(), ordinalPagesSeen);

            // Sharing-by-breaker check, expressed as the average bytes added per additional
            // page rather than a multiplicative ratio. The first-page mark captures the full
            // chunk footprint (dict array + first wrapper + ordinals + page bookkeeping).
            // With sharing: each subsequent page adds only wrapper + ordinals + page overhead
            // — observed locally at ~1 KB / page for a 64-entry dict, far below the first-page
            // mark since the dict array dominates that baseline.
            // Without sharing (the regression we want to detect): every page deep-copies the
            // dict, so per-page growth approximates the *entire* first-page mark.
            // Bounding per-page growth at 1/3 of the first-page mark cleanly separates the two
            // regimes: shared sits around 0.15x; per-batch deep copy would land near 1.0x.
            long peak = breaker.getUsed();
            long extraPages = pages.size() - 1;
            long perPageGrowth = extraPages > 0 ? (peak - usedAfterFirstPage) / extraPages : 0;
            long perPageCeiling = usedAfterFirstPage / 3;
            logger.debug(
                "dict cache test breaker stats: usedAfterFirstPage={}, peak={}, pages={}, perPageGrowth={}, perPageCeiling={}",
                usedAfterFirstPage,
                peak,
                pages.size(),
                perPageGrowth,
                perPageCeiling
            );
            assertTrue(
                "average per-page breaker growth ["
                    + perPageGrowth
                    + " bytes] should stay below 1/3 of first-page footprint ["
                    + perPageCeiling
                    + " bytes] across "
                    + pages.size()
                    + " pages — a per-batch deep copy of the dictionary would push this near 1.0x",
                perPageGrowth < perPageCeiling
            );
        } finally {
            List<Page> shuffled = new ArrayList<>(pages);
            Collections.shuffle(shuffled, new Random(randomLong()));
            for (Page page : shuffled) {
                page.releaseBlocks();
            }
        }
        assertEquals("breaker must return to zero after random-order release", 0, breaker.getUsed());
    }

    /**
     * Reads a multi-row-group file with a tracking breaker wired through {@link MockBigArrays}.
     * The breaker must return to zero — a leaked cached {@code BytesRefArray} would manifest as
     * a positive remainder because its underlying {@code BigArrays} bytes are never released.
     */
    public void testCachedDictionaryArrayReleasedOnChunkBoundary() throws IOException {
        int numRows = 6_000;
        int batchSize = 256;
        int dictSize = 50;
        IntFunction<String> valueFor = row -> "term_" + (row % dictSize);
        // Small row group → multiple chunks → exercises the close-and-rebuild path between
        // chunks where the previous reader's cached array must be released.
        byte[] data = writeStringFile(numRows, false, valueFor, /* rowGroupSize */ 8 * 1024L);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();

        int totalRows = 0;
        try (CloseableIterator<Page> iter = openIterator(blockFactory, data, batchSize)) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertEquals(numRows, totalRows);
        assertEquals("breaker must return to zero — a leaked cached BytesRefArray would show here", 0, breaker.getUsed());
    }

    /**
     * Producer-thread keeps decoding pages while a separate consumer-thread closes already-emitted
     * pages after a randomized small delay (and a {@code page.allowPassingToDifferentDriver()} hop
     * to mirror {@code AsyncExternalSourceOperatorFactory}). Repeated for many iterations and
     * iterator instances; the breaker must still return to zero. With a non-thread-safe shared
     * refcount, this would race and either leak or trigger an assertion in {@code decRef}.
     */
    public void testCrossDriverCloseDuringDecode() throws Exception {
        int iterations = 50;
        int numRows = 1_500;
        int batchSize = 64;
        int dictSize = 32;
        IntFunction<String> valueFor = row -> "v_" + (row % dictSize);
        byte[] data = writeStringFile(numRows, false, valueFor);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();

        ExecutorService consumer = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> consumerFailure = new AtomicReference<>();
        try {
            for (int i = 0; i < iterations; i++) {
                CountDownLatch consumed = new CountDownLatch(1);
                List<Page> emitted = new ArrayList<>();
                int rowsRead = 0;
                try (CloseableIterator<Page> iter = openIterator(blockFactory, data, batchSize)) {
                    while (iter.hasNext()) {
                        Page page = iter.next();
                        rowsRead += page.getPositionCount();
                        page.allowPassingToDifferentDriver();
                        emitted.add(page);
                    }
                }
                assertEquals(numRows, rowsRead);
                final List<Page> toClose = emitted;
                consumer.execute(() -> {
                    try {
                        for (Page page : toClose) {
                            try {
                                Thread.sleep(0, randomIntBetween(0, 200_000));
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                            page.releaseBlocks();
                        }
                    } catch (Throwable t) {
                        consumerFailure.compareAndSet(null, t);
                    } finally {
                        consumed.countDown();
                    }
                });
                if (consumed.await(30, TimeUnit.SECONDS) == false) {
                    fail("consumer did not finish in time on iteration " + i);
                }
                if (consumerFailure.get() != null) {
                    throw new AssertionError("consumer thread failed on iteration " + i, consumerFailure.get());
                }
            }
        } finally {
            consumer.shutdown();
            assertTrue("consumer did not terminate", consumer.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertEquals("breaker must return to zero across all cross-driver iterations", 0, breaker.getUsed());
    }

    // --- helpers ---

    private CloseableIterator<Page> openIterator(BlockFactory blockFactory, byte[] data, int batchSize) throws IOException {
        StorageObject so = storageObject(data);
        // Force optimizedReader=true so we exercise the PageColumnReader / OptimizedParquetColumnIterator paths.
        return new ParquetFormatReader(blockFactory, true).read(so, FormatReadContext.of(List.of("name"), batchSize));
    }

    private byte[] writeStringFile(int numRows, boolean optional, IntFunction<String> valueFor) throws IOException {
        return writeStringFile(numRows, optional, valueFor, 64 * 1024L);
    }

    private byte[] writeStringFile(int numRows, boolean optional, IntFunction<String> valueFor, long rowGroupSize) throws IOException {
        Types.MessageTypeBuilder schemaBuilder = Types.buildMessage();
        var col = optional ? Types.optional(BINARY) : Types.required(BINARY);
        col.as(LogicalTypeAnnotation.stringType());
        schemaBuilder.addField(col.named("name"));
        MessageType schema = schemaBuilder.named("dict_cache_test");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.set("parquet.enable.dictionary", "true");
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(conf)
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withDictionaryPageSize(64 * 1024)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(4 * 1024)
                .build()
        ) {
            for (int i = 0; i < numRows; i++) {
                Group g = factory.newGroup();
                String v = valueFor.apply(i);
                if (v != null) {
                    g.append("name", v);
                }
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    @SuppressWarnings("unused")
    private static StorageObject storageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.EPOCH;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://dict-cache-test.parquet");
            }
        };
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        out.close();
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://dict-cache-test.parquet";
            }
        };
    }
}
