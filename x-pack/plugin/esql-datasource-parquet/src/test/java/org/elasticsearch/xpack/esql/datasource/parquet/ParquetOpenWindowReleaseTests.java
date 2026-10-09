/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
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
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.NodeByteBudgetService;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.HeapFootprint;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Guards the open-time sliding-window floor: parquet-mr's dictionary row-group filter reads a
 * few KB through the reader stream, which allocates a 4 MiB window. The optimized iterator then
 * never reads that stream again. {@link ParquetStorageObjectAdapter#releaseIdleWindows()} must
 * drop the charge before the driver parks on tickets.
 *
 * <p>On main (without the release) {@link #testWindowReleasedAfterFilteredConstruction} fails:
 * {@code WINDOW_BREAKER_LABEL} stays charged ({@code windowOutstanding} equals the
 * file-clamped window, observed 19536). {@link #testReadersFinishWhenCapBelowNWindows}
 * fails the aggregate check: {@code watermark.used() >= 6 × window} while six iterators
 * are open. The fixture is small, so the window is the file length, not 4 MiB.
 */
public class ParquetOpenWindowReleaseTests extends ESTestCase {

    private static final int DICT_CARDINALITY = 16;
    private static final int ROWS = 4_096;
    private static final String FILTER_VALUE = "cat_03";
    private static final int EXPECTED_MATCHING_ROWS = ROWS / DICT_CARDINALITY;
    private static final List<String> CATEGORY_ONLY = List.of("category");

    private WindowTrackingBreaker breaker;
    private BlockFactory blockFactory;
    private ExecutorService asyncIo;
    private byte[] parquet;

    @Before
    public void init() throws IOException {
        breaker = new WindowTrackingBreaker();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        asyncIo = Executors.newFixedThreadPool(4, EsExecutors.daemonThreadFactory("test", "open-window"));
        parquet = dictionaryFilterFile();
        assertThat("dictionary-encoded fixture must not be empty", parquet.length, greaterThanOrEqualTo(1024));
    }

    @After
    public void stop() throws Exception {
        terminate(asyncIo);
    }

    /**
     * Real reader path. Dictionary filter allocates the window; after construction the window
     * charge is gone. Drain and close leak-check the watermark and breaker.
     */
    public void testWindowReleasedAfterFilteredConstruction() throws Exception {
        long windowCharge = HeapFootprint.byteArrayBytes(Math.min(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE, parquet.length));
        ParquetIoWatermark watermark = new ParquetIoWatermark(64 * 1024 * 1024);
        ImmediateAsyncStorage storage = new ImmediateAsyncStorage(parquet, asyncIo);
        try (CloseableIterator<Page> iter = openFiltered(storage, watermark)) {
            assertThat(
                "fixture must take the dictionary-filter read that allocates a window",
                breaker.windowPeak(),
                greaterThanOrEqualTo(windowCharge)
            );
            assertEquals("WINDOW_BREAKER_LABEL must be refunded after the row-group filter", 0L, breaker.windowOutstanding());
            assertEquals(EXPECTED_MATCHING_ROWS, drain(iter));
        }
        assertEquals(0, watermark.used());
        assertEquals(0, breaker.getUsed());
        assertNull(watermark.nodeByteBudget().overshootOwner());
        assertEquals(0, watermark.waiterCount());
    }

    /**
     * Six filtered iterators sharing one watermark. After construction, used is below six
     * windows. Then all six drain. On main {@code used} is at least six windows.
     */
    public void testReadersFinishWhenCapBelowNWindows() throws Exception {
        long windowFootprint = HeapFootprint.byteArrayBytes(Math.min(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE, parquet.length));
        long cap = 3 * windowFootprint;
        NodeByteBudgetService budget = new NodeByteBudgetService(cap);
        ParquetIoWatermark watermark = new ParquetIoWatermark(budget);
        ImmediateAsyncStorage storage = new ImmediateAsyncStorage(parquet, asyncIo);
        List<CloseableIterator<Page>> iters = new ArrayList<>(6);
        try {
            for (int i = 0; i < 6; i++) {
                iters.add(openFiltered(storage, watermark));
            }
            assertThat(
                "fixture must charge a window on each of the six readers",
                breaker.windowPeak(),
                greaterThanOrEqualTo(6 * windowFootprint)
            );
            assertEquals("six reader windows must be refunded after the row-group filter", 0L, breaker.windowOutstanding());
            assertThat(
                "six open iterators must not hold six windows; used=" + watermark.used() + " cap=" + cap,
                watermark.used(),
                lessThan(6 * windowFootprint)
            );
            for (CloseableIterator<Page> iter : iters) {
                assertEquals(EXPECTED_MATCHING_ROWS, drain(iter));
            }
        } finally {
            IOException first = null;
            for (CloseableIterator<Page> iter : iters) {
                try {
                    iter.close();
                } catch (IOException e) {
                    if (first == null) {
                        first = e;
                    } else {
                        first.addSuppressed(e);
                    }
                }
            }
            if (first != null) {
                throw first;
            }
        }
        assertEquals(0, watermark.used());
        assertNull(watermark.nodeByteBudget().overshootOwner());
        assertEquals(0, watermark.waiterCount());
        assertEquals(0, breaker.getUsed());
    }

    private CloseableIterator<Page> openFiltered(StorageObject storage, ParquetIoWatermark watermark) throws IOException {
        ReferenceAttribute category = new ReferenceAttribute(Source.EMPTY, "category", DataType.KEYWORD);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(
            new ParquetPushedExpressions(
                List.of(new Equals(Source.EMPTY, category, new Literal(Source.EMPTY, new BytesRef(FILTER_VALUE), DataType.KEYWORD), null))
            )
        ).withIoWatermark(watermark);
        return reader.read(
            storage,
            FormatReadContext.builder().projectedColumns(CATEGORY_ONLY).readSchema(List.of(category)).batchSize(64).build()
        );
    }

    private static int drain(CloseableIterator<Page> iter) throws Exception {
        int rows = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (iter.waitForReady().isDone() == false) {
                Thread.yield();
                continue;
            }
            Page page = iter.tryAdvance();
            if (page == null) {
                if (iter.waitForReady().isDone() == false) {
                    Thread.yield();
                    continue;
                }
                page = iter.tryAdvance();
                if (page == null) {
                    if (iter.waitForReady().isDone() == false) {
                        Thread.yield();
                        continue;
                    }
                    return rows;
                }
            }
            rows += page.getPositionCount();
            page.releaseBlocks();
        }
        fail("timed out draining iterator after 30s; window floor is still wedging admission");
        return rows;
    }

    /**
     * Dictionary-encoded {@code category} so {@code RowGroupFilter} DICTIONARY reads a dictionary
     * page through the reader stream. Tests project only {@code category} so tickets stay small
     * relative to the open-time window.
     */
    private static byte[] dictionaryFilterFile() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("category")
            .named("open_window");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withRowGroupSize(64 * 1024)
                .withPageSize(8 * 1024)
                .withDictionaryPageSize(64 * 1024)
                .build()
        ) {
            String[] categories = new String[DICT_CARDINALITY];
            for (int c = 0; c < DICT_CARDINALITY; c++) {
                categories[c] = c < 10 ? "cat_0" + c : "cat_" + c;
            }
            for (int i = 0; i < ROWS; i++) {
                writer.write(factory.newGroup().append("id", i).append("category", categories[i % DICT_CARDINALITY]));
            }
        }
        return out.toByteArray();
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() {
                        return out.size();
                    }

                    @Override
                    public void write(int b) {
                        out.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        out.write(b, off, len);
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
                return "memory://open-window.parquet";
            }
        };
    }

    private static final class WindowTrackingBreaker extends LimitedBreaker {
        private final AtomicLong windowPeak = new AtomicLong();
        private final AtomicLong windowOutstanding = new AtomicLong();
        private final AtomicLong windowUnit = new AtomicLong();

        private WindowTrackingBreaker() {
            super("open-window", ByteSizeValue.ofMb(16));
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            super.addEstimateBytesAndMaybeBreak(bytes, label);
            if (ParquetStorageObjectAdapter.WINDOW_BREAKER_LABEL.equals(label) && bytes > 0) {
                windowOutstanding.addAndGet(bytes);
                windowPeak.addAndGet(bytes);
                windowUnit.set(bytes);
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            super.addWithoutBreaking(bytes);
            // Window refunds are unlabeled. Count only exact window-sized refunds so a ticket
            // or preload release cannot zero outstanding while the reader window is still held.
            long unit = windowUnit.get();
            if (bytes < 0 && unit > 0 && -bytes == unit) {
                windowOutstanding.updateAndGet(cur -> cur >= unit ? cur + bytes : cur);
            }
        }

        long windowPeak() {
            return windowPeak.get();
        }

        long windowOutstanding() {
            return windowOutstanding.get();
        }
    }

    private static class ImmediateAsyncStorage extends AbstractTestStorageObject {
        private final byte[] data;
        private final ExecutorService asyncIo;

        private ImmediateAsyncStorage(byte[] data, ExecutorService asyncIo) {
            this.data = data;
            this.asyncIo = asyncIo;
        }

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
            return StoragePath.of("memory://open-window.parquet");
        }

        @Override
        public boolean supportsNativeAsync() {
            return true;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            DirectReadBuffer drb;
            try {
                drb = factory.allocateWritableWindow((int) length);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            asyncIo.execute(() -> {
                try {
                    int pos = (int) position;
                    int len = (int) Math.min(length, data.length - position);
                    ByteBuffer buffer = drb.buffer();
                    buffer.put(data, pos, len);
                    buffer.flip();
                    listener.onResponse(drb);
                } catch (Exception e) {
                    drb.close();
                    listener.onFailure(e);
                }
            });
        }
    }
}
