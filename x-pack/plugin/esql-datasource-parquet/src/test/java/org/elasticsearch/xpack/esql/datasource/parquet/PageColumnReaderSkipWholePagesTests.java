/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.hamcrest.Matchers.equalTo;

/**
 * Checks that {@link PageColumnReader#skipRows} does not decompress the pages that lie wholly inside a skip. It builds the pages of
 * one required {@code INT64} column by hand, so that a decompressor that counts its calls can tell how many pages the reader
 * decompressed; {@link PageColumnReaderCorrectnessTests} checks the values that a mix of skips and reads returns over real files.
 */
public class PageColumnReaderSkipWholePagesTests extends ESTestCase {

    private static final MessageType SCHEMA = Types.buildMessage().required(INT64).named("v").named("skip_whole_pages");

    /** The rows of each page. The value of a row is its index in the row group. */
    private static final int[] PAGE_ROWS = { 7, 100, 3, 50, 1, 80 };

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();

    /** A decompressor that returns what it gets, and counts the calls so that a test sees which pages were decompressed. */
    private static final class CountingDecompressor implements BytesInputDecompressor {
        final AtomicInteger pages = new AtomicInteger();

        @Override
        public BytesInput decompress(BytesInput bytes, int decompressedSize) {
            pages.incrementAndGet();
            return bytes;
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) {
            pages.incrementAndGet();
            output.put(input.duplicate().limit(compressedSize));
        }

        @Override
        public void release() {}
    }

    private static List<PrefetchedPageReader.CompressedPage> pages(boolean withFirstRowIndex) {
        List<PrefetchedPageReader.CompressedPage> pages = new ArrayList<>();
        long row = 0;
        for (int rows : PAGE_ROWS) {
            ByteBuffer values = ByteBuffer.allocate(rows * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < rows; i++) {
                values.putLong(row + i);
            }
            Statistics<?> statistics = new LongStatistics();
            DataPageV1 page = withFirstRowIndex
                ? new DataPageV1(
                    BytesInput.from(values.array()),
                    rows,
                    values.capacity(),
                    row,
                    rows,
                    statistics,
                    Encoding.RLE,
                    Encoding.RLE,
                    Encoding.PLAIN
                )
                : new DataPageV1(
                    BytesInput.from(values.array()),
                    rows,
                    values.capacity(),
                    statistics,
                    Encoding.RLE,
                    Encoding.RLE,
                    Encoding.PLAIN
                );
            pages.add(new PrefetchedPageReader.CompressedPage(page, withFirstRowIndex ? row : -1L));
            row += rows;
        }
        return pages;
    }

    private long readOne(PageColumnReader reader) {
        LongBlock block = (LongBlock) reader.readBatch(1, blockFactory);
        try {
            assertThat(block.getPositionCount(), equalTo(1));
            return block.getLong(0);
        } finally {
            block.close();
        }
    }

    /** Runs {@code body} on a reader over the hand-built pages, and gives it the count of pages that the decompressor has seen. */
    private void withReader(boolean withFirstRowIndex, ReaderBody body) throws IOException {
        CountingDecompressor decompressor = new CountingDecompressor();
        ColumnDescriptor descriptor = SCHEMA.getColumns().getFirst();
        ColumnInfo info = new ColumnInfo(
            descriptor,
            descriptor.getPrimitiveType().getPrimitiveTypeName(),
            DataType.LONG,
            descriptor.getMaxDefinitionLevel(),
            descriptor.getMaxRepetitionLevel(),
            descriptor.getPrimitiveType().getLogicalTypeAnnotation()
        );
        long total = 0;
        for (int rows : PAGE_ROWS) {
            total += rows;
        }
        try (
            PrefetchedPageReader pageReader = new PrefetchedPageReader(
                decompressor,
                new NoopCircuitBreaker("test"),
                pages(withFirstRowIndex),
                null,
                total
            );
            PageColumnReader reader = new PageColumnReader(pageReader, descriptor, info, RowRanges.all(total))
        ) {
            body.run(reader, decompressor.pages);
        }
    }

    private interface ReaderBody {
        void run(PageColumnReader reader, AtomicInteger decompressedPages) throws IOException;
    }

    /** A skip over four whole pages and the read of the first row of the fifth decompresses one page: the fifth. */
    public void testSkipOverWholePagesDecompressesOnlyThePageThatIsRead() throws IOException {
        for (boolean withFirstRowIndex : new boolean[] { false, true }) {
            withReader(withFirstRowIndex, (reader, decompressed) -> {
                int rowsOfFirstFourPages = 7 + 100 + 3 + 50;
                reader.skipRows(rowsOfFirstFourPages);
                assertThat("a skip decompresses nothing when it ends on a page boundary", decompressed.get(), equalTo(0));
                assertThat(readOne(reader), equalTo((long) rowsOfFirstFourPages));
                assertThat(decompressed.get(), equalTo(1));
            });
        }
    }

    /** A skip that ends inside a page needs that page, and no other. */
    public void testSkipEndingInsideAPageDecompressesThatPageOnly() throws IOException {
        for (boolean withFirstRowIndex : new boolean[] { false, true }) {
            withReader(withFirstRowIndex, (reader, decompressed) -> {
                reader.skipRows(7 + 100 + 10);
                assertThat(decompressed.get(), equalTo(1));
                assertThat(readOne(reader), equalTo(117L));
                assertThat(decompressed.get(), equalTo(1));
            });
        }
    }

    /** Skips in small steps still drop the pages that the steps add up to, once the current page is used up. */
    public void testSmallSkipsDropWholePagesAfterTheCurrentPageIsUsedUp() throws IOException {
        withReader(false, (reader, decompressed) -> {
            assertThat(readOne(reader), equalTo(0L));
            assertThat(decompressed.get(), equalTo(1));
            // the rest of page 0 (6 rows) is skipped by its decoders, then pages 1 and 2 (103 rows) are dropped whole
            reader.skipRows(6);
            reader.skipRows(100);
            reader.skipRows(3);
            assertThat("pages 1 and 2 are not decompressed", decompressed.get(), equalTo(1));
            assertThat(readOne(reader), equalTo(110L));
            assertThat(decompressed.get(), equalTo(2));
        });
    }

    /** A skip to the end of the column drops every page and leaves the reader at its end. */
    public void testSkipOverTheWholeColumnDecompressesNothing() throws IOException {
        withReader(true, (reader, decompressed) -> {
            reader.skipRows(7 + 100 + 3 + 50 + 1 + 80);
            assertThat(decompressed.get(), equalTo(0));
        });
    }
}
