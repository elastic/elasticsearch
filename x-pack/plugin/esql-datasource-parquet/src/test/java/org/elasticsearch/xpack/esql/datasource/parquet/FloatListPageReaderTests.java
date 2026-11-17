/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

/**
 * Level-stream tests for {@link FloatListPageReader}. parquet-mr's writer refuses to produce malformed level streams,
 * so pages are assembled by hand here, in both page formats, with the same expectations the general
 * {@link ParquetColumnDecoding.ListColumnReader} path has in {@link ParquetListCorruptionTests}.
 */
public class FloatListPageReaderTests extends ESTestCase {

    private static final String FILE = "memory://floats.parquet";

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static ColumnInfo info() {
        Type list = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.FLOAT).named("x");
        return ParquetFormatReader.resolveColumnInfo(new MessageType("test", list), "x");
    }

    public void testListsNullsAndEmptiesAcrossPages() {
        // Row 0: [1, 2]; row 1: null list (def 0); row 2: empty list (def 1); row 3: [3]; then a page break inside a
        // list: row 4: [4, 5 | 6] where the page boundary falls between 5 and 6.
        for (boolean v2 : new boolean[] { false, true }) {
            List<DataPage> pages = List.of(
                page(v2, new int[] { 0, 1, 0, 0, 0, 0, 1 }, new int[] { 3, 3, 0, 1, 3, 3, 3 }, new float[] { 1, 2, 3, 4, 5 }),
                page(v2, new int[] { 1 }, new int[] { 3 }, new float[] { 6 })
            );
            List<String> warnings = new ArrayList<>();
            FloatListPageReader reader = reader(pages, ErrorPolicy.STRICT, warnings);
            try (Block block = reader.readBatch(5, blockFactory, SkipWarnings.NOOP)) {
                FloatBlock floats = (FloatBlock) block;
                assertList(floats, 0, 1, 2);
                assertTrue(floats.isNull(1));
                assertTrue(floats.isNull(2));
                assertList(floats, 3, 3);
                assertList(floats, 4, 4, 5, 6);
            }
            reader.validateExhausted();
            assertThat(warnings, empty());
        }
    }

    public void testNullElementsAreDroppedAndAnnouncedOnce() {
        // Row 0: [1, null, 2]; row 1: [null] -> no values -> null position; row 2: [3].
        List<DataPage> pages = List.of(page(true, new int[] { 0, 1, 1, 0, 0 }, new int[] { 3, 2, 3, 2, 3 }, new float[] { 1, 2, 3 }));
        List<String> notices = new ArrayList<>();
        SkipWarnings loss = new SkipWarnings(ParquetColumnDecoding.NULL_LIST_ELEMENTS_SUMMARY, notices::add);
        FloatListPageReader reader = reader(pages, ErrorPolicy.STRICT, new ArrayList<>());
        try (Block block = reader.readBatch(3, blockFactory, loss)) {
            FloatBlock floats = (FloatBlock) block;
            assertList(floats, 0, 1, 2);
            assertTrue(floats.isNull(1));
            assertList(floats, 2, 3);
        }
        reader.validateExhausted();
        assertThat(notices, hasSize(2));
        assertThat(notices.getLast(), containsString(ParquetColumnDecoding.nullListElementsMessage("x")));
    }

    public void testSkipRowsAdvancesValues() {
        List<DataPage> pages = List.of(page(false, new int[] { 0, 1, 0, 0, 1 }, new int[] { 3, 3, 0, 3, 3 }, new float[] { 1, 2, 3, 4 }));
        FloatListPageReader reader = reader(pages, ErrorPolicy.STRICT, new ArrayList<>());
        reader.skipRows(2);
        try (Block block = reader.readBatch(1, blockFactory, SkipWarnings.NOOP)) {
            assertList((FloatBlock) block, 0, 3, 4);
        }
        reader.validateExhausted();
    }

    public void testLeadingContinuationIsDiscardedUnderLenientPolicies() {
        // Levels start mid-list: an orphan value (rep 1) precedes row 0 = [2, 3], row 1 = [4].
        List<DataPage> pages = List.of(page(true, new int[] { 1, 0, 1, 0 }, new int[] { 3, 3, 3, 3 }, new float[] { 1, 2, 3, 4 }));
        List<String> warnings = new ArrayList<>();
        FloatListPageReader reader = reader(pages, ErrorPolicy.PERMISSIVE, warnings);
        try (Block block = reader.readBatch(2, blockFactory, SkipWarnings.NOOP)) {
            FloatBlock floats = (FloatBlock) block;
            assertList(floats, 0, 2, 3);
            assertList(floats, 1, 4);
        }
        reader.validateExhausted();
        assertThat(warnings.getLast(), containsString("[1] list values dropped"));
    }

    public void testLeadingContinuationFailsUnderStrictPolicy() {
        List<DataPage> pages = List.of(page(false, new int[] { 1, 0 }, new int[] { 3, 3 }, new float[] { 1, 2 }));
        FloatListPageReader reader = reader(pages, ErrorPolicy.STRICT, new ArrayList<>());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> reader.readBatch(1, blockFactory, SkipWarnings.NOOP).close()
        );
        assertThat(e.getMessage(), containsString("starts at repetition level [1] instead of [0]"));
    }

    public void testFooterUndercountFailsInEveryMode() {
        for (ErrorPolicy policy : policies()) {
            FloatListPageReader reader = reader(
                List.of(page(true, new int[] { 0, 0 }, new int[] { 3, 3 }, new float[] { 1, 2 })),
                policy,
                new ArrayList<>()
            );
            reader.readBatch(1, blockFactory, SkipWarnings.NOOP).close();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, reader::validateExhausted);
            assertThat(e.getMessage(), containsString("[1] level values remain"));
        }
    }

    public void testFooterOvercountFailsInEveryMode() {
        for (ErrorPolicy policy : policies()) {
            FloatListPageReader reader = reader(
                List.of(page(false, new int[] { 0 }, new int[] { 3 }, new float[] { 1 })),
                policy,
                new ArrayList<>()
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> reader.readBatch(2, blockFactory, SkipWarnings.NOOP).close()
            );
            assertThat(e.getMessage(), containsString("footer row count exceeds"));
        }
    }

    public void testOutOfRangeLevelsFail() {
        // The hybrid encoder masks bit-packed values to the level width, so an out-of-range level can only arrive in an
        // RLE run, whose value is stored in whole bytes: one run of two repetition levels of 2 where the maximum is 1.
        DataPage page = new DataPageV2(
            1,
            0,
            2,
            BytesInput.from(new byte[] { 2 << 1, 2 }),
            levels(new int[] { 3, 3 }, 3),
            Encoding.PLAIN,
            floats(1, 2),
            16,
            Statistics.getBuilderForReading(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named("x")).build(),
            false
        );
        FloatListPageReader reader = reader(List.of(page), ErrorPolicy.PERMISSIVE, new ArrayList<>());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> reader.readBatch(1, blockFactory, SkipWarnings.NOOP).close()
        );
        assertThat(e.getMessage(), containsString("repetition level [2] is outside [0, 1]"));
    }

    public void testTruncatedValuesFail() {
        FloatListPageReader reader = reader(
            List.of(page(false, new int[] { 0, 1 }, new int[] { 3, 3 }, new float[] { 1 })),
            ErrorPolicy.PERMISSIVE,
            new ArrayList<>()
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> reader.readBatch(1, blockFactory, SkipWarnings.NOOP).close()
        );
        assertThat(e.getMessage(), containsString("page value data ends before its last value"));
    }

    private static List<ErrorPolicy> policies() {
        return List.of(ErrorPolicy.STRICT, new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, Long.MAX_VALUE, 0.0, false), ErrorPolicy.PERMISSIVE);
    }

    private static void assertList(FloatBlock block, int position, float... expected) {
        assertFalse("position " + position, block.isNull(position));
        assertEquals("position " + position, expected.length, block.getValueCount(position));
        int first = block.getFirstValueIndex(position);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], block.getFloat(first + i), 0.0f);
        }
    }

    private static FloatListPageReader reader(List<DataPage> pages, ErrorPolicy policy, List<String> warnings) {
        ParquetColumnDecoding.ListCorruptionHandler handler = new ParquetColumnDecoding.ListCorruptionHandler(policy, FILE, warnings::add);
        return new FloatListPageReader(new ListPageReader(pages), info(), handler, "x", FILE, 0, 0);
    }

    private static BytesInput floats(float... values) {
        ByteBuffer data = ByteBuffer.allocate(values.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (float f : values) {
            data.putFloat(f);
        }
        return BytesInput.from(data.array());
    }

    private static DataPage page(boolean v2, int[] rep, int[] def, float[] values) {
        try {
            BytesInput valueBytes = floats(values);
            BytesInput repBytes = levels(rep, 1);
            BytesInput defBytes = levels(def, 3);
            if (v2) {
                int rows = 0;
                for (int r : rep) {
                    rows += r == 0 ? 1 : 0;
                }
                return new DataPageV2(
                    rows,
                    0,
                    rep.length,
                    repBytes,
                    defBytes,
                    Encoding.PLAIN,
                    valueBytes,
                    (int) (repBytes.size() + defBytes.size() + valueBytes.size()),
                    Statistics.getBuilderForReading(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named("x")).build(),
                    false
                );
            }
            BytesInput body = BytesInput.concat(lengthPrefixed(repBytes), lengthPrefixed(defBytes), valueBytes);
            return new DataPageV1(
                BytesInput.from(body.toByteArray()),
                rep.length,
                (int) body.size(),
                Statistics.getBuilderForReading(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named("x")).build(),
                Encoding.RLE,
                Encoding.RLE,
                Encoding.PLAIN
            );
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static BytesInput lengthPrefixed(BytesInput levels) throws IOException {
        byte[] bytes = levels.toByteArray();
        return BytesInput.concat(BytesInput.fromInt(bytes.length), BytesInput.from(bytes));
    }

    private static BytesInput levels(int[] levels, int maxLevel) {
        try {
            return encodeLevels(levels, maxLevel);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static BytesInput encodeLevels(int[] levels, int maxLevel) throws IOException {
        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxLevel);
        RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth, 64, 1024, new HeapByteBufferAllocator());
        for (int level : levels) {
            encoder.writeInt(level);
        }
        return BytesInput.from(encoder.toBytes().toByteArray());
    }

    private static final class ListPageReader implements PageReader {
        private final Deque<DataPage> pages;
        private final long valueCount;

        ListPageReader(List<DataPage> pages) {
            this.pages = new ArrayDeque<>(pages);
            long count = 0;
            for (DataPage page : pages) {
                count += page.getValueCount();
            }
            this.valueCount = count;
        }

        @Override
        public DictionaryPage readDictionaryPage() {
            return null;
        }

        @Override
        public long getTotalValueCount() {
            return valueCount;
        }

        @Override
        public DataPage readPage() {
            return pages.poll();
        }
    }
}
