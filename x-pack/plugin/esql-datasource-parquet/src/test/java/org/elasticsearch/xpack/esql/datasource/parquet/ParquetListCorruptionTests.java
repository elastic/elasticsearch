/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * Focused malformed-level tests for specialized LIST decoders. parquet-mr's writer refuses to
 * produce these invalid streams, so a small {@link ColumnReader} models level and physical-value
 * cursors independently and verifies that recovery advances both.
 */
public class ParquetListCorruptionTests extends ESTestCase {

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    public void testDateNanosListDiscardsLeadingContinuation() {
        Type listType = Types.optionalList()
            .optionalElement(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("x");
        ColumnInfo info = ParquetFormatReader.resolveColumnInfo(new MessageType("test", listType), "x");
        FakeColumnReader reader = new FakeColumnReader(new int[] { 1, 0, 1, 0 }, new long[] { 0, 1, 2, 3 });
        List<String> warnings = new ArrayList<>();
        ParquetColumnDecoding.ListColumnReader input = input(reader, info, ErrorPolicy.PERMISSIVE, warnings);

        try (Block block = ParquetColumnDecoding.readListColumn(input, info, 2, blockFactory)) {
            LongBlock longs = (LongBlock) block;
            assertEquals(2, longs.getValueCount(0));
            int first = longs.getFirstValueIndex(0);
            assertEquals(1_000L, longs.getLong(first));
            assertEquals(2_000L, longs.getLong(first + 1));
            assertEquals(1, longs.getValueCount(1));
            assertEquals(3_000L, longs.getLong(longs.getFirstValueIndex(1)));
        }
        input.validateExhausted();
        assertEquals(4, reader.physicalIndex);
        assertThat(warnings.getLast(), containsString("discarded [1] orphan values"));
    }

    public void testStringDatetimeListDiscardsLeadingContinuation() {
        Type listType = Types.optionalList()
            .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("x");
        ColumnInfo fileInfo = ParquetFormatReader.resolveColumnInfo(new MessageType("test", listType), "x");
        ColumnInfo info = new ColumnInfo(
            fileInfo.descriptor(),
            fileInfo.parquetType(),
            DataType.DATETIME,
            fileInfo.maxDefLevel(),
            fileInfo.maxRepLevel(),
            fileInfo.logicalType(),
            null,
            DataType.KEYWORD
        );
        FakeColumnReader reader = new FakeColumnReader(
            new int[] { 1, 0, 1, 0 },
            new String[] { "not-a-date", "2000-01-01T00:00:00Z", "2000-01-02T00:00:00Z", "2000-01-03T00:00:00Z" }
        );
        List<String> warnings = new ArrayList<>();
        ParquetColumnDecoding.ListColumnReader input = input(reader, info, ErrorPolicy.PERMISSIVE, warnings);

        try (Block block = ParquetColumnDecoding.readListColumn(input, info, 2, blockFactory)) {
            LongBlock longs = (LongBlock) block;
            assertEquals(2, longs.getValueCount(0));
            assertEquals(946684800000L, longs.getLong(longs.getFirstValueIndex(0)));
            assertEquals(946771200000L, longs.getLong(longs.getFirstValueIndex(0) + 1));
            assertEquals(946857600000L, longs.getLong(longs.getFirstValueIndex(1)));
        }
        input.validateExhausted();
        assertEquals(4, reader.physicalIndex);
        assertEquals(2, warnings.size());
    }

    public void testUnsupportedListSkipDiscardsLeadingContinuation() {
        Type listType = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("x");
        ColumnInfo fileInfo = ParquetFormatReader.resolveColumnInfo(new MessageType("test", listType), "x");
        ColumnInfo info = new ColumnInfo(
            fileInfo.descriptor(),
            fileInfo.parquetType(),
            DataType.UNSUPPORTED,
            fileInfo.maxDefLevel(),
            fileInfo.maxRepLevel(),
            fileInfo.logicalType(),
            null,
            DataType.UNSUPPORTED
        );
        FakeColumnReader reader = new FakeColumnReader(new int[] { 1, 0, 1, 0 }, new int[] { 0, 1, 2, 3 });
        List<String> warnings = new ArrayList<>();
        ParquetColumnDecoding.ListColumnReader input = input(reader, info, ErrorPolicy.PERMISSIVE, warnings);

        try (Block block = ParquetColumnDecoding.readListColumn(input, info, 2, blockFactory)) {
            assertTrue(block.areAllValuesNull());
            assertEquals(2, block.getPositionCount());
        }
        input.validateExhausted();
        assertEquals(4, reader.physicalIndex);
        assertThat(warnings.getLast(), containsString("discarded [1] orphan values"));
    }

    public void testFooterUndercountFailsInEveryMode() {
        ColumnInfo info = intListInfo();
        for (ErrorPolicy policy : policies()) {
            FakeColumnReader reader = new FakeColumnReader(new int[] { 0, 0 }, new int[] { 1, 2 });
            ParquetColumnDecoding.ListColumnReader input = input(reader, info, policy, new ArrayList<>());
            try (Block ignored = ParquetColumnDecoding.readListColumn(input, info, 1, blockFactory)) {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, input::validateExhausted);
                assertThat(e.getMessage(), containsString("[1] level values remain"));
            }
        }
    }

    public void testZeroFooterRowsWithLevelValuesFailsInEveryMode() {
        ColumnInfo info = intListInfo();
        for (ErrorPolicy policy : policies()) {
            FakeColumnReader reader = new FakeColumnReader(new int[] { 0 }, new int[] { 1 });
            ParquetColumnDecoding.ListColumnReader input = input(reader, info, policy, new ArrayList<>());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, input::validateExhausted);
            assertThat(e.getMessage(), containsString("after [0] rows"));
        }
    }

    public void testFooterOvercountFailsInEveryMode() {
        ColumnInfo info = intListInfo();
        for (ErrorPolicy policy : policies()) {
            FakeColumnReader reader = new FakeColumnReader(new int[] { 0 }, new int[] { 1 });
            ParquetColumnDecoding.ListColumnReader input = input(reader, info, policy, new ArrayList<>());
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ParquetColumnDecoding.readListColumn(input, info, 2, blockFactory)
            );
            assertThat(e.getMessage(), containsString("footer row count exceeds"));
        }
    }

    public void testErrorRatioUsesRowsFromEarlierRowGroups() {
        ColumnInfo info = intListInfo();
        ErrorPolicy policy = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, Long.MAX_VALUE, 0.1, false);
        List<String> warnings = new ArrayList<>();
        String file = "memory://malformed-list.parquet";
        ParquetColumnDecoding.ListCorruptionHandler handler = new ParquetColumnDecoding.ListCorruptionHandler(policy, file, warnings::add);
        FakeColumnReader reader = new FakeColumnReader(new int[] { 1, 0 }, new int[] { 0, 7 });
        ParquetColumnDecoding.ListColumnReader input = new ParquetColumnDecoding.ListColumnReader(
            reader,
            info.maxDefLevel(),
            handler,
            "x",
            file,
            1,
            1_000
        );

        try (Block block = ParquetColumnDecoding.readListColumn(input, info, 1, blockFactory)) {
            assertEquals(7, ((IntBlock) block).getInt(0));
        }
        input.validateExhausted();
        assertThat(warnings.getLast(), containsString("discarded [1] orphan values"));
    }

    private static ColumnInfo intListInfo() {
        Type listType = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("x");
        return ParquetFormatReader.resolveColumnInfo(new MessageType("test", listType), "x");
    }

    private static List<ErrorPolicy> policies() {
        return List.of(ErrorPolicy.STRICT, new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, Long.MAX_VALUE, 0.0, false), ErrorPolicy.PERMISSIVE);
    }

    private static ParquetColumnDecoding.ListColumnReader input(
        FakeColumnReader reader,
        ColumnInfo info,
        ErrorPolicy policy,
        List<String> warnings
    ) {
        String file = "memory://malformed-list.parquet";
        ParquetColumnDecoding.ListCorruptionHandler handler = new ParquetColumnDecoding.ListCorruptionHandler(policy, file, warnings::add);
        return new ParquetColumnDecoding.ListColumnReader(reader, info.maxDefLevel(), handler, "x", file, 0, 0);
    }

    /**
     * Invalid-stream test double. Accessors and {@link #skip()} select the current physical value;
     * {@link #consume()} advances that cursor only after selection, while always advancing levels.
     */
    private static final class FakeColumnReader implements ColumnReader {
        private final int[] repetitionLevels;
        private final Object[] values;
        private int levelIndex;
        private int physicalIndex;
        private boolean physicalSelected;

        FakeColumnReader(int[] repetitionLevels, int[] values) {
            this(repetitionLevels, box(values));
        }

        FakeColumnReader(int[] repetitionLevels, long[] values) {
            this(repetitionLevels, box(values));
        }

        FakeColumnReader(int[] repetitionLevels, String[] values) {
            this(repetitionLevels, (Object[]) values);
        }

        private FakeColumnReader(int[] repetitionLevels, Object[] values) {
            this.repetitionLevels = repetitionLevels;
            this.values = values;
            assert repetitionLevels.length == values.length;
        }

        @Override
        public long getTotalValueCount() {
            return repetitionLevels.length;
        }

        @Override
        public ColumnDescriptor getDescriptor() {
            return null;
        }

        @Override
        public void consume() {
            if (physicalSelected) {
                physicalIndex++;
                physicalSelected = false;
            }
            levelIndex++;
        }

        @Override
        public int getCurrentRepetitionLevel() {
            return repetitionLevels[levelIndex];
        }

        @Override
        public int getCurrentDefinitionLevel() {
            return 3;
        }

        @Override
        public void writeCurrentValueToConverter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getInteger() {
            physicalSelected = true;
            return (Integer) values[physicalIndex];
        }

        @Override
        public boolean getBoolean() {
            physicalSelected = true;
            return (Boolean) values[physicalIndex];
        }

        @Override
        public long getLong() {
            physicalSelected = true;
            return ((Number) values[physicalIndex]).longValue();
        }

        @Override
        public Binary getBinary() {
            physicalSelected = true;
            return Binary.fromString((String) values[physicalIndex]);
        }

        @Override
        public float getFloat() {
            physicalSelected = true;
            return ((Number) values[physicalIndex]).floatValue();
        }

        @Override
        public double getDouble() {
            physicalSelected = true;
            return ((Number) values[physicalIndex]).doubleValue();
        }

        @Override
        public int getCurrentValueDictionaryID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void skip() {
            physicalSelected = true;
        }

        private static Object[] box(int[] values) {
            Object[] boxed = new Object[values.length];
            for (int i = 0; i < values.length; i++) {
                boxed[i] = values[i];
            }
            return boxed;
        }

        private static Object[] box(long[] values) {
            Object[] boxed = new Object[values.length];
            for (int i = 0; i < values.length; i++) {
                boxed[i] = values[i];
            }
            return boxed;
        }
    }
}
