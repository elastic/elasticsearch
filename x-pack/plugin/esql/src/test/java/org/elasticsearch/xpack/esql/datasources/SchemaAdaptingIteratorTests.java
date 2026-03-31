/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SchemaAdaptingIteratorTests extends ESTestCase {

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    public void testIdentityPassThrough() {
        List<Attribute> schema = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD));
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(new int[] { 0, 1 }, null);

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(42, 3);
        Block bBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("hello"), 3);
        Page inputPage = new Page(3, new Block[] { aBlock, bBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), schema, mapping, blockFactory)) {
            assertTrue(iter.hasNext());
            Page result = iter.next();
            assertThat(result.getPositionCount(), equalTo(3));
            assertThat(result.getBlockCount(), equalTo(2));

            IntBlock resultA = result.getBlock(0);
            assertThat(resultA.getInt(0), equalTo(42));

            assertFalse(iter.hasNext());
        }
    }

    public void testColumnReorder() {
        List<Attribute> unified = List.of(attr("b", DataType.KEYWORD), attr("a", DataType.INTEGER));
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(new int[] { 1, 0 }, null);

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(10, 2);
        Block bBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("x"), 2);
        Page inputPage = new Page(2, new Block[] { aBlock, bBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getBlockCount(), equalTo(2));

            IntBlock resultA = result.getBlock(1);
            assertThat(resultA.getInt(0), equalTo(10));
        }
    }

    public void testMissingColumnNullFill() {
        List<Attribute> unified = List.of(attr("a", DataType.INTEGER), attr("missing", DataType.LONG), attr("b", DataType.KEYWORD));
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(new int[] { 0, -1, 1 }, null);

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(1, 4);
        Block bBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("v"), 4);
        Page inputPage = new Page(4, new Block[] { aBlock, bBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getBlockCount(), equalTo(3));
            assertThat(result.getPositionCount(), equalTo(4));

            Block nullBlock = result.getBlock(1);
            assertTrue(nullBlock.isNull(0));
            assertTrue(nullBlock.isNull(3));
        }
    }

    public void testCastIntToLong() {
        List<Attribute> unified = List.of(attr("val", DataType.LONG));
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(
            new int[] { 0 },
            new DataType[] { DataType.LONG }
        );

        IntBlock intBlock = blockFactory.newConstantIntBlockWith(123, 2);
        Page inputPage = new Page(2, new Block[] { intBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            LongBlock longBlock = result.getBlock(0);
            assertThat(longBlock.getLong(0), equalTo(123L));
            assertThat(longBlock.getLong(1), equalTo(123L));
        }
    }

    public void testCastIntToDouble() {
        List<Attribute> unified = List.of(attr("val", DataType.DOUBLE));
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(
            new int[] { 0 },
            new DataType[] { DataType.DOUBLE }
        );

        IntBlock intBlock = blockFactory.newConstantIntBlockWith(42, 3);
        Page inputPage = new Page(3, new Block[] { intBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            DoubleBlock doubleBlock = result.getBlock(0);
            assertThat(doubleBlock.getDouble(0), equalTo(42.0));
            assertThat(doubleBlock.getDouble(2), equalTo(42.0));
        }
    }

    public void testCastDatetimeToDateNanos() {
        List<Attribute> unified = List.of(attr("ts", DataType.DATE_NANOS));
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(
            new int[] { 0 },
            new DataType[] { DataType.DATE_NANOS }
        );

        long millisValue = 1711800000000L;
        LongBlock datetimeBlock = blockFactory.newConstantLongBlockWith(millisValue, 2);
        Page inputPage = new Page(2, new Block[] { datetimeBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            LongBlock nanosBlock = result.getBlock(0);
            assertThat(nanosBlock.getLong(0), equalTo(millisValue * 1_000_000L));
            assertThat(nanosBlock.getLong(1), equalTo(millisValue * 1_000_000L));
        }
    }

    public void testEmptyPage() {
        List<Attribute> unified = List.of(attr("a", DataType.INTEGER), attr("b", DataType.LONG));
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(new int[] { 0, -1 }, null);

        IntBlock emptyBlock = blockFactory.newConstantIntBlockWith(0, 0);
        Page inputPage = new Page(0, new Block[] { emptyBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getPositionCount(), equalTo(0));
            assertThat(result.getBlockCount(), equalTo(2));
        }
    }

    public void testMemoryCleanupOnFailure() {
        List<Attribute> unified = List.of(attr("a", DataType.INTEGER), attr("b", DataType.LONG));
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(
            new int[] { 0, 1 },
            new DataType[] { null, DataType.DATE_NANOS }
        );

        IntBlock intBlock1 = blockFactory.newConstantIntBlockWith(1, 2);
        IntBlock intBlock2 = blockFactory.newConstantIntBlockWith(2, 2);
        Page inputPage = new Page(2, new Block[] { intBlock1, intBlock2 });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            RuntimeException e = expectThrows(RuntimeException.class, iter::next);
            assertThat(e.getMessage(), containsString("Failed to adapt page"));
        }
    }

    public void testCloseDelegation() {
        List<Attribute> unified = List.of(attr("a", DataType.INTEGER));
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(new int[] { 0 }, null);

        AtomicBoolean closed = new AtomicBoolean(false);
        CloseableIterator<Page> delegate = new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Page next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };

        SchemaAdaptingIterator iter = new SchemaAdaptingIterator(delegate, unified, mapping, blockFactory);
        assertFalse(closed.get());
        iter.close();
        assertTrue(closed.get());
    }

    /**
     * Mirrors production usage: full attributes include partition columns appended after
     * data columns, but only the data prefix is passed to SchemaAdaptingIterator via
     * {@code attributes.subList(0, mapping.columnCount())}.
     */
    public void testDataColumnSubListWithPartitionSuffix() {
        List<Attribute> dataColumns = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(new int[] { 1, 0 }, null);

        IntBlock idBlock = blockFactory.newConstantIntBlockWith(7, 2);
        Block nameBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("Alice"), 2);
        Page inputPage = new Page(2, new Block[] { idBlock, nameBlock });

        List<Attribute> fullAttributes = List.of(
            attr("id", DataType.INTEGER),
            attr("name", DataType.KEYWORD),
            attr("year", DataType.INTEGER)
        );
        List<Attribute> subList = fullAttributes.subList(0, mapping.columnCount());
        assertThat(subList.size(), equalTo(2));

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), subList, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getBlockCount(), equalTo(2));
            assertThat(result.getPositionCount(), equalTo(2));

            IntBlock resultId = result.getBlock(1);
            assertThat(resultId.getInt(0), equalTo(7));
        }
    }

    /**
     * Verifies the constructor rejects a schema whose size doesn't match the mapping's
     * column count. This guards against accidentally passing the full attributes list
     * (including partition columns) instead of just the data column prefix.
     */
    public void testConstructorRejectsMismatchedSchemaSize() {
        List<Attribute> threeColumnSchema = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD), attr("c", DataType.LONG));
        SchemaReconciliation.ColumnMapping twoColumnMapping = new SchemaReconciliation.ColumnMapping(new int[] { 0, 1 }, null);

        CloseableIterator<Page> emptyIter = new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Page next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {}
        };

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new SchemaAdaptingIterator(emptyIter, threeColumnSchema, twoColumnMapping, blockFactory)
        );
        assertThat(ex.getMessage(), containsString("Schema size [3] does not match mapping column count [2]"));
        assertThat(ex.getMessage(), containsString("partition columns"));
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type);
    }

    private static CloseableIterator<Page> singlePageIterator(Page page) {
        return new CloseableIterator<>() {
            private boolean consumed = false;

            @Override
            public boolean hasNext() {
                return consumed == false;
            }

            @Override
            public Page next() {
                if (consumed) {
                    throw new NoSuchElementException();
                }
                consumed = true;
                return page;
            }

            @Override
            public void close() {}
        };
    }
}
