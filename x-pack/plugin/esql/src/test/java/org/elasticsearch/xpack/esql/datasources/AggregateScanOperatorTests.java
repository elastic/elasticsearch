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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.AggregateScanReader;
import org.elasticsearch.xpack.esql.datasources.spi.AggregateScanSpec;
import org.elasticsearch.xpack.esql.datasources.spi.AggregateScanSpec.AggOp;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Unit tests for {@link AggregateScanOperator}: verifies aggregate-to-spec lowering, multi-split
 * iteration including {@link CoalescedSplit} unwrapping, and the file-open-error fallback.
 *
 * The {@link AggregateScanReader} stub returns canned intermediate-state pages instead of opening
 * a real Parquet file, isolating the operator's driver behavior from format-specific decoding.
 */
public class AggregateScanOperatorTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final ReferenceAttribute AGE = referenceAttribute("age", DataType.INTEGER);
    private static final ReferenceAttribute SCORE = referenceAttribute("score", DataType.DOUBLE);

    public void testFactoryLowersCountStarAndMinMax() {
        Alias countStar = alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
        Alias minAge = alias("mn", new Min(Source.EMPTY, AGE));
        Alias maxScore = alias("mx", new Max(Source.EMPTY, SCORE));
        List<NamedExpression> aggregates = List.of(countStar, minAge, maxScore);
        List<Attribute> intermediates = intermediatesFor(aggregates);

        AggregateScanSpec spec = AggregateScanOperator.Factory.lower(aggregates, intermediates);

        assertEquals(3, spec.ops().size());
        assertEquals(new AggOp.CountStar(), spec.ops().get(0));
        assertEquals(new AggOp.MinField("age"), spec.ops().get(1));
        assertEquals(new AggOp.MaxField("score"), spec.ops().get(2));
        assertSame("intermediate attributes carried verbatim", intermediates.get(0), spec.intermediateAttributes().get(0));
        assertEquals(intermediates.size(), spec.intermediateAttributes().size());
    }

    public void testFactoryLowersCountField() {
        Alias countAge = alias("c", new Count(Source.EMPTY, AGE));
        List<NamedExpression> aggregates = List.of(countAge);
        List<Attribute> intermediates = intermediatesFor(aggregates);

        AggregateScanSpec spec = AggregateScanOperator.Factory.lower(aggregates, intermediates);
        assertEquals(1, spec.ops().size());
        assertEquals(new AggOp.CountField("age"), spec.ops().get(0));
    }

    public void testFactoryRejectsRawAttribute() {
        // Plain (unaliased) NamedExpression is invalid: the operator factory expects Alias(...).
        expectThrows(IllegalArgumentException.class, () -> AggregateScanOperator.Factory.lower(List.of(AGE), List.of(AGE, AGE)));
    }

    public void testFactoryRejectsFilteredAggregates() {
        // The planner rule excludes filtered aggregates upstream; the lowering step must
        // refuse them too so a future caller (or a plan-node round-trip) can't slip a
        // filtered aggregate through and silently drop the WHERE clause. Literal.TRUE is the
        // "no filter" sentinel (see AggregateFunction#hasFilter); use a real predicate.
        Expression filterExpr = greaterThanOf(AGE, of(10));
        Alias filteredCount = alias("c", new Count(Source.EMPTY, AGE).withFilter(filterExpr));
        Alias filteredMin = alias("mn", new Min(Source.EMPTY, AGE).withFilter(filterExpr));
        Alias filteredMax = alias("mx", new Max(Source.EMPTY, AGE).withFilter(filterExpr));
        for (Alias filtered : List.of(filteredCount, filteredMin, filteredMax)) {
            List<NamedExpression> aggs = List.of(filtered);
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AggregateScanOperator.Factory.lower(aggs, intermediatesFor(aggs))
            );
            assertTrue(
                "exception message must mention filtered aggregates, got: " + e.getMessage(),
                e.getMessage().contains("filtered aggregates")
            );
        }
    }

    public void testOperatorDrainsIteratorPagesPerLeaf() throws IOException {
        // Two FileSplits each producing 2 intermediate pages; the operator must yield 4 pages
        // before signalling completion.
        FileSplit split1 = newSplit("file:///a.parquet");
        FileSplit split2 = newSplit("file:///b.parquet");
        ExternalSliceQueue queue = new ExternalSliceQueue(List.of(split1, split2));
        StubAggregateScanReader reader = new StubAggregateScanReader(2, false);

        try (AggregateScanOperator op = newOperator(queue, reader)) {
            List<Page> output = drain(op);
            try {
                assertEquals(4, output.size());
                for (Page page : output) {
                    assertSeenLong(page, 0, 100L);
                }
            } finally {
                output.forEach(Page::releaseBlocks);
            }
            assertEquals(2, reader.opened);
        }
    }

    public void testOperatorRethrowsOnFileOpenError() throws IOException {
        // File-open failure must fail the query — silently emitting an unseen page would
        // under-count the aggregate. Matches the standard scan path.
        FileSplit split = newSplit("file:///broken.parquet");
        ExternalSliceQueue queue = new ExternalSliceQueue(List.of(split));
        StubAggregateScanReader reader = new StubAggregateScanReader(0, true);

        try (AggregateScanOperator op = newOperator(queue, reader)) {
            UncheckedIOException e = expectThrows(UncheckedIOException.class, op::getOutput);
            assertEquals("simulated open failure", e.getCause().getMessage());
        }
    }

    public void testOperatorRethrowsOnMidIterationFailure() throws IOException {
        // Iterator yields one good page then throws on the next hasNext(). The first page
        // has already been forwarded to the reducer, so the operator must rethrow rather
        // than emit an unseen page (which would silently truncate the aggregate).
        FileSplit split = newSplit("file:///broken-mid-iteration.parquet");
        ExternalSliceQueue queue = new ExternalSliceQueue(List.of(split));
        StubAggregateScanReader reader = new StubAggregateScanReader(1, false, true);

        List<Page> drained = new ArrayList<>();
        try (AggregateScanOperator op = newOperator(queue, reader)) {
            try {
                Page first = op.getOutput();
                assertNotNull("first row group must be emitted before the failure", first);
                drained.add(first);
                RuntimeException e = expectThrows(RuntimeException.class, op::getOutput);
                assertEquals("simulated mid-iteration failure", e.getMessage());
            } finally {
                drained.forEach(Page::releaseBlocks);
            }
        }
    }

    public void testOperatorUnwrapsCoalescedSplits() throws IOException {
        FileSplit a = newSplit("file:///a.parquet");
        FileSplit b = newSplit("file:///b.parquet");
        CoalescedSplit coalesced = new CoalescedSplit("parquet", List.of(a, b));
        ExternalSliceQueue queue = new ExternalSliceQueue(List.of(coalesced));
        StubAggregateScanReader reader = new StubAggregateScanReader(1, false);

        try (AggregateScanOperator op = newOperator(queue, reader)) {
            List<Page> output = drain(op);
            try {
                assertEquals(2, output.size());
            } finally {
                output.forEach(Page::releaseBlocks);
            }
            assertEquals("CoalescedSplit must expose both child files to the reader", 2, reader.opened);
        }
    }

    public void testOperatorPassesBoundedByteRangeToReader() throws IOException {
        // Sanity check the bounded-range arithmetic: byteRangeEnd must be offset + length.
        FileSplit split = newSplit("file:///bounded.parquet", 100, 900);
        ExternalSliceQueue queue = new ExternalSliceQueue(List.of(split));
        StubAggregateScanReader reader = new StubAggregateScanReader(1, false);

        try (AggregateScanOperator op = newOperator(queue, reader)) {
            List<Page> output = drain(op);
            try {
                assertEquals(1, output.size());
            } finally {
                output.forEach(Page::releaseBlocks);
            }
            assertEquals(1, reader.byteRanges.size());
            assertEquals(100L, reader.byteRanges.get(0)[0]);
            assertEquals(1000L, reader.byteRanges.get(0)[1]);
        }
    }

    public void testOperatorClampsUnboundedLengthToMaxValue() throws IOException {
        // FileSplit.length() is Long.MAX_VALUE for unbounded splits (single, undivided file).
        // Naively computing offset + length would overflow to a negative number; the operator
        // must clamp byteRangeEnd to Long.MAX_VALUE so the reader scans to EOF.
        FileSplit split = newSplit("file:///unbounded.parquet", 1024, Long.MAX_VALUE);
        ExternalSliceQueue queue = new ExternalSliceQueue(List.of(split));
        StubAggregateScanReader reader = new StubAggregateScanReader(1, false);

        try (AggregateScanOperator op = newOperator(queue, reader)) {
            List<Page> output = drain(op);
            try {
                assertEquals(1, output.size());
            } finally {
                output.forEach(Page::releaseBlocks);
            }
            assertEquals(1, reader.byteRanges.size());
            assertEquals(1024L, reader.byteRanges.get(0)[0]);
            assertEquals("unbounded length must clamp to Long.MAX_VALUE, not overflow", Long.MAX_VALUE, reader.byteRanges.get(0)[1]);
        }
    }

    private AggregateScanOperator newOperator(ExternalSliceQueue queue, AggregateScanReader reader) {
        Alias countStar = alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
        AggregateScanSpec spec = AggregateScanOperator.Factory.lower(List.of(countStar), intermediatesFor(List.of(countStar)));
        StorageProvider storage = Mockito.mock(StorageProvider.class);
        StorageObject obj = Mockito.mock(StorageObject.class);
        Mockito.when(storage.newObject(Mockito.any(StoragePath.class))).thenReturn(obj);
        return new AggregateScanOperator(BLOCK_FACTORY, storage, reader, queue, spec);
    }

    private static List<Page> drain(SourceOperator op) {
        List<Page> pages = new ArrayList<>();
        while (op.isFinished() == false) {
            Page page = op.getOutput();
            if (page != null) {
                pages.add(page);
            } else {
                op.finish();
            }
        }
        return pages;
    }

    private static FileSplit newSplit(String uri) {
        return newSplit(uri, 0, 1024);
    }

    private static FileSplit newSplit(String uri, long offset, long length) {
        return new FileSplit("parquet", StoragePath.of(uri), offset, length, "parquet", Map.of(), Map.of());
    }

    /**
     * Build a stub intermediate-attribute list for a set of aggregates. Uses a fixed
     * {@code (long value, boolean seen)} layout — the operator only forwards these attributes
     * to the spec and never inspects their types itself.
     */
    private static List<Attribute> intermediatesFor(List<NamedExpression> aggregates) {
        List<Attribute> out = new ArrayList<>(aggregates.size() * 2);
        for (NamedExpression ne : aggregates) {
            String name = ne.name();
            out.add(referenceAttribute(name + ".v", DataType.LONG));
            out.add(referenceAttribute(name + ".seen", DataType.BOOLEAN));
        }
        return out;
    }

    private static void assertSeenLong(Page page, int aggIdx, long expectedValue) {
        LongBlock value = (LongBlock) page.getBlock(aggIdx * 2);
        BooleanBlock seen = (BooleanBlock) page.getBlock(aggIdx * 2 + 1);
        assertTrue("aggregate at index " + aggIdx + " must be seen", seen.getBoolean(0));
        assertEquals(expectedValue, value.getLong(0));
    }

    /**
     * Stub reader that yields {@code pagesPerFile} canned intermediate pages per file, each
     * carrying COUNT=100 with seen=true. Three failure modes are configurable:
     * <ul>
     *   <li>{@code failFirstOpen}: throw {@link IOException} on the first
     *       {@code scanForAggregates} call (simulates a file-open failure).</li>
     *   <li>{@code throwAfterIteratorExhausted}: after yielding all {@code pagesPerFile}
     *       pages, the iterator throws a {@link RuntimeException} on the next
     *       {@code hasNext()} (simulates a mid-iteration decode failure).</li>
     * </ul>
     */
    private static final class StubAggregateScanReader implements AggregateScanReader {
        private final int pagesPerFile;
        private final boolean failFirstOpen;
        private final boolean throwAfterIteratorExhausted;
        private int opened;
        private final List<long[]> byteRanges = new ArrayList<>();

        StubAggregateScanReader(int pagesPerFile, boolean failFirstOpen) {
            this(pagesPerFile, failFirstOpen, false);
        }

        StubAggregateScanReader(int pagesPerFile, boolean failFirstOpen, boolean throwAfterIteratorExhausted) {
            this.pagesPerFile = pagesPerFile;
            this.failFirstOpen = failFirstOpen;
            this.throwAfterIteratorExhausted = throwAfterIteratorExhausted;
        }

        @Override
        public CloseableIterator<Page> scanForAggregates(
            StorageObject object,
            AggregateScanSpec spec,
            BlockFactory blockFactory,
            long byteRangeStart,
            long byteRangeEnd
        ) throws IOException {
            opened++;
            byteRanges.add(new long[] { byteRangeStart, byteRangeEnd });
            if (failFirstOpen && opened == 1) {
                throw new IOException("simulated open failure");
            }
            return new StubIterator(blockFactory, spec.ops().size(), pagesPerFile, throwAfterIteratorExhausted);
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String formatName() {
            return "parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static final class StubIterator implements CloseableIterator<Page> {
        private final BlockFactory blockFactory;
        private final int aggCount;
        private final boolean throwOnExhaustion;
        private int remaining;

        StubIterator(BlockFactory blockFactory, int aggCount, int pages, boolean throwOnExhaustion) {
            this.blockFactory = blockFactory;
            this.aggCount = aggCount;
            this.remaining = pages;
            this.throwOnExhaustion = throwOnExhaustion;
        }

        @Override
        public boolean hasNext() {
            if (remaining > 0) {
                return true;
            }
            if (throwOnExhaustion) {
                throw new RuntimeException("simulated mid-iteration failure");
            }
            return false;
        }

        @Override
        public Page next() {
            remaining--;
            Block[] blocks = new Block[aggCount * 2];
            for (int i = 0; i < aggCount; i++) {
                blocks[i * 2] = blockFactory.newConstantLongBlockWith(100L, 1);
                blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(true, 1);
            }
            return new Page(blocks);
        }

        @Override
        public void close() {}
    }
}
