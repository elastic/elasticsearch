/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class RoundToLongsFromDocValuesBlockLoaderTests extends AbstractLongsFromDocValuesBlockLoaderTests {
    private static final long[] POINTS = new long[] { 0, 1_000_000, 10_000_000, 50_000_000, 90_000_000 };

    public RoundToLongsFromDocValuesBlockLoaderTests(boolean multiValues, boolean missingValues) {
        super(multiValues, missingValues);
    }

    @Override
    protected void innerTest(CircuitBreaker breaker, LeafReaderContext ctx, int mvCount) throws IOException {
        LongsBlockLoader longsLoader = new LongsBlockLoader("field");
        RoundToLongsFromDocValuesBlockLoader roundToLoader = new RoundToLongsFromDocValuesBlockLoader("field", POINTS);
        BlockLoader.Docs docs = TestBlock.docs(ctx);

        try (
            BlockLoader.ColumnAtATimeReader longsReader = longsLoader.reader(breaker, ctx);
            BlockLoader.ColumnAtATimeReader roundToReader = roundToLoader.reader(breaker, ctx)
        ) {
            assertThat(roundToReader, readerMatcher());
            try (TestBlock longs = read(longsReader, docs); TestBlock rounded = read(roundToReader, docs)) {
                checkBlocks(longs, rounded);
            }
        }

        try (
            BlockLoader.ColumnAtATimeReader longsReader = longsLoader.reader(breaker, ctx);
            BlockLoader.ColumnAtATimeReader roundToReader = roundToLoader.reader(breaker, ctx)
        ) {
            for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
                int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
                for (int d = 0; d < docsArray.length; d++) {
                    docsArray[d] = i + d;
                }
                docs = TestBlock.docs(docsArray);
                try (TestBlock longs = read(longsReader, docs); TestBlock rounded = read(roundToReader, docs)) {
                    checkBlocks(longs, rounded);
                }
            }
        }
    }

    /**
     * All docs have the same value so every block should be constant.
     */
    public void testSkipperConstantBlock() throws IOException {
        long[] points = new long[] { 0, 100, 200, 300 };
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(5));
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = 500;
            for (int i = 0; i < docCount; i++) {
                iw.addDocument(List.of(SortedNumericDocValuesField.indexedField("field", 150)));
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                assertNotNull(ctx.reader().getDocValuesSkipper("field"));
                RoundToLongsFromDocValuesBlockLoader loader = new RoundToLongsFromDocValuesBlockLoader("field", points);
                try (BlockLoader.ColumnAtATimeReader reader = loader.reader(breaker, ctx)) {
                    BlockLoader.Docs docs = TestBlock.docs(ctx);
                    try (TestBlock block = (TestBlock) reader.read(TestBlock.factory(), docs, 0, false)) {
                        assertTrue("expected constant block from skipper optimization", block.isConstant());
                        assertThat(block.size(), equalTo(docCount));
                        for (int i = 0; i < docCount; i++) {
                            assertThat(block.get(i), equalTo(100L));
                        }
                    }
                }
            }
        }
    }

    /**
     * Values span two rounding buckets so the skipper can't shortcircuit.
     */
    public void testSkipperFallsBackWhenValuesSpanBuckets() throws IOException {
        long[] points = new long[] { 0, 100, 200, 300 };
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(5));
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = 200;
            for (int i = 0; i < docCount; i++) {
                long value = i < 100 ? 150 : 250;
                iw.addDocument(List.of(SortedNumericDocValuesField.indexedField("field", value)));
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                assertNotNull(ctx.reader().getDocValuesSkipper("field"));

                // Derive expected rounded values from actual index state since
                // RandomIndexWriter may reorder segments during merge.
                SortedNumericDocValues dv = ctx.reader().getSortedNumericDocValues("field");
                long[] expectedRounded = new long[docCount];
                int bucket100Count = 0;
                int bucket200Count = 0;
                for (int i = 0; i < docCount; i++) {
                    assertTrue("doc " + i + " should have a value", dv.advanceExact(i));
                    expectedRounded[i] = RoundToLongsFromDocValuesBlockLoader.roundTo(dv.nextValue(), points);
                    if (expectedRounded[i] == 100L) bucket100Count++;
                    else bucket200Count++;
                }
                assertThat("docs rounding to 100", bucket100Count, equalTo(100));
                assertThat("docs rounding to 200", bucket200Count, equalTo(100));

                RoundToLongsFromDocValuesBlockLoader loader = new RoundToLongsFromDocValuesBlockLoader("field", points);
                try (BlockLoader.ColumnAtATimeReader reader = loader.reader(breaker, ctx)) {
                    BlockLoader.Docs docs = TestBlock.docs(ctx);
                    try (TestBlock block = (TestBlock) reader.read(TestBlock.factory(), docs, 0, false)) {
                        assertFalse("expected non-constant block when values span buckets", block.isConstant());
                        assertThat(block.size(), equalTo(docCount));
                        for (int i = 0; i < docCount; i++) {
                            assertThat("doc " + i, block.get(i), equalTo(expectedRounded[i]));
                        }
                    }
                }
            }
        }
    }

    /**
     * Some docs have no value. The skipper should detect gaps and fall back to doc-by-doc.
     */
    public void testSkipperFallsBackWithMissingValues() throws IOException {
        long[] points = new long[] { 0, 100, 200, 300 };
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(5));
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = 200;
            for (int i = 0; i < docCount; i++) {
                if (i % 10 == 0) {
                    iw.addDocument(List.of());
                } else {
                    iw.addDocument(List.of(SortedNumericDocValuesField.indexedField("field", 150)));
                }
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                assertNotNull(ctx.reader().getDocValuesSkipper("field"));

                // Derive which docs have values from the actual index state since
                // RandomIndexWriter may reorder segments during merge.
                SortedNumericDocValues dv = ctx.reader().getSortedNumericDocValues("field");
                boolean[] docHasValue = new boolean[docCount];
                int missingCount = 0;
                for (int i = 0; i < docCount; i++) {
                    docHasValue[i] = dv != null && dv.advanceExact(i);
                    if (docHasValue[i] == false) missingCount++;
                }
                assertThat("number of missing docs", missingCount, equalTo(20));

                RoundToLongsFromDocValuesBlockLoader loader = new RoundToLongsFromDocValuesBlockLoader("field", points);
                try (BlockLoader.ColumnAtATimeReader reader = loader.reader(breaker, ctx)) {
                    BlockLoader.Docs docs = TestBlock.docs(ctx);
                    try (TestBlock block = (TestBlock) reader.read(TestBlock.factory(), docs, 0, false)) {
                        assertFalse("expected non-constant block when values are missing", block.isConstant());
                        assertThat(block.size(), equalTo(docCount));
                        for (int i = 0; i < docCount; i++) {
                            if (docHasValue[i]) {
                                assertThat("doc " + i, block.get(i), equalTo(100L));
                            } else {
                                assertThat("doc " + i, block.get(i), nullValue());
                            }
                        }
                    }
                }
            }
        }
    }

    private Matcher<Object> readerMatcher() {
        if (multiValues) {
            return hasToString("RoundToLongsFromDocValues.Sorted");
        }
        return hasToString("RoundToLongsFromDocValues.Singleton");
    }

    @SuppressWarnings("unchecked")
    private void checkBlocks(TestBlock longs, TestBlock rounded) {
        for (int i = 0; i < longs.size(); i++) {
            Object v = longs.get(i);
            if (v == null) {
                assertThat(rounded.get(i), nullValue());
                continue;
            }
            if (v instanceof List<?> l) {
                List<Long> expected = ((List<Long>) l).stream()
                    .map(val -> RoundToLongsFromDocValuesBlockLoader.roundTo(val, POINTS))
                    .toList();
                assertThat(rounded.get(i), equalTo(expected));
            } else {
                assertThat(rounded.get(i), equalTo(RoundToLongsFromDocValuesBlockLoader.roundTo((Long) v, POINTS)));
            }
        }
    }
}
