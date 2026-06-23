/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SharedMinCompetitiveTests extends ComputeTestCase {
    public void testEmpty() {
        try (SharedMinCompetitive minCompetitive = longMinCompetitive()) {
            assertThat(minCompetitive.get(blockFactory()), nullValue());
        }
    }

    public void testOneOffer() {
        long v = randomLong();
        try (SharedMinCompetitive minCompetitive = longMinCompetitive()) {
            offerLong(blockFactory(), minCompetitive, v);
            try (Page p = minCompetitive.get(blockFactory())) {
                assertThat(p.getPositionCount(), equalTo(1));
                assertThat(p.getBlockCount(), equalTo(1));
                LongVector vector = p.<LongBlock>getBlock(0).asVector();
                assertThat(vector.getLong(0), equalTo(v));
            }
        }
    }

    public void testManyOffers() {
        int count = 10_000;
        long[] values = randomLongs().limit(count).toArray();
        try (SharedMinCompetitive minCompetitive = longMinCompetitive()) {
            BlockFactory blockFactory = blockFactory();
            for (long v : values) {
                offerLong(blockFactory, minCompetitive, v);
            }
            try (Page p = minCompetitive.get(blockFactory())) {
                assertThat(p.getPositionCount(), equalTo(1));
                assertThat(p.getBlockCount(), equalTo(1));
                LongVector vector = p.<LongBlock>getBlock(0).asVector();
                assertThat(vector.getLong(0), equalTo(Arrays.stream(values).max().getAsLong()));
            }
        }
    }

    public void testManyConcurrentOffers() throws ExecutionException, InterruptedException, TimeoutException {
        int count = 10_000;
        int threads = 5;
        long max = Long.MIN_VALUE;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        List<Future<?>> wait = new ArrayList<>();
        try {
            try (SharedMinCompetitive minCompetitive = longMinCompetitive()) {
                for (int t = 0; t < threads; t++) {
                    BlockFactory blockFactory = blockFactory();
                    long[] values = randomLongs().limit(count).toArray();
                    max = Math.max(max, Arrays.stream(values).max().getAsLong());
                    wait.add(exec.submit(() -> {
                        for (long v : values) {
                            offerLong(blockFactory, minCompetitive, v);
                        }
                    }));
                }
                for (Future<?> w : wait) {
                    w.get(1, TimeUnit.MINUTES);
                }
                try (Page p = minCompetitive.get(blockFactory())) {
                    assertThat(p.getPositionCount(), equalTo(1));
                    assertThat(p.getBlockCount(), equalTo(1));
                    LongVector vector = p.<LongBlock>getBlock(0).asVector();
                    assertThat(vector.getLong(0), equalTo(max));
                }
            }
        } finally {
            exec.shutdown();
        }
    }

    public void testBytesRefEmptyHasNoBound() {
        try (SharedMinCompetitive minCompetitive = bytesRefMinCompetitive(randomBoolean(), randomBoolean())) {
            assertThat(minCompetitive.minCompetitiveValue(), nullValue());
        }
    }

    public void testBytesRefRoundTrip() {
        for (boolean asc : new boolean[] { true, false }) {
            for (boolean nullsFirst : new boolean[] { true, false }) {
                try (SharedMinCompetitive minCompetitive = bytesRefMinCompetitive(asc, nullsFirst)) {
                    offerBytesRef(blockFactory(), minCompetitive, asc, nullsFirst, "hello");
                    assertThat(minCompetitive.minCompetitiveValue(), equalTo(new BytesRef("hello")));
                }
            }
        }
    }

    /**
     * For ascending sort the encoded byte order matches the string order, so the channel keeps the
     * smallest offered key, which is exactly the published competitive bound a reader compares against.
     */
    public void testBytesRefAscendingKeepsSmallest() {
        boolean nullsFirst = randomBoolean();
        try (SharedMinCompetitive minCompetitive = bytesRefMinCompetitive(true, nullsFirst)) {
            BlockFactory blockFactory = blockFactory();
            offerBytesRef(blockFactory, minCompetitive, true, nullsFirst, "delta");
            offerBytesRef(blockFactory, minCompetitive, true, nullsFirst, "alpha");
            offerBytesRef(blockFactory, minCompetitive, true, nullsFirst, "charlie");
            assertThat(minCompetitive.minCompetitiveValue(), equalTo(new BytesRef("alpha")));
        }
    }

    /**
     * For descending sort the encoder inverts the bytes, so the byte minimum corresponds to the
     * largest string. The decoded bound is still the original (largest) value.
     */
    public void testBytesRefDescendingKeepsLargest() {
        boolean nullsFirst = randomBoolean();
        try (SharedMinCompetitive minCompetitive = bytesRefMinCompetitive(false, nullsFirst)) {
            BlockFactory blockFactory = blockFactory();
            offerBytesRef(blockFactory, minCompetitive, false, nullsFirst, "delta");
            offerBytesRef(blockFactory, minCompetitive, false, nullsFirst, "alpha");
            offerBytesRef(blockFactory, minCompetitive, false, nullsFirst, "charlie");
            assertThat(minCompetitive.minCompetitiveValue(), equalTo(new BytesRef("delta")));
        }
    }

    /**
     * A null most-competitive key yields no usable byte bound: every non-null value may still be
     * competitive, so the reader must not skip any range.
     */
    public void testBytesRefNullKeyHasNoBound() {
        boolean asc = randomBoolean();
        boolean nullsFirst = randomBoolean();
        try (SharedMinCompetitive minCompetitive = bytesRefMinCompetitive(asc, nullsFirst)) {
            offerNull(blockFactory(), minCompetitive, asc, nullsFirst);
            assertThat(minCompetitive.minCompetitiveValue(), nullValue());
        }
    }

    /**
     * Locks the manual bound encoding used by the Parquet/ORC reader tests (which cannot reach the
     * package-private {@link KeyExtractor}) against the real extractor: a single non-null marker byte
     * ({@code nullsFirst ? BIG_NULL : SMALL_NULL}) followed by the directional UTF-8 key must be
     * byte-for-byte identical to what {@link KeyExtractor#writeKey} produces.
     */
    public void testManualBoundEncodingMatchesKeyExtractor() {
        for (boolean asc : new boolean[] { true, false }) {
            for (boolean nullsFirst : new boolean[] { true, false }) {
                BlockFactory blockFactory = blockFactory();
                String value = "competitive";
                BytesRef viaExtractor;
                try (
                    BytesRefBlock block = blockFactory.newConstantBytesRefBlockWith(new BytesRef(value), 1);
                    BreakingBytesRefBuilder b = new BreakingBytesRefBuilder(blockFactory.breaker(), "extractor")
                ) {
                    TopNOperator.SortOrder so = new TopNOperator.SortOrder(0, asc, nullsFirst);
                    KeyExtractor extractor = KeyExtractor.extractorFor(
                        ElementType.BYTES_REF,
                        TopNEncoder.UTF8,
                        asc,
                        so.nul(),
                        so.nonNul(),
                        block
                    );
                    extractor.writeKey(b, 0);
                    viaExtractor = BytesRef.deepCopyOf(b.bytesRefView());
                }
                BytesRef viaManual;
                try (BreakingBytesRefBuilder b = new BreakingBytesRefBuilder(blockFactory.breaker(), "manual")) {
                    // The exact formula the datasource reader tests use to build a competitive bound.
                    b.append(nullsFirst ? (byte) 0x02 : (byte) 0x01);
                    TopNEncoder encoder = TopNEncoder.UTF8;
                    encoder.toSortable(asc).encodeBytesRef(new BytesRef(value), b);
                    viaManual = BytesRef.deepCopyOf(b.bytesRefView());
                }
                assertThat("asc=" + asc + " nullsFirst=" + nullsFirst, viaManual, equalTo(viaExtractor));
            }
        }
    }

    public void testNoFurtherCandidatesDefaultsFalse() {
        try (SharedMinCompetitive minCompetitive = bytesRefMinCompetitive(randomBoolean(), randomBoolean())) {
            assertFalse(minCompetitive.noFurtherCandidates());
        }
    }

    public void testMarkNoFurtherCandidatesIsSticky() {
        boolean asc = randomBoolean();
        boolean nullsFirst = randomBoolean();
        try (SharedMinCompetitive minCompetitive = bytesRefMinCompetitive(asc, nullsFirst)) {
            assertFalse(minCompetitive.noFurtherCandidates());
            minCompetitive.markNoFurtherCandidates();
            assertTrue(minCompetitive.noFurtherCandidates());
            // A later offer does not clear the terminal signal.
            offerBytesRef(blockFactory(), minCompetitive, asc, nullsFirst, "later");
            assertTrue(minCompetitive.noFurtherCandidates());
        }
    }

    /**
     * The decoded bound is cached per generation, but every call must hand back an independent view so
     * concurrent readers never share a mutable {@link BytesRef}; a new offer must invalidate the cache.
     */
    public void testMinCompetitiveValueCachesYetReturnsIndependentViews() {
        boolean asc = randomBoolean();
        try (SharedMinCompetitive minCompetitive = bytesRefMinCompetitive(asc, false)) {
            BlockFactory blockFactory = blockFactory();
            offerBytesRef(blockFactory, minCompetitive, asc, false, "mmm");
            BytesRef first = minCompetitive.minCompetitiveValue();
            BytesRef second = minCompetitive.minCompetitiveValue();
            assertThat(first, equalTo(new BytesRef("mmm")));
            assertThat(second, equalTo(first));
            assertNotSame(first, second);
            // A tighter offer (for ASC, a smaller value) must be reflected, proving cache invalidation.
            offerBytesRef(blockFactory, minCompetitive, asc, false, asc ? "aaa" : "zzz");
            assertThat(minCompetitiveValueString(minCompetitive), equalTo(asc ? "aaa" : "zzz"));
        }
    }

    private static String minCompetitiveValueString(SharedMinCompetitive minCompetitive) {
        BytesRef value = minCompetitive.minCompetitiveValue();
        return value == null ? null : value.utf8ToString();
    }

    private SharedMinCompetitive bytesRefMinCompetitive(boolean asc, boolean nullsFirst) {
        SharedMinCompetitive.KeyConfig keyConfig = new SharedMinCompetitive.KeyConfig(
            ElementType.BYTES_REF,
            TopNEncoder.UTF8,
            asc,
            nullsFirst
        );
        return new SharedMinCompetitive.Supplier(blockFactory().breaker(), List.of(keyConfig)).get();
    }

    private void offerBytesRef(
        BlockFactory blockFactory,
        SharedMinCompetitive minCompetitive,
        boolean asc,
        boolean nullsFirst,
        String value
    ) {
        try (
            BytesRefBlock block = blockFactory.newConstantBytesRefBlockWith(new BytesRef(value), 1);
            BreakingBytesRefBuilder b = new BreakingBytesRefBuilder(blockFactory.breaker(), "work");
        ) {
            TopNOperator.SortOrder so = new TopNOperator.SortOrder(0, asc, nullsFirst);
            KeyExtractor extractor = KeyExtractor.extractorFor(ElementType.BYTES_REF, TopNEncoder.UTF8, asc, so.nul(), so.nonNul(), block);
            extractor.writeKey(b, 0);
            minCompetitive.offer(b.bytesRefView());
        }
    }

    private void offerNull(BlockFactory blockFactory, SharedMinCompetitive minCompetitive, boolean asc, boolean nullsFirst) {
        try (
            Block block = blockFactory.newConstantNullBlock(1);
            BreakingBytesRefBuilder b = new BreakingBytesRefBuilder(blockFactory.breaker(), "work");
        ) {
            TopNOperator.SortOrder so = new TopNOperator.SortOrder(0, asc, nullsFirst);
            KeyExtractor extractor = KeyExtractor.extractorFor(ElementType.BYTES_REF, TopNEncoder.UTF8, asc, so.nul(), so.nonNul(), block);
            extractor.writeKey(b, 0);
            minCompetitive.offer(b.bytesRefView());
        }
    }

    private SharedMinCompetitive longMinCompetitive() {
        TopNOperator.SortOrder so = longSortOrder();
        SharedMinCompetitive.KeyConfig keyConfig = new SharedMinCompetitive.KeyConfig(
            ElementType.LONG,
            TopNEncoder.DEFAULT_UNSORTABLE,
            so.asc(),
            so.nullsFirst()
        );
        return new SharedMinCompetitive.Supplier(blockFactory().breaker(), List.of(keyConfig)).get();
    }

    private void offerLong(BlockFactory blockFactory, SharedMinCompetitive minCompetitive, long l) {
        try (
            Block block = blockFactory.newConstantLongBlockWith(l, 1);
            BreakingBytesRefBuilder b = new BreakingBytesRefBuilder(blockFactory.breaker(), "work");
        ) {
            KeyExtractor extractor = longExtractor(block);
            extractor.writeKey(b, 0);
            minCompetitive.offer(b.bytesRefView());
        }
    }

    private KeyExtractor longExtractor(Block block) {
        TopNOperator.SortOrder so = longSortOrder();
        return KeyExtractor.extractorFor(ElementType.LONG, TopNEncoder.DEFAULT_SORTABLE, so.asc(), so.nul(), so.nonNul(), block);
    }

    private TopNOperator.SortOrder longSortOrder() {
        return new TopNOperator.SortOrder(0, false, false);
    }
}
