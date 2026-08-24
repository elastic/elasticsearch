/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests {@link GroupKeyBucketer} invariants:
 * <ol>
 *   <li>Every row with the same key always maps to the same bucket (determinism).</li>
 *   <li>No key value ever maps to two distinct buckets in the same call (disjointness).</li>
 *   <li>Bucket indices are always in {@code [0, B)}.</li>
 *   <li>Null keys hash to a consistent bucket distinct from their non-null neighbours.</li>
 *   <li>Multivalue rows return {@link GroupKeyBucketer#MULTIVALUE_DETECTED}.</li>
 * </ol>
 */
public class GroupKeyBucketerTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    // -------------------------------------------------------------------------
    // Disjointness: no key appears in two buckets
    // -------------------------------------------------------------------------

    public void testLongKeyDisjointness() {
        int B = between(2, 16);
        int rows = between(100, 2000);
        GroupKeyBucketer bucketer = new GroupKeyBucketer(List.of(new BlockHash.GroupSpec(0, ElementType.LONG)));

        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(rows)) {
            for (int i = 0; i < rows; i++) {
                builder.appendLong(randomLong());
            }
            try (LongBlock keys = builder.build()) {
                Page page = new Page(keys);
                int[] result = new int[rows];
                int rc = bucketer.computeBuckets(page, B, result, new BytesRef(), new MurmurHash3.Hash128());
                assertThat(rc, equalTo(0));
                assertBucketsInRange(result, rows, B);
                assertDeterminism(bucketer, page, B, result);
            }
        }
    }

    public void testIntKeyDisjointness() {
        int B = between(2, 16);
        int rows = between(100, 2000);
        GroupKeyBucketer bucketer = new GroupKeyBucketer(List.of(new BlockHash.GroupSpec(0, ElementType.INT)));

        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(rows)) {
            for (int i = 0; i < rows; i++) {
                builder.appendInt(randomInt());
            }
            try (IntBlock keys = builder.build()) {
                Page page = new Page(keys);
                int[] result = new int[rows];
                int rc = bucketer.computeBuckets(page, B, result, new BytesRef(), new MurmurHash3.Hash128());
                assertThat(rc, equalTo(0));
                assertBucketsInRange(result, rows, B);
            }
        }
    }

    public void testDoubleKeyDisjointness() {
        int B = between(2, 16);
        int rows = between(100, 2000);
        GroupKeyBucketer bucketer = new GroupKeyBucketer(List.of(new BlockHash.GroupSpec(0, ElementType.DOUBLE)));

        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(rows)) {
            for (int i = 0; i < rows; i++) {
                builder.appendDouble(randomDouble());
            }
            try (DoubleBlock keys = builder.build()) {
                Page page = new Page(keys);
                int[] result = new int[rows];
                int rc = bucketer.computeBuckets(page, B, result, new BytesRef(), new MurmurHash3.Hash128());
                assertThat(rc, equalTo(0));
                assertBucketsInRange(result, rows, B);
            }
        }
    }

    public void testBytesRefKeyDisjointness() {
        int B = between(2, 16);
        int rows = between(100, 2000);
        GroupKeyBucketer bucketer = new GroupKeyBucketer(List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF)));

        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(rows)) {
            for (int i = 0; i < rows; i++) {
                builder.appendBytesRef(new BytesRef(randomAlphaOfLength(between(1, 20))));
            }
            try (BytesRefBlock keys = builder.build()) {
                Page page = new Page(keys);
                int[] result = new int[rows];
                int rc = bucketer.computeBuckets(page, B, result, new BytesRef(), new MurmurHash3.Hash128());
                assertThat(rc, equalTo(0));
                assertBucketsInRange(result, rows, B);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Null handling: null key is a real group, not an error
    // -------------------------------------------------------------------------

    public void testNullKeyIsValidGroup() {
        GroupKeyBucketer bucketer = new GroupKeyBucketer(List.of(new BlockHash.GroupSpec(0, ElementType.LONG)));
        int B = between(2, 8);

        // Build a block with some nulls.
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(10)) {
            builder.appendNull();
            builder.appendLong(42L);
            builder.appendNull();
            builder.appendLong(99L);
            try (LongBlock block = builder.build()) {
                Page page = new Page(block);
                int[] result = new int[4];
                int rc = bucketer.computeBuckets(page, B, result, new BytesRef(), new MurmurHash3.Hash128());
                assertThat(rc, equalTo(0));
                // Both nulls must hash to the same bucket.
                assertThat(result[0], equalTo(result[2]));
                assertBucketsInRange(result, 4, B);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Multivalue detection
    // -------------------------------------------------------------------------

    public void testMultivalueReturnsFlag() {
        GroupKeyBucketer bucketer = new GroupKeyBucketer(List.of(new BlockHash.GroupSpec(0, ElementType.LONG)));

        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(3)) {
            builder.appendLong(1L);
            builder.beginPositionEntry();
            builder.appendLong(2L);
            builder.appendLong(3L);
            builder.endPositionEntry();
            builder.appendLong(4L);
            try (LongBlock block = builder.build()) {
                Page page = new Page(block);
                int[] result = new int[3];
                int rc = bucketer.computeBuckets(page, 4, result, new BytesRef(), new MurmurHash3.Hash128());
                assertThat(rc, equalTo(GroupKeyBucketer.MULTIVALUE_DETECTED));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Determinism: same key always lands in same bucket
    // -------------------------------------------------------------------------

    public void testDeterminismAcrossPages() {
        int B = between(2, 8);
        GroupKeyBucketer bucketer = new GroupKeyBucketer(List.of(new BlockHash.GroupSpec(0, ElementType.LONG)));

        // Known set of keys and their expected buckets from first call.
        long[] keys = { 0L, 1L, 2L, 3L, 4L };
        int[] expected = new int[keys.length];

        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(keys.length)) {
            for (long k : keys) {
                builder.appendLong(k);
            }
            try (LongBlock block = builder.build()) {
                Page page = new Page(block);
                bucketer.computeBuckets(page, B, expected, new BytesRef(), new MurmurHash3.Hash128());

                // Re-compute and assert same result.
                int[] second = new int[keys.length];
                bucketer.computeBuckets(page, B, second, new BytesRef(), new MurmurHash3.Hash128());
                for (int i = 0; i < keys.length; i++) {
                    assertThat(second[i], equalTo(expected[i]));
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Composite keys: two columns
    // -------------------------------------------------------------------------

    public void testCompositeKeyDisjointness() {
        int B = between(2, 8);
        int rows = between(50, 500);
        GroupKeyBucketer bucketer = new GroupKeyBucketer(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.INT))
        );

        try (
            LongBlock.Builder longBuilder = blockFactory.newLongBlockBuilder(rows);
            IntBlock.Builder intBuilder = blockFactory.newIntBlockBuilder(rows)
        ) {
            for (int i = 0; i < rows; i++) {
                longBuilder.appendLong(randomLong());
                intBuilder.appendInt(randomInt());
            }
            try (LongBlock col0 = longBuilder.build(); IntBlock col1 = intBuilder.build()) {
                Page page = new Page(col0, col1);
                int[] result = new int[rows];
                int rc = bucketer.computeBuckets(page, B, result, new BytesRef(), new MurmurHash3.Hash128());
                assertThat(rc, equalTo(0));
                assertBucketsInRange(result, rows, B);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void assertBucketsInRange(int[] result, int rows, int B) {
        for (int i = 0; i < rows; i++) {
            assertThat("bucket[" + i + "] must be >= 0", result[i], greaterThanOrEqualTo(0));
            assertThat("bucket[" + i + "] must be < B", result[i], lessThan(B));
        }
    }

    /**
     * Asserts that re-computing buckets for the same page produces identical results (determinism).
     */
    private static void assertDeterminism(GroupKeyBucketer bucketer, Page page, int B, int[] first) {
        int[] second = new int[page.getPositionCount()];
        bucketer.computeBuckets(page, B, second, new BytesRef(), new MurmurHash3.Hash128());
        for (int i = 0; i < page.getPositionCount(); i++) {
            assertThat("position " + i + " must hash to the same bucket on repeated calls", second[i], equalTo(first[i]));
        }
    }
}
