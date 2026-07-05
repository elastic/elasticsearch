/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.swisshash.LongSwissHash;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;

public class BlockHashTests extends BlockHashTestCase {

    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[] { false });
        params.add(new Object[] { true });
        return params;
    }

    private final boolean forcePackedHash;

    public BlockHashTests(@Name("forcePackedHash") boolean forcePackedHash) {
        this.forcePackedHash = forcePackedHash;
    }

    public void testIntHash() {
        int[] values = new int[] { 1, 2, 3, 1, 2, 3, 1, 2, 3 };
        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:INT], entries=3, size="));
                assertOrds(ordsAndKeys.ords(), 0, 1, 2, 0, 1, 2, 0, 1, 2);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 3)));
            } else {
                assertThat(ordsAndKeys.description(), equalTo("IntBlockHash{channel=0, entries=3, seenNull=false}"));
                assertOrds(ordsAndKeys.ords(), 1, 2, 3, 1, 2, 3, 1, 2, 3);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(1, 4)));
            }
            assertKeys(ordsAndKeys.keys(), 1, 2, 3);
        }, blockFactory.newIntArrayVector(values, values.length).asBlock());
    }

    public void testIntHashWithNulls() {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(4)) {
            builder.appendInt(0);
            builder.appendNull();
            builder.appendInt(2);
            builder.appendNull();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:INT], entries=3, size="));
                    assertOrds(ordsAndKeys.ords(), 0, 1, 2, 1);
                    assertKeys(ordsAndKeys.keys(), 0, null, 2);
                } else {
                    assertThat(ordsAndKeys.description(), equalTo("IntBlockHash{channel=0, entries=2, seenNull=true}"));
                    assertOrds(ordsAndKeys.ords(), 1, 0, 2, 0);
                    assertKeys(ordsAndKeys.keys(), null, 0, 2);
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 3)));
            }, builder);
        }
    }

    public void testIntHashWithMultiValuedFields() {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(8)) {
            builder.appendInt(1);
            builder.beginPositionEntry();
            builder.appendInt(1);
            builder.appendInt(2);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendInt(3);
            builder.appendInt(1);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendInt(3);
            builder.appendInt(3);
            builder.endPositionEntry();
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendInt(3);
            builder.appendInt(2);
            builder.appendInt(1);
            builder.endPositionEntry();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:INT], entries=4, size="));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 0 },
                        new int[] { 0, 1 },
                        new int[] { 2, 0 },
                        new int[] { 2 },
                        new int[] { 3 },
                        new int[] { 2, 1, 0 }
                    );
                    assertKeys(ordsAndKeys.keys(), 1, 2, 3, null);
                } else {
                    assertThat(ordsAndKeys.description(), equalTo("IntBlockHash{channel=0, entries=3, seenNull=true}"));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 1 },
                        new int[] { 1, 2 },
                        new int[] { 3, 1 },
                        new int[] { 3 },
                        new int[] { 0 },
                        new int[] { 3, 2, 1 }
                    );
                    assertKeys(ordsAndKeys.keys(), null, 1, 2, 3);
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
            }, builder);
        }
    }

    public void testLongHash() {
        long[] values = new long[] { 2, 1, 4, 2, 4, 1, 3, 4 };

        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:LONG], entries=4, size="));
                assertOrds(ordsAndKeys.ords(), 0, 1, 2, 0, 2, 1, 3, 2);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
            } else {
                assertThat(ordsAndKeys.description(), equalTo("LongBlockHash{channel=0, entries=4, seenNull=false}"));
                assertOrds(ordsAndKeys.ords(), 1, 2, 3, 1, 3, 2, 4, 3);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(1, 5)));
            }
            assertKeys(ordsAndKeys.keys(), 2L, 1L, 4L, 3L);
        }, blockFactory.newLongArrayVector(values, values.length).asBlock());
    }

    /**
     * Builds a {@link LongBlockHash} big enough to form a big core, then replays the keys across
     * several pages so the batched prefetch path ({@code addWithPrefetch}) is exercised and asserted
     * to produce the same ords as the scalar path. {@code shouldPrefetch} is checked once per page,
     * so a single page never reaches the prefetch path: it needs the table primed by an earlier page.
     */
    public void testLongHashHighCardinalityPrefetch() {
        assumeFalse("the packed hash has no long prefetch path", forcePackedHash);
        LongSwissHash.PREFETCH_THRESHOLD = between(1, 1024);
        int distinct = between(2_000, 8_000); // exceeds the small-core capacity so a big core (and the prefetch path) forms
        int pages = between(3, 6);
        try (BlockHash hash = new LongBlockHash(0, blockFactory)) {
            for (int page = 0; page < pages; page++) {
                long[] values = new long[distinct];
                for (int i = 0; i < distinct; i++) {
                    values[i] = i;
                }
                try (LongBlock block = blockFactory.newLongArrayVector(values, distinct).asBlock()) {
                    // value i is first seen at position i on page 0, so it always resolves to ord i + 1
                    hash(true, hash, ordsAndKeys -> {
                        IntBlock ords = ordsAndKeys.ords();
                        for (int p = 0; p < distinct; p++) {
                            assertThat(ords.getInt(p), equalTo(p + 1));
                        }
                    }, block);
                }
            }
            try (IntVector nonEmpty = hash.nonEmpty()) {
                assertThat(nonEmpty.getPositionCount(), equalTo(distinct));
            }
        }
    }

    public void testLongHashWithNulls() {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(4)) {
            builder.appendLong(0);
            builder.appendNull();
            builder.appendLong(2);
            builder.appendNull();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:LONG], entries=3, size="));
                    assertOrds(ordsAndKeys.ords(), 0, 1, 2, 1);
                    assertKeys(ordsAndKeys.keys(), 0L, null, 2L);
                } else {
                    assertThat(ordsAndKeys.description(), equalTo("LongBlockHash{channel=0, entries=2, seenNull=true}"));
                    assertOrds(ordsAndKeys.ords(), 1, 0, 2, 0);
                    assertKeys(ordsAndKeys.keys(), null, 0L, 2L);
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 3)));
            }, builder);
        }
    }

    public void testLongHashWithMultiValuedFields() {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(8)) {
            builder.appendLong(1);
            builder.beginPositionEntry();
            builder.appendLong(1);
            builder.appendLong(2);
            builder.appendLong(3);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendLong(1);
            builder.appendLong(1);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendLong(3);
            builder.endPositionEntry();
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendLong(3);
            builder.appendLong(2);
            builder.appendLong(1);
            builder.endPositionEntry();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:LONG], entries=4, size="));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 0 },
                        new int[] { 0, 1, 2 },
                        new int[] { 0 },
                        new int[] { 2 },
                        new int[] { 3 },
                        new int[] { 2, 1, 0 }
                    );
                    assertKeys(ordsAndKeys.keys(), 1L, 2L, 3L, null);
                } else {
                    assertThat(ordsAndKeys.description(), equalTo("LongBlockHash{channel=0, entries=3, seenNull=true}"));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 1 },
                        new int[] { 1, 2, 3 },
                        new int[] { 1 },
                        new int[] { 3 },
                        new int[] { 0 },
                        new int[] { 3, 2, 1 }
                    );
                    assertKeys(ordsAndKeys.keys(), null, 1L, 2L, 3L);
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
            }, builder);
        }
    }

    public void testDoubleHash() {
        double[] values = new double[] { 2.0, 1.0, 4.0, 2.0, 4.0, 1.0, 3.0, 4.0 };
        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:DOUBLE], entries=4, size="));
                assertOrds(ordsAndKeys.ords(), 0, 1, 2, 0, 2, 1, 3, 2);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
            } else {
                assertThat(ordsAndKeys.description(), equalTo("DoubleBlockHash{channel=0, entries=4, seenNull=false}"));
                assertOrds(ordsAndKeys.ords(), 1, 2, 3, 1, 3, 2, 4, 3);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(1, 5)));
            }
            assertKeys(ordsAndKeys.keys(), 2.0, 1.0, 4.0, 3.0);
        }, blockFactory.newDoubleArrayVector(values, values.length).asBlock());
    }

    public void testDoubleHashWithNulls() {
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(4)) {
            builder.appendDouble(0);
            builder.appendNull();
            builder.appendDouble(2);
            builder.appendNull();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:DOUBLE], entries=3, size="));
                    assertOrds(ordsAndKeys.ords(), 0, 1, 2, 1);
                    assertKeys(ordsAndKeys.keys(), 0.0, null, 2.0);
                } else {
                    assertThat(ordsAndKeys.description(), equalTo("DoubleBlockHash{channel=0, entries=2, seenNull=true}"));
                    assertOrds(ordsAndKeys.ords(), 1, 0, 2, 0);
                    assertKeys(ordsAndKeys.keys(), null, 0.0, 2.0);
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 3)));
            }, builder);
        }
    }

    public void testDoubleHashWithMultiValuedFields() {
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(8)) {
            builder.appendDouble(1);
            builder.beginPositionEntry();
            builder.appendDouble(2);
            builder.appendDouble(3);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendDouble(3);
            builder.appendDouble(2);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendDouble(1);
            builder.endPositionEntry();
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendDouble(1);
            builder.appendDouble(1);
            builder.appendDouble(2);
            builder.endPositionEntry();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:DOUBLE], entries=4, size="));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 0 },
                        new int[] { 1, 2 },
                        new int[] { 2, 1 },
                        new int[] { 0 },
                        new int[] { 3 },
                        new int[] { 0, 1 }
                    );
                    assertKeys(ordsAndKeys.keys(), 1.0, 2.0, 3.0, null);
                } else {
                    assertThat(ordsAndKeys.description(), equalTo("DoubleBlockHash{channel=0, entries=3, seenNull=true}"));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 1 },
                        new int[] { 2, 3 },
                        new int[] { 3, 2 },
                        new int[] { 1 },
                        new int[] { 0 },
                        new int[] { 1, 2 }
                    );
                    assertKeys(ordsAndKeys.keys(), null, 1.0, 2.0, 3.0);
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
            }, builder);
        }
    }

    public void testBasicBytesRefHash() {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(8)) {
            builder.appendBytesRef(new BytesRef("item-2"));
            builder.appendBytesRef(new BytesRef("item-1"));
            builder.appendBytesRef(new BytesRef("item-4"));
            builder.appendBytesRef(new BytesRef("item-2"));
            builder.appendBytesRef(new BytesRef("item-4"));
            builder.appendBytesRef(new BytesRef("item-1"));
            builder.appendBytesRef(new BytesRef("item-3"));
            builder.appendBytesRef(new BytesRef("item-4"));

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BYTES_REF], entries=4, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b}"));
                    assertOrds(ordsAndKeys.ords(), 0, 1, 2, 0, 2, 1, 3, 2);
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
                } else {
                    assertThat(ordsAndKeys.description(), startsWith("BytesRefBlockHash{channel=0, entries=4, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b, seenNull=false}"));
                    assertOrds(ordsAndKeys.ords(), 1, 2, 3, 1, 3, 2, 4, 3);
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(1, 5)));
                }
                assertKeys(ordsAndKeys.keys(), "item-2", "item-1", "item-4", "item-3");
            }, builder);
        }
    }

    public void testBytesRefHashWithNulls() {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(4)) {
            builder.appendBytesRef(new BytesRef("cat"));
            builder.appendNull();
            builder.appendBytesRef(new BytesRef("dog"));
            builder.appendNull();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BYTES_REF], entries=3, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b}"));
                    assertOrds(ordsAndKeys.ords(), 0, 1, 2, 1);
                    assertKeys(ordsAndKeys.keys(), "cat", null, "dog");
                } else {
                    assertThat(ordsAndKeys.description(), startsWith("BytesRefBlockHash{channel=0, entries=2, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b, seenNull=true}"));
                    assertOrds(ordsAndKeys.ords(), 1, 0, 2, 0);
                    assertKeys(ordsAndKeys.keys(), null, "cat", "dog");
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 3)));
            }, builder);
        }
    }

    public void testBytesRefHashWithMultiValuedFields() {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(8)) {
            builder.appendBytesRef(new BytesRef("foo"));
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("foo"));
            builder.appendBytesRef(new BytesRef("bar"));
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("bar"));
            builder.appendBytesRef(new BytesRef("bort"));
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("bort"));
            builder.appendBytesRef(new BytesRef("bar"));
            builder.endPositionEntry();
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("bort"));
            builder.appendBytesRef(new BytesRef("bort"));
            builder.appendBytesRef(new BytesRef("bar"));
            builder.endPositionEntry();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BYTES_REF], entries=4, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b}"));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 0 },
                        new int[] { 0, 1 },
                        new int[] { 1, 2 },
                        new int[] { 2, 1 },
                        new int[] { 3 },
                        new int[] { 2, 1 }
                    );
                    assertKeys(ordsAndKeys.keys(), "foo", "bar", "bort", null);
                } else {
                    assertThat(ordsAndKeys.description(), startsWith("BytesRefBlockHash{channel=0, entries=3, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b, seenNull=true}"));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 1 },
                        new int[] { 1, 2 },
                        new int[] { 2, 3 },
                        new int[] { 3, 2 },
                        new int[] { 0 },
                        new int[] { 3, 2 }
                    );
                    assertKeys(ordsAndKeys.keys(), null, "foo", "bar", "bort");
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
            }, builder);
        }
    }

    public void testBasicOrdinals() {
        try (
            IntVector.Builder ords = blockFactory.newIntVectorFixedBuilder(8);
            BytesRefVector.Builder bytes = blockFactory.newBytesRefVectorBuilder(8)
        ) {
            ords.appendInt(1);
            ords.appendInt(0);
            ords.appendInt(3);
            ords.appendInt(1);
            ords.appendInt(3);
            ords.appendInt(0);
            ords.appendInt(2);
            ords.appendInt(3);
            bytes.appendBytesRef(new BytesRef("item-1"));
            bytes.appendBytesRef(new BytesRef("item-2"));
            bytes.appendBytesRef(new BytesRef("item-3"));
            bytes.appendBytesRef(new BytesRef("item-4"));

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BYTES_REF], entries=4, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b}"));
                    assertOrds(ordsAndKeys.ords(), 0, 1, 2, 0, 2, 1, 3, 2);
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
                    assertKeys(ordsAndKeys.keys(), "item-2", "item-1", "item-4", "item-3");
                } else {
                    assertThat(ordsAndKeys.description(), startsWith("BytesRefBlockHash{channel=0, entries=4, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b, seenNull=false}"));
                    assertOrds(ordsAndKeys.ords(), 2, 1, 4, 2, 4, 1, 3, 4);
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(1, 5)));
                    assertKeys(ordsAndKeys.keys(), "item-1", "item-2", "item-3", "item-4");
                }
            }, new OrdinalBytesRefVector(ords.build(), bytes.build()).asBlock());
        }
    }

    public void testOrdinalsWithNulls() {
        try (
            IntBlock.Builder ords = blockFactory.newIntBlockBuilder(4);
            BytesRefVector.Builder bytes = blockFactory.newBytesRefVectorBuilder(2)
        ) {
            ords.appendInt(0);
            ords.appendNull();
            ords.appendInt(1);
            ords.appendNull();
            bytes.appendBytesRef(new BytesRef("cat"));
            bytes.appendBytesRef(new BytesRef("dog"));

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BYTES_REF], entries=3, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b}"));
                    assertOrds(ordsAndKeys.ords(), 0, 1, 2, 1);
                    assertKeys(ordsAndKeys.keys(), "cat", null, "dog");
                } else {
                    assertThat(ordsAndKeys.description(), startsWith("BytesRefBlockHash{channel=0, entries=2, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b, seenNull=true}"));
                    assertOrds(ordsAndKeys.ords(), 1, 0, 2, 0);
                    assertKeys(ordsAndKeys.keys(), null, "cat", "dog");
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 3)));
            }, new OrdinalBytesRefBlock(ords.build(), bytes.build()));
        }
    }

    public void testOrdinalsWithMultiValuedFields() {
        try (
            IntBlock.Builder ords = blockFactory.newIntBlockBuilder(4);
            BytesRefVector.Builder bytes = blockFactory.newBytesRefVectorBuilder(2)
        ) {
            ords.appendInt(0);
            ords.beginPositionEntry();
            ords.appendInt(0);
            ords.appendInt(1);
            ords.endPositionEntry();
            ords.beginPositionEntry();
            ords.appendInt(1);
            ords.appendInt(2);
            ords.endPositionEntry();
            ords.beginPositionEntry();
            ords.appendInt(2);
            ords.appendInt(1);
            ords.endPositionEntry();
            ords.appendNull();
            ords.beginPositionEntry();
            ords.appendInt(2);
            ords.appendInt(2);
            ords.appendInt(1);
            ords.endPositionEntry();

            bytes.appendBytesRef(new BytesRef("foo"));
            bytes.appendBytesRef(new BytesRef("bar"));
            bytes.appendBytesRef(new BytesRef("bort"));

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BYTES_REF], entries=4, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b}"));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 0 },
                        new int[] { 0, 1 },
                        new int[] { 1, 2 },
                        new int[] { 2, 1 },
                        new int[] { 3 },
                        new int[] { 2, 1 }
                    );
                    assertKeys(ordsAndKeys.keys(), "foo", "bar", "bort", null);
                } else {
                    assertThat(ordsAndKeys.description(), startsWith("BytesRefBlockHash{channel=0, entries=3, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b, seenNull=true}"));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 1 },
                        new int[] { 1, 2 },
                        new int[] { 2, 3 },
                        new int[] { 3, 2 },
                        new int[] { 0 },
                        new int[] { 3, 2 }
                    );
                    assertKeys(ordsAndKeys.keys(), null, "foo", "bar", "bort");
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
            }, new OrdinalBytesRefBlock(ords.build(), bytes.build()));
        }
    }

    public void testBooleanHashFalseFirst() {
        boolean[] values = new boolean[] { false, true, true, true, true };
        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BOOLEAN], entries=2, size="));
                assertOrds(ordsAndKeys.ords(), 0, 1, 1, 1, 1);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 2)));
            } else {
                assertThat(
                    ordsAndKeys.description(),
                    equalTo("BooleanBlockHash{channel=0, seenFalse=true, seenTrue=true, seenNull=false}")
                );
                assertOrds(ordsAndKeys.ords(), 1, 2, 2, 2, 2);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(1, 3)));
            }
            assertKeys(ordsAndKeys.keys(), false, true);
        }, blockFactory.newBooleanArrayVector(values, values.length).asBlock());
    }

    public void testBooleanHashTrueFirst() {
        boolean[] values = new boolean[] { true, false, false, true, true };
        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BOOLEAN], entries=2, size="));
                assertOrds(ordsAndKeys.ords(), 0, 1, 1, 0, 0);
                assertKeys(ordsAndKeys.keys(), true, false);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 2)));
            } else {
                assertThat(
                    ordsAndKeys.description(),
                    equalTo("BooleanBlockHash{channel=0, seenFalse=true, seenTrue=true, seenNull=false}")
                );
                assertOrds(ordsAndKeys.ords(), 2, 1, 1, 2, 2);
                assertKeys(ordsAndKeys.keys(), false, true);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(1, 3)));
            }
        }, blockFactory.newBooleanArrayVector(values, values.length).asBlock());
    }

    public void testBooleanHashTrueOnly() {
        boolean[] values = new boolean[] { true, true, true, true };
        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BOOLEAN], entries=1, size="));
                assertOrds(ordsAndKeys.ords(), 0, 0, 0, 0);
                assertKeys(ordsAndKeys.keys(), true);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(TestBlockFactory.getNonBreakingInstance().newConstantIntVector(0, 1)));
            } else {
                assertThat(
                    ordsAndKeys.description(),
                    equalTo("BooleanBlockHash{channel=0, seenFalse=false, seenTrue=true, seenNull=false}")
                );
                assertOrds(ordsAndKeys.ords(), 2, 2, 2, 2);
                assertKeys(ordsAndKeys.keys(), true);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(TestBlockFactory.getNonBreakingInstance().newConstantIntVector(2, 1)));
            }
        }, blockFactory.newBooleanArrayVector(values, values.length).asBlock());
    }

    public void testBooleanHashFalseOnly() {
        boolean[] values = new boolean[] { false, false, false, false };
        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BOOLEAN], entries=1, size="));
                assertOrds(ordsAndKeys.ords(), 0, 0, 0, 0);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(TestBlockFactory.getNonBreakingInstance().newConstantIntVector(0, 1)));
            } else {
                assertThat(
                    ordsAndKeys.description(),
                    equalTo("BooleanBlockHash{channel=0, seenFalse=true, seenTrue=false, seenNull=false}")
                );
                assertOrds(ordsAndKeys.ords(), 1, 1, 1, 1);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(TestBlockFactory.getNonBreakingInstance().newConstantIntVector(1, 1)));
            }
            assertKeys(ordsAndKeys.keys(), false);
        }, blockFactory.newBooleanArrayVector(values, values.length).asBlock());
    }

    public void testBooleanHashWithNulls() {
        try (BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(4)) {
            builder.appendBoolean(false);
            builder.appendNull();
            builder.appendBoolean(true);
            builder.appendNull();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BOOLEAN], entries=3, size="));
                    assertOrds(ordsAndKeys.ords(), 0, 1, 2, 1);
                    assertKeys(ordsAndKeys.keys(), false, null, true);
                } else {
                    assertThat(
                        ordsAndKeys.description(),
                        equalTo("BooleanBlockHash{channel=0, seenFalse=true, seenTrue=true, seenNull=true}")
                    );
                    assertOrds(ordsAndKeys.ords(), 1, 0, 2, 0);
                    assertKeys(ordsAndKeys.keys(), null, false, true);
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 3)));
            }, builder);
        }
    }

    public void testBooleanHashWithMultiValuedFields() {
        try (BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(8)) {
            builder.appendBoolean(false);
            builder.beginPositionEntry();
            builder.appendBoolean(false);
            builder.appendBoolean(true);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBoolean(true);
            builder.appendBoolean(false);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBoolean(true);
            builder.endPositionEntry();
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendBoolean(true);
            builder.appendBoolean(true);
            builder.appendBoolean(false);
            builder.endPositionEntry();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:BOOLEAN], entries=3, size="));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 0 },
                        new int[] { 0, 1 },
                        new int[] { 0, 1 },  // Order is not preserved
                        new int[] { 1 },
                        new int[] { 2 },
                        new int[] { 0, 1 }
                    );
                    assertKeys(ordsAndKeys.keys(), false, true, null);
                } else {
                    assertThat(
                        ordsAndKeys.description(),
                        equalTo("BooleanBlockHash{channel=0, seenFalse=true, seenTrue=true, seenNull=true}")
                    );
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 1 },
                        new int[] { 1, 2 },
                        new int[] { 1, 2 },  // Order is not preserved
                        new int[] { 2 },
                        new int[] { 0 },
                        new int[] { 1, 2 }
                    );
                    assertKeys(ordsAndKeys.keys(), null, false, true);
                }
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 3)));
            }, builder);
        }
    }

    public void testNullHash() {
        Object[] values = new Object[] { null, null, null, null };
        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:NULL], entries=1, size="));
            } else {
                assertThat(ordsAndKeys.description(), equalTo("NullBlockHash{channel=0, seenNull=true}"));
            }
            assertOrds(ordsAndKeys.ords(), 0, 0, 0, 0);
            assertThat(ordsAndKeys.nonEmpty(), equalTo(TestBlockFactory.getNonBreakingInstance().newConstantIntVector(0, 1)));
            assertKeys(ordsAndKeys.keys(), new Object[][] { new Object[] { null } });
        }, blockFactory.newConstantNullBlock(values.length));
    }

    public void testLongLongHash() {
        long[] values1 = new long[] { 0, 1, 0, 1, 0, 1 };
        long[] values2 = new long[] { 0, 0, 0, 1, 1, 1 };
        hash(ordsAndKeys -> {
            Object[][] expectedKeys = {
                new Object[] { 0L, 0L },
                new Object[] { 1L, 0L },
                new Object[] { 1L, 1L },
                new Object[] { 0L, 1L } };

            assertThat(
                ordsAndKeys.description(),
                forcePackedHash
                    ? startsWith("PackedValuesBlockHash{groups=[0:LONG, 1:LONG], entries=4, size=")
                    : equalTo("LongLongBlockHash{channels=[0,1], entries=4}")
            );
            assertOrds(ordsAndKeys.ords(), 0, 1, 0, 2, 3, 2);
            assertKeys(ordsAndKeys.keys(), expectedKeys);
            assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
        },
            blockFactory.newLongArrayVector(values1, values1.length).asBlock(),
            blockFactory.newLongArrayVector(values2, values2.length).asBlock()
        );
    }

    private void append(LongBlock.Builder b1, LongBlock.Builder b2, long[] v1, long[] v2) {
        if (v1 == null) {
            b1.appendNull();
        } else if (v1.length == 1) {
            b1.appendLong(v1[0]);
        } else {
            b1.beginPositionEntry();
            for (long v : v1) {
                b1.appendLong(v);
            }
            b1.endPositionEntry();
        }
        if (v2 == null) {
            b2.appendNull();
        } else if (v2.length == 1) {
            b2.appendLong(v2[0]);
        } else {
            b2.beginPositionEntry();
            for (long v : v2) {
                b2.appendLong(v);
            }
            b2.endPositionEntry();
        }
    }

    public void testLongLongHashWithMultiValuedFields() {
        try (LongBlock.Builder b1 = blockFactory.newLongBlockBuilder(8); LongBlock.Builder b2 = blockFactory.newLongBlockBuilder(8)) {
            append(b1, b2, new long[] { 1, 2 }, new long[] { 10, 20 });
            append(b1, b2, new long[] { 1, 2 }, new long[] { 10 });
            append(b1, b2, new long[] { 1 }, new long[] { 10, 20 });
            append(b1, b2, new long[] { 1 }, new long[] { 10 });
            append(b1, b2, null, new long[] { 10 });
            append(b1, b2, new long[] { 1 }, null);
            append(b1, b2, new long[] { 1, 1, 1 }, new long[] { 10, 10, 10 });
            append(b1, b2, new long[] { 1, 1, 2, 2 }, new long[] { 10, 20, 20 });
            append(b1, b2, new long[] { 1, 2, 3 }, new long[] { 30, 30, 10 });

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:LONG, 1:LONG], entries=10, size="));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 0, 1, 2, 3 },
                        new int[] { 0, 2 },
                        new int[] { 0, 1 },
                        new int[] { 0 },
                        new int[] { 4 },
                        new int[] { 5 },
                        new int[] { 0 },
                        new int[] { 0, 1, 2, 3 },
                        new int[] { 6, 0, 7, 2, 8, 9 }
                    );
                    assertKeys(
                        ordsAndKeys.keys(),
                        new Object[][] {
                            new Object[] { 1L, 10L },
                            new Object[] { 1L, 20L },
                            new Object[] { 2L, 10L },
                            new Object[] { 2L, 20L },
                            new Object[] { null, 10L },
                            new Object[] { 1L, null },
                            new Object[] { 1L, 30L },
                            new Object[] { 2L, 30L },
                            new Object[] { 3L, 30L },
                            new Object[] { 3L, 10L }, }
                    );
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 10)));
                } else {
                    assertThat(ordsAndKeys.description(), equalTo("LongLongBlockHash{channels=[0,1], entries=8}"));
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 0, 1, 2, 3 },
                        new int[] { 0, 2 },
                        new int[] { 0, 1 },
                        new int[] { 0 },
                        null,
                        null,
                        new int[] { 0 },
                        new int[] { 0, 1, 2, 3 },
                        new int[] { 4, 0, 5, 2, 6, 7 }
                    );
                    assertKeys(
                        ordsAndKeys.keys(),
                        new Object[][] {
                            new Object[] { 1L, 10L },
                            new Object[] { 1L, 20L },
                            new Object[] { 2L, 10L },
                            new Object[] { 2L, 20L },
                            new Object[] { 1L, 30L },
                            new Object[] { 2L, 30L },
                            new Object[] { 3L, 30L },
                            new Object[] { 3L, 10L }, }
                    );
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 8)));
                }
            }, b1, b2);
        }
    }

    public void testLongLongHashHugeCombinatorialExplosion() {
        long[] v1 = LongStream.range(0, 5000).toArray();
        long[] v2 = LongStream.range(100, 200).toArray();

        try (
            LongBlock.Builder b1 = blockFactory.newLongBlockBuilder(v1.length);
            LongBlock.Builder b2 = blockFactory.newLongBlockBuilder(v2.length)
        ) {
            append(b1, b2, v1, v2);
            int[] expectedEntries = new int[1];
            int pageSize = between(1000, 16 * 1024);
            hash(ordsAndKeys -> {
                int start = expectedEntries[0];
                expectedEntries[0] = Math.min(expectedEntries[0] + pageSize, v1.length * v2.length);
                assertThat(
                    ordsAndKeys.description(),
                    forcePackedHash
                        ? startsWith("PackedValuesBlockHash{groups=[0:LONG, 1:LONG], entries=" + expectedEntries[0] + ", size=")
                        : equalTo("LongLongBlockHash{channels=[0,1], entries=" + expectedEntries[0] + "}")
                );
                assertOrds(ordsAndKeys.ords(), IntStream.range(start, expectedEntries[0]).toArray());
                assertKeys(
                    ordsAndKeys.keys(),
                    IntStream.range(0, expectedEntries[0])
                        .mapToObj(i -> new Object[] { v1[i / v2.length], v2[i % v2.length] })
                        .toArray(l -> new Object[l][])
                );
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, expectedEntries[0])));
            }, pageSize, b1, b2);

            assertThat("misconfigured test", expectedEntries[0], greaterThan(0));
        }
    }

    public void testIntLongHash() {
        int[] values1 = new int[] { 0, 1, 0, -1, 0, -1 };
        long[] values2 = new long[] { 0, 0, 0, 1, 1, 1 };
        Object[][] expectedKeys = { new Object[] { 0, 0L }, new Object[] { 1, 0L }, new Object[] { -1, 1L }, new Object[] { 0, 1L } };
        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:INT, 1:LONG], entries=4, size="));
                assertThat(ordsAndKeys.description(), endsWith("b}"));
            } else {
                assertThat(
                    ordsAndKeys.description(),
                    startsWith("Adaptive{LongIntBlockHash{keys=[LongKey[channel=1], IntKey[channel=0]], entries=4")
                );
                assertThat(ordsAndKeys.description(), endsWith("b}}"));
            }
            assertOrds(ordsAndKeys.ords(), 0, 1, 0, 2, 3, 2);
            assertKeys(ordsAndKeys.keys(), expectedKeys);
        },
            blockFactory.newIntArrayVector(values1, values1.length).asBlock(),
            blockFactory.newLongArrayVector(values2, values2.length).asBlock()
        );
    }

    public void testLongIntHashWithNulls() {
        try (var longsBuilder = blockFactory.newLongBlockBuilder(6); var intsBuilder = blockFactory.newIntBlockBuilder(6)) {
            longsBuilder.appendLong(2);
            longsBuilder.appendNull();
            longsBuilder.appendLong(3);
            longsBuilder.appendLong(2);
            longsBuilder.appendNull();
            longsBuilder.appendLong(1);
            longsBuilder.appendLong(3);

            intsBuilder.appendInt(-1);
            intsBuilder.appendInt(1);
            intsBuilder.appendInt(5);
            intsBuilder.appendNull();
            intsBuilder.appendNull();
            intsBuilder.appendInt(-10);
            intsBuilder.appendInt(5);
            Object[][] expectedKeys = {
                new Object[] { 2L, -1 },
                new Object[] { null, 1 },
                new Object[] { 3L, 5 },
                new Object[] { 2L, null },
                new Object[] { null, null },
                new Object[] { 1L, -10 } };
            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:LONG, 1:INT], entries=6, size="));
                    assertThat(ordsAndKeys.description(), endsWith("b}"));
                } else {
                    assertThat(
                        ordsAndKeys.description(),
                        startsWith("Adaptive{PackedValuesBlockHash{groups=[0:LONG, 1:INT], entries=6, size=")
                    );
                    assertThat(ordsAndKeys.description(), endsWith("b}}"));
                }
                assertOrds(ordsAndKeys.ords(), 0, 1, 2, 3, 4, 5, 2);
                assertKeys(ordsAndKeys.keys(), expectedKeys);
            }, longsBuilder.build(), intsBuilder.build());
        }
    }

    public void testLongDoubleHash() {
        long[] values1 = new long[] { 0, 1, 0, 1, 0, 1 };
        double[] values2 = new double[] { 0, 0, 0, 1, 1, 1 };
        Object[][] expectedKeys = { new Object[] { 0L, 0d }, new Object[] { 1L, 0d }, new Object[] { 1L, 1d }, new Object[] { 0L, 1d } };
        hash((OrdsAndKeys ordsAndKeys) -> {
            assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:LONG, 1:DOUBLE], entries=4, size="));
            assertThat(ordsAndKeys.description(), endsWith("b}"));
            assertOrds(ordsAndKeys.ords(), 0, 1, 0, 2, 3, 2);
            assertKeys(ordsAndKeys.keys(), expectedKeys);
        },
            blockFactory.newLongArrayVector(values1, values1.length).asBlock(),
            blockFactory.newDoubleArrayVector(values2, values2.length).asBlock()
        );
    }

    public void testIntBooleanHash() {
        int[] values1 = new int[] { 0, 1, 0, 1, 0, 1 };
        boolean[] values2 = new boolean[] { false, false, false, true, true, true };
        Object[][] expectedKeys = {
            new Object[] { 0, false },
            new Object[] { 1, false },
            new Object[] { 1, true },
            new Object[] { 0, true } };
        hash((OrdsAndKeys ordsAndKeys) -> {
            assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:INT, 1:BOOLEAN], entries=4, size="));
            assertThat(ordsAndKeys.description(), endsWith("b}"));
            assertOrds(ordsAndKeys.ords(), 0, 1, 0, 2, 3, 2);
            assertKeys(ordsAndKeys.keys(), expectedKeys);
        },
            blockFactory.newIntArrayVector(values1, values1.length).asBlock(),
            blockFactory.newBooleanArrayVector(values2, values2.length).asBlock()
        );
    }

    public void testLongLongHashWithNull() {
        try (LongBlock.Builder b1 = blockFactory.newLongBlockBuilder(2); LongBlock.Builder b2 = blockFactory.newLongBlockBuilder(2)) {
            b1.appendLong(1);
            b2.appendLong(0);
            b1.appendNull();
            b2.appendNull();
            b1.appendLong(0);
            b2.appendLong(1);
            b1.appendLong(0);
            b2.appendNull();
            b1.appendNull();
            b2.appendLong(0);

            hash((OrdsAndKeys ordsAndKeys) -> {
                if (forcePackedHash) {
                    assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:LONG, 1:LONG], entries=5, size="));
                    assertOrds(ordsAndKeys.ords(), 0, 1, 2, 3, 4);
                    assertKeys(
                        ordsAndKeys.keys(),
                        new Object[][] {
                            new Object[] { 1L, 0L },
                            new Object[] { null, null },
                            new Object[] { 0L, 1L },
                            new Object[] { 0L, null },
                            new Object[] { null, 0L }, }
                    );
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 5)));
                } else {
                    assertThat(ordsAndKeys.description(), equalTo("LongLongBlockHash{channels=[0,1], entries=2}"));
                    assertOrds(ordsAndKeys.ords(), 0, null, 1, null, null);
                    assertKeys(ordsAndKeys.keys(), new Object[][] { new Object[] { 1L, 0L }, new Object[] { 0L, 1L } });
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 2)));
                }
            }, b1, b2);
        }
    }

    public void testLongBytesRefHash() {
        try (
            LongBlock.Builder b1 = blockFactory.newLongBlockBuilder(8);
            BytesRefBlock.Builder b2 = blockFactory.newBytesRefBlockBuilder(8)
        ) {
            append(b1, b2, new long[] { 0 }, new String[] { "cat" });
            append(b1, b2, new long[] { 1 }, new String[] { "cat" });
            append(b1, b2, new long[] { 0 }, new String[] { "cat" });
            append(b1, b2, new long[] { 1 }, new String[] { "dog" });
            append(b1, b2, new long[] { 0 }, new String[] { "dog" });
            append(b1, b2, new long[] { 1 }, new String[] { "dog" });
            Object[][] expectedKeys = {
                new Object[] { 0L, "cat" },
                new Object[] { 1L, "cat" },
                new Object[] { 1L, "dog" },
                new Object[] { 0L, "dog" } };

            hash((OrdsAndKeys ordsAndKeys) -> {
                assertThat(
                    ordsAndKeys.description(),
                    startsWith(
                        forcePackedHash
                            ? "PackedValuesBlockHash{groups=[0:LONG, 1:BYTES_REF], entries=4, size="
                            : "Adaptive{BytesRefLongBlockHash{keys=[BytesRefKey[channel=1], LongKey[channel=0]], entries=4, size="
                    )
                );
                assertThat(ordsAndKeys.description(), endsWith(forcePackedHash ? "b}" : "b}}"));
                assertOrds(ordsAndKeys.ords(), 0, 1, 0, 2, 3, 2);
                assertKeys(ordsAndKeys.keys(), expectedKeys);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
            }, b1, b2);
        }
    }

    public void testLongBytesRefHashWithNull() {
        try (
            LongBlock.Builder b1 = blockFactory.newLongBlockBuilder(2);
            BytesRefBlock.Builder b2 = blockFactory.newBytesRefBlockBuilder(2)
        ) {
            append(b1, b2, new long[] { 1 }, new String[] { "cat" });
            append(b1, b2, null, null);
            append(b1, b2, new long[] { 0 }, new String[] { "dog" });
            append(b1, b2, new long[] { 0 }, null);
            append(b1, b2, null, new String[] { "nn" });

            // Either path (packed or adaptive) handles nulls correctly: the adaptive hash sees a non-vector
            // page and migrates to PackedValuesBlockHash before doing any work, so ord assignment matches.
            hash((OrdsAndKeys ordsAndKeys) -> {
                assertThat(
                    ordsAndKeys.description(),
                    startsWith(
                        forcePackedHash
                            ? "PackedValuesBlockHash{groups=[0:LONG, 1:BYTES_REF], entries=5, size="
                            : "Adaptive{PackedValuesBlockHash{groups=[0:LONG, 1:BYTES_REF], entries=5, size="
                    )
                );
                assertThat(ordsAndKeys.description(), endsWith(forcePackedHash ? "b}" : "b}}"));
                assertOrds(ordsAndKeys.ords(), 0, 1, 2, 3, 4);
                assertKeys(
                    ordsAndKeys.keys(),
                    new Object[][] {
                        new Object[] { 1L, "cat" },
                        new Object[] { null, null },
                        new Object[] { 0L, "dog" },
                        new Object[] { 0L, null },
                        new Object[] { null, "nn" } }
                );
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 5)));
            }, b1, b2);
        }
    }

    private void append(LongBlock.Builder b1, BytesRefBlock.Builder b2, long[] v1, String[] v2) {
        if (v1 == null) {
            b1.appendNull();
        } else if (v1.length == 1) {
            b1.appendLong(v1[0]);
        } else {
            b1.beginPositionEntry();
            for (long v : v1) {
                b1.appendLong(v);
            }
            b1.endPositionEntry();
        }
        if (v2 == null) {
            b2.appendNull();
        } else if (v2.length == 1) {
            b2.appendBytesRef(new BytesRef(v2[0]));
        } else {
            b2.beginPositionEntry();
            for (String v : v2) {
                b2.appendBytesRef(new BytesRef(v));
            }
            b2.endPositionEntry();
        }
    }

    // Returns the size of the bytesRefBlockHash depending on the underlying implementation.
    static String byteRefBlockHashSize() {
        if (HashImplFactory.SWISS_HASH_AVAILABLE) {
            return "35128b";
        }
        return "531b";
    }

    public void testLongBytesRefHashWithMultiValuedFields() {
        try (
            LongBlock.Builder b1 = blockFactory.newLongBlockBuilder(8);
            BytesRefBlock.Builder b2 = blockFactory.newBytesRefBlockBuilder(8)
        ) {
            append(b1, b2, new long[] { 1, 2 }, new String[] { "a", "b" });
            append(b1, b2, new long[] { 1, 2 }, new String[] { "a" });
            append(b1, b2, new long[] { 1 }, new String[] { "a", "b" });
            append(b1, b2, new long[] { 1 }, new String[] { "a" });
            append(b1, b2, null, new String[] { "a" });
            append(b1, b2, new long[] { 1 }, null);
            append(b1, b2, new long[] { 1, 1, 1 }, new String[] { "a", "a", "a" });
            append(b1, b2, new long[] { 1, 1, 2, 2 }, new String[] { "a", "b", "b" });
            append(b1, b2, new long[] { 1, 2, 3 }, new String[] { "c", "c", "a" });

            // MV inputs cannot be vectors, so the adaptive hash migrates to PackedValuesBlockHash on
            // the first page. Ord assignment then matches the forced-packed branch exactly.
            hash((OrdsAndKeys ordsAndKeys) -> {
                assertThat(
                    ordsAndKeys.description(),
                    startsWith(
                        forcePackedHash
                            ? "PackedValuesBlockHash{groups=[0:LONG, 1:BYTES_REF], entries=10, size="
                            : "Adaptive{PackedValuesBlockHash{groups=[0:LONG, 1:BYTES_REF], entries=10, size="
                    )
                );
                assertThat(ordsAndKeys.description(), endsWith(forcePackedHash ? "b}" : "b}}"));
                assertOrds(
                    ordsAndKeys.ords(),
                    new int[] { 0, 1, 2, 3 },
                    new int[] { 0, 2 },
                    new int[] { 0, 1 },
                    new int[] { 0 },
                    new int[] { 4 },
                    new int[] { 5 },
                    new int[] { 0 },
                    new int[] { 0, 1, 2, 3 },
                    new int[] { 6, 0, 7, 2, 8, 9 }
                );
                assertKeys(
                    ordsAndKeys.keys(),
                    new Object[][] {
                        new Object[] { 1L, "a" },
                        new Object[] { 1L, "b" },
                        new Object[] { 2L, "a" },
                        new Object[] { 2L, "b" },
                        new Object[] { null, "a" },
                        new Object[] { 1L, null },
                        new Object[] { 1L, "c" },
                        new Object[] { 2L, "c" },
                        new Object[] { 3L, "c" },
                        new Object[] { 3L, "a" }, }
                );
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 10)));
            }, b1, b2);
        }
    }

    public void testBytesRefLongHashHugeCombinatorialExplosion() {
        long[] v1 = LongStream.range(0, 3000).toArray();
        String[] v2 = LongStream.range(100, 200).mapToObj(l -> "a" + l).toArray(String[]::new);

        try (
            LongBlock.Builder b1 = blockFactory.newLongBlockBuilder(v1.length);
            BytesRefBlock.Builder b2 = blockFactory.newBytesRefBlockBuilder(v2.length);
        ) {
            append(b1, b2, v1, v2);
            int[] expectedEntries = new int[1];
            int pageSize = between(1000, 16 * 1024);
            // The single MV row forces both paths through PackedValuesBlockHash (the adaptive hash
            // migrates immediately on a non-vector page), so ords/keys are identical.
            hash(ordsAndKeys -> {
                int start = expectedEntries[0];
                expectedEntries[0] = Math.min(expectedEntries[0] + pageSize, v1.length * v2.length);
                assertThat(
                    ordsAndKeys.description(),
                    startsWith(
                        forcePackedHash
                            ? "PackedValuesBlockHash{groups=[0:LONG, 1:BYTES_REF], entries=" + expectedEntries[0] + ", size="
                            : "Adaptive{PackedValuesBlockHash{groups=[0:LONG, 1:BYTES_REF], entries=" + expectedEntries[0] + ", size="
                    )
                );
                assertOrds(ordsAndKeys.ords(), IntStream.range(start, expectedEntries[0]).toArray());
                assertKeys(
                    ordsAndKeys.keys(),
                    IntStream.range(0, expectedEntries[0])
                        .mapToObj(i -> new Object[] { v1[i / v2.length], v2[i % v2.length] })
                        .toArray(l -> new Object[l][])
                );
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, expectedEntries[0])));
            }, pageSize, b1, b2);

            assertThat("misconfigured test", expectedEntries[0], greaterThan(0));
        }
    }

    /**
     * Drive the {@link LongBytesRefAdaptiveBlockHash} through a vector page first, then a block page
     * with both nulls and multivalues, then another vector page that re-uses an old key. Verifies that:
     * <ul>
     *   <li>group ids assigned on the vector page survive the migration to {@link PackedValuesBlockHash};</li>
     *   <li>null/MV groups are correctly enumerated post-migration (the broken behavior {@code #99434}
     *       guarded against was dropping null-keyed rows and never forming {@code (null, x)}/{@code (x, null)} groups);</li>
     *   <li>a re-occurrence of an old key on a later page maps to the same group id as before.</li>
     * </ul>
     */
    public void testLongBytesRefHashMidStreamMigrationToPacked() {
        if (forcePackedHash) {
            // Nothing to migrate: both inputs go to PackedValuesBlockHash from the start.
            // The general null/MV correctness for that path is exercised by the other tests above.
            return;
        }
        // Page 1: pure vectors. The adaptive hash uses its dict-aware fast path. Two groups: (1L,"a"), (2L,"b").
        try (
            LongBlock.Builder b1a = blockFactory.newLongBlockBuilder(2);
            BytesRefBlock.Builder b2a = blockFactory.newBytesRefBlockBuilder(2)
        ) {
            append(b1a, b2a, new long[] { 1 }, new String[] { "a" });
            append(b1a, b2a, new long[] { 2 }, new String[] { "b" });
            // Page 2: a null key + an MV key. Forces migration to PackedValuesBlockHash.
            // null group: (null, "z"); MV: (1L,"a"), (1L,"b"), (3L,"a"), (3L,"b") — the last two are new.
            try (
                LongBlock.Builder b1b = blockFactory.newLongBlockBuilder(2);
                BytesRefBlock.Builder b2b = blockFactory.newBytesRefBlockBuilder(2)
            ) {
                append(b1b, b2b, null, new String[] { "z" });
                append(b1b, b2b, new long[] { 1, 3 }, new String[] { "a", "b" });
                // Page 3: vector again, re-uses the old (1L,"a") key (must keep its old ord).
                try (
                    LongBlock.Builder b1c = blockFactory.newLongBlockBuilder(1);
                    BytesRefBlock.Builder b2c = blockFactory.newBytesRefBlockBuilder(1)
                ) {
                    append(b1c, b2c, new long[] { 1 }, new String[] { "a" });

                    Block[] page1 = Block.Builder.buildAll(b1a, b2a);
                    Block[] page2 = Block.Builder.buildAll(b1b, b2b);
                    Block[] page3 = Block.Builder.buildAll(b1c, b2c);
                    try (
                        BlockHash hash = BlockHash.build(
                            List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.BYTES_REF)),
                            blockFactory,
                            16 * 1024,
                            true
                        )
                    ) {
                        // Page 1: vector path; expect ords (0, 1).
                        List<Integer> ords1 = collectOrds(hash, page1);
                        assertThat(ords1, equalTo(List.of(0, 1)));
                        assertThat(hash.toString(), startsWith("Adaptive{BytesRefLongBlockHash{"));

                        // Page 2: triggers migration. Existing groups (1L,"a")=0, (2L,"b")=1 stay put.
                        // New groups: 2 = (null,"z"); 3 = (1L,"b") (cross), 4 = (3L,"a"), 5 = (3L,"b").
                        // (1L,"a"=0) is reused for the (1L,"a") corner of the cross product.
                        // For position 1, the (1L,"a"),(1L,"b"),(3L,"a"),(3L,"b") combos may emit in
                        // PackedValues iteration order (long-outer, bytes-inner), so we just collect the set.
                        List<List<Integer>> ords2 = collectOrdsByPosition(hash, page2);
                        assertThat(hash.toString(), startsWith("Adaptive{PackedValuesBlockHash{"));
                        // Position 0: (null, "z") -> a single ord (the null group). Distinct from prior groups.
                        assertThat("position 0 single value", ords2.get(0).size(), equalTo(1));
                        int nullGroupOrd = ords2.get(0).get(0);
                        assertThat(nullGroupOrd, greaterThan(1));
                        // Position 1: (1,a),(1,b),(3,a),(3,b) -> 4 groups; (1L,"a") must be 0 (preserved).
                        assertThat(ords2.get(1).size(), equalTo(4));
                        assertThat("(1L, a) keeps its ord", ords2.get(1).contains(0), equalTo(true));

                        // Page 3: re-add (1L, "a") and verify the same ord as page 1.
                        List<Integer> ords3 = collectOrds(hash, page3);
                        assertThat(ords3, equalTo(List.of(0)));

                        // Final group set: (1,a)=0, (2,b)=1, plus the four added on page 2 -> 6 total.
                        assertThat(hash.numKeys(), equalTo(6));
                        try (IntVector nonEmpty = hash.nonEmpty()) {
                            assertThat(nonEmpty.getPositionCount(), equalTo(6));
                            Block[] keys = hash.getKeys(nonEmpty);
                            try {
                                LongBlock kLong = (LongBlock) keys[0];
                                BytesRefBlock kBytes = (BytesRefBlock) keys[1];
                                BytesRef scratch = new BytesRef();
                                Set<List<Object>> got = new HashSet<>();
                                for (int p = 0; p < 6; p++) {
                                    Object lv = kLong.isNull(p) ? null : kLong.getLong(p);
                                    Object bv = kBytes.isNull(p) ? null : kBytes.getBytesRef(p, scratch).utf8ToString();
                                    got.add(List.of(lv == null ? "_NULL_" : lv, bv == null ? "_NULL_" : bv));
                                }
                                assertThat(
                                    got,
                                    equalTo(
                                        Set.of(
                                            List.of(1L, "a"),
                                            List.of(2L, "b"),
                                            List.of("_NULL_", "z"),
                                            List.of(1L, "b"),
                                            List.of(3L, "a"),
                                            List.of(3L, "b")
                                        )
                                    )
                                );
                            } finally {
                                Releasables.closeExpectNoException(keys);
                            }
                        }
                    } finally {
                        Releasables.closeExpectNoException(Releasables.wrap(page1), Releasables.wrap(page2), Releasables.wrap(page3));
                    }
                }
            }
        }
    }

    /**
     * Mirror of {@link #testLongBytesRefHashMidStreamMigrationToPacked} for the swapped key order
     * {@code (BYTES_REF, LONG)}. Specifically exercises the hand-written
     * {@code reverseOutput == true} branch in
     * {@link LongBytesRefAdaptiveBlockHash.BytesRefLongVectorOnlyBlockHash#migrateToPackedHash()}:
     * if the byte-layout swap were wrong, migrated keys would collide with each other or with
     * pre-migration keys, breaking the ord-stability invariant asserted below.
     */
    public void testBytesRefLongHashMidStreamMigrationToPacked() {
        if (forcePackedHash) {
            return;
        }
        try (
            LongBlock.Builder b1a = blockFactory.newLongBlockBuilder(2);
            BytesRefBlock.Builder b2a = blockFactory.newBytesRefBlockBuilder(2)
        ) {
            append(b1a, b2a, new long[] { 1 }, new String[] { "a" });
            append(b1a, b2a, new long[] { 2 }, new String[] { "b" });
            try (
                LongBlock.Builder b1b = blockFactory.newLongBlockBuilder(2);
                BytesRefBlock.Builder b2b = blockFactory.newBytesRefBlockBuilder(2)
            ) {
                append(b1b, b2b, null, new String[] { "z" });
                append(b1b, b2b, new long[] { 1, 3 }, new String[] { "a", "b" });
                try (
                    LongBlock.Builder b1c = blockFactory.newLongBlockBuilder(1);
                    BytesRefBlock.Builder b2c = blockFactory.newBytesRefBlockBuilder(1)
                ) {
                    append(b1c, b2c, new long[] { 1 }, new String[] { "a" });

                    Block[] longsPage1 = Block.Builder.buildAll(b1a, b2a);
                    Block[] longsPage2 = Block.Builder.buildAll(b1b, b2b);
                    Block[] longsPage3 = Block.Builder.buildAll(b1c, b2c);
                    // Reverse shape: bytes are channel 0, longs are channel 1. Page columns are [bytes, long].
                    Block[] page1 = new Block[] { longsPage1[1], longsPage1[0] };
                    Block[] page2 = new Block[] { longsPage2[1], longsPage2[0] };
                    Block[] page3 = new Block[] { longsPage3[1], longsPage3[0] };
                    try (
                        BlockHash hash = BlockHash.build(
                            List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF), new BlockHash.GroupSpec(1, ElementType.LONG)),
                            blockFactory,
                            16 * 1024,
                            true
                        )
                    ) {
                        List<Integer> ords1 = collectOrds(hash, page1);
                        assertThat(ords1, equalTo(List.of(0, 1)));
                        assertThat(hash.toString(), startsWith("Adaptive{BytesRefLongBlockHash{"));

                        List<List<Integer>> ords2 = collectOrdsByPosition(hash, page2);
                        assertThat(hash.toString(), startsWith("Adaptive{PackedValuesBlockHash{"));
                        assertThat("position 0 single value", ords2.get(0).size(), equalTo(1));
                        int nullGroupOrd = ords2.get(0).get(0);
                        assertThat(nullGroupOrd, greaterThan(1));
                        assertThat(ords2.get(1).size(), equalTo(4));
                        assertThat("(a, 1L) keeps its ord", ords2.get(1).contains(0), equalTo(true));

                        List<Integer> ords3 = collectOrds(hash, page3);
                        assertThat(ords3, equalTo(List.of(0)));

                        assertThat(hash.numKeys(), equalTo(6));
                        try (IntVector nonEmpty = hash.nonEmpty()) {
                            assertThat(nonEmpty.getPositionCount(), equalTo(6));
                            Block[] keys = hash.getKeys(nonEmpty);
                            try {
                                BytesRefBlock kBytes = (BytesRefBlock) keys[0];
                                LongBlock kLong = (LongBlock) keys[1];
                                BytesRef scratch = new BytesRef();
                                Set<List<Object>> got = new HashSet<>();
                                for (int p = 0; p < 6; p++) {
                                    Object bv = kBytes.isNull(p) ? null : kBytes.getBytesRef(p, scratch).utf8ToString();
                                    Object lv = kLong.isNull(p) ? null : kLong.getLong(p);
                                    got.add(List.of(bv == null ? "_NULL_" : bv, lv == null ? "_NULL_" : lv));
                                }
                                assertThat(
                                    got,
                                    equalTo(
                                        Set.of(
                                            List.of("a", 1L),
                                            List.of("b", 2L),
                                            List.of("z", "_NULL_"),
                                            List.of("b", 1L),
                                            List.of("a", 3L),
                                            List.of("b", 3L)
                                        )
                                    )
                                );
                            } finally {
                                Releasables.closeExpectNoException(keys);
                            }
                        }
                    } finally {
                        Releasables.closeExpectNoException(
                            Releasables.wrap(longsPage1),
                            Releasables.wrap(longsPage2),
                            Releasables.wrap(longsPage3)
                        );
                    }
                }
            }
        }
    }

    /**
     * Collects all group ids emitted by {@link BlockHash#add} into a single flat list, in the order
     * the hash emitted them. Used by {@link #testLongBytesRefHashMidStreamMigrationToPacked}.
     */
    private List<Integer> collectOrds(BlockHash hash, Block[] page) {
        List<Integer> out = new ArrayList<>();
        hash.add(new Page(page), new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntVector groupIds) {
                for (int i = 0; i < groupIds.getPositionCount(); i++) {
                    out.add(groupIds.getInt(i));
                }
            }

            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addBlock(groupIds);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addBlock(groupIds);
            }

            private void addBlock(IntBlock groupIds) {
                for (int p = 0; p < groupIds.getPositionCount(); p++) {
                    int start = groupIds.getFirstValueIndex(p);
                    int end = start + groupIds.getValueCount(p);
                    for (int i = start; i < end; i++) {
                        out.add(groupIds.getInt(i));
                    }
                }
            }

            @Override
            public void close() {}
        });
        return out;
    }

    /**
     * Like {@link #collectOrds} but groups the emitted ords by input position. Multivalued positions
     * yield a list of size > 1; null positions yield an empty list.
     */
    private List<List<Integer>> collectOrdsByPosition(BlockHash hash, Block[] page) {
        List<List<Integer>> out = new ArrayList<>();
        hash.add(new Page(page), new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntVector groupIds) {
                for (int i = 0; i < groupIds.getPositionCount(); i++) {
                    out.add(List.of(groupIds.getInt(i)));
                }
            }

            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addBlock(groupIds);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addBlock(groupIds);
            }

            private void addBlock(IntBlock groupIds) {
                for (int p = 0; p < groupIds.getPositionCount(); p++) {
                    if (groupIds.isNull(p)) {
                        out.add(List.of());
                        continue;
                    }
                    int start = groupIds.getFirstValueIndex(p);
                    int end = start + groupIds.getValueCount(p);
                    List<Integer> at = new ArrayList<>(end - start);
                    for (int i = start; i < end; i++) {
                        at.add(groupIds.getInt(i));
                    }
                    out.add(at);
                }
            }

            @Override
            public void close() {}
        });
        return out;
    }

    public void testLongNull() {
        long[] values = new long[] { 0, 1, 0, 2, 3, 1 };
        hash(ordsAndKeys -> {
            Object[][] expectedKeys = {
                new Object[] { 0L, null },
                new Object[] { 1L, null },
                new Object[] { 2L, null },
                new Object[] { 3L, null } };

            assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:LONG, 1:NULL], entries=4, size="));
            assertOrds(ordsAndKeys.ords(), 0, 1, 0, 2, 3, 1);
            assertKeys(ordsAndKeys.keys(), expectedKeys);
            assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 4)));
        }, blockFactory.newLongArrayVector(values, values.length).asBlock(), blockFactory.newConstantNullBlock(values.length));
    }

    public void test2BytesRefsHighCardinalityKey() {
        final Page page;
        int positions1 = 10;
        int positions2 = 100_000;
        if (randomBoolean()) {
            positions1 = 100_000;
            positions2 = 10;
        }
        final int totalPositions = positions1 * positions2;
        try (
            BytesRefBlock.Builder builder1 = blockFactory.newBytesRefBlockBuilder(totalPositions);
            BytesRefBlock.Builder builder2 = blockFactory.newBytesRefBlockBuilder(totalPositions);
        ) {
            for (int i = 0; i < positions1; i++) {
                for (int p = 0; p < positions2; p++) {
                    builder1.appendBytesRef(new BytesRef("abcdef" + i));
                    builder2.appendBytesRef(new BytesRef("abcdef" + p));
                }
            }
            page = new Page(builder1.build(), builder2.build());
        }
        record Output(int offset, IntBlock block, IntVector vector) implements Releasable {
            @Override
            public void close() {
                Releasables.close(block, vector);
            }
        }
        List<Output> output = new ArrayList<>();

        try (BlockHash hash1 = new BytesRef2BlockHash(blockFactory, 0, 1, totalPositions);) {
            hash1.add(page, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    groupIds.incRef();
                    output.add(new Output(positionOffset, groupIds, null));
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    groupIds.incRef();
                    output.add(new Output(positionOffset, groupIds, null));
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    groupIds.incRef();
                    output.add(new Output(positionOffset, null, groupIds));
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });

            try (IntVector nonEmpty = hash1.nonEmpty()) {
                Block[] keys = hash1.getKeys(nonEmpty);
                try {
                    Set<String> distinctKeys = new HashSet<>();
                    BytesRefBlock block0 = (BytesRefBlock) keys[0];
                    BytesRefBlock block1 = (BytesRefBlock) keys[1];
                    BytesRef scratch = new BytesRef();
                    StringBuilder builder = new StringBuilder();
                    for (int i = 0; i < totalPositions; i++) {
                        builder.setLength(0);
                        builder.append(BytesRefs.toString(block0.getBytesRef(i, scratch)));
                        builder.append("#");
                        builder.append(BytesRefs.toString(block1.getBytesRef(i, scratch)));
                        distinctKeys.add(builder.toString());
                    }
                    assertThat(distinctKeys.size(), equalTo(totalPositions));
                } finally {
                    Releasables.close(keys);
                }
            }
        } finally {
            Releasables.close(output);
            page.releaseBlocks();
        }
    }

    public void test2BytesRefs() {
        final Page page;
        final int positions = randomIntBetween(1, 1000);
        final boolean generateVector = randomBoolean();
        try (
            BytesRefBlock.Builder builder1 = blockFactory.newBytesRefBlockBuilder(positions);
            BytesRefBlock.Builder builder2 = blockFactory.newBytesRefBlockBuilder(positions);
        ) {
            List<BytesRefBlock.Builder> builders = List.of(builder1, builder2);
            for (int p = 0; p < positions; p++) {
                for (BytesRefBlock.Builder builder : builders) {
                    int valueCount = generateVector ? 1 : between(0, 3);
                    switch (valueCount) {
                        case 0 -> builder.appendNull();
                        case 1 -> builder.appendBytesRef(new BytesRef(Integer.toString(between(1, 100))));
                        default -> {
                            builder.beginPositionEntry();
                            for (int v = 0; v < valueCount; v++) {
                                builder.appendBytesRef(new BytesRef(Integer.toString(between(1, 100))));
                            }
                            builder.endPositionEntry();
                        }
                    }
                }
            }
            page = new Page(builder1.build(), builder2.build());
        }
        final int emitBatchSize = between(positions, 10 * 1024);
        var groupSpecs = List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF), new BlockHash.GroupSpec(1, ElementType.BYTES_REF));
        record Output(int offset, IntBlock block, IntVector vector) implements Releasable {
            @Override
            public void close() {
                Releasables.close(block, vector);
            }
        }
        List<Output> output1 = new ArrayList<>();
        List<Output> output2 = new ArrayList<>();
        try (
            BlockHash hash1 = new BytesRef2BlockHash(blockFactory, 0, 1, emitBatchSize);
            BlockHash hash2 = new PackedValuesBlockHash(groupSpecs, blockFactory, emitBatchSize)
        ) {
            hash1.add(page, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    groupIds.incRef();
                    output1.add(new Output(positionOffset, groupIds, null));
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    groupIds.incRef();
                    output1.add(new Output(positionOffset, groupIds, null));
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    groupIds.incRef();
                    output1.add(new Output(positionOffset, null, groupIds));
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });
            hash2.add(page, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    groupIds.incRef();
                    output2.add(new Output(positionOffset, groupIds, null));
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    groupIds.incRef();
                    output2.add(new Output(positionOffset, groupIds, null));
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    groupIds.incRef();
                    output2.add(new Output(positionOffset, null, groupIds));
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });
            assertThat(output1.size(), equalTo(output2.size()));
            for (int i = 0; i < output1.size(); i++) {
                Output o1 = output1.get(i);
                Output o2 = output2.get(i);
                assertThat(o1.offset, equalTo(o2.offset));
                if (o1.vector != null) {
                    assertNull(o1.block);
                    assertThat(o1.vector, equalTo(o2.vector != null ? o2.vector : o2.block.asVector()));
                } else {
                    assertNull(o2.vector);
                    assertThat(o1.block, equalTo(o2.block));
                }
            }
        } finally {
            Releasables.close(output1);
            Releasables.close(output2);
            page.releaseBlocks();
        }
    }

    public void test3BytesRefs() {
        final Page page;
        final int positions = randomIntBetween(1, 1000);
        final boolean generateVector = randomBoolean();
        try (
            BytesRefBlock.Builder builder1 = blockFactory.newBytesRefBlockBuilder(positions);
            BytesRefBlock.Builder builder2 = blockFactory.newBytesRefBlockBuilder(positions);
            BytesRefBlock.Builder builder3 = blockFactory.newBytesRefBlockBuilder(positions)
        ) {
            List<BytesRefBlock.Builder> builders = List.of(builder1, builder2, builder3);
            for (int p = 0; p < positions; p++) {
                for (BytesRefBlock.Builder builder : builders) {
                    int valueCount = generateVector ? 1 : between(0, 3);
                    switch (valueCount) {
                        case 0 -> builder.appendNull();
                        case 1 -> builder.appendBytesRef(new BytesRef(Integer.toString(between(1, 100))));
                        default -> {
                            builder.beginPositionEntry();
                            for (int v = 0; v < valueCount; v++) {
                                builder.appendBytesRef(new BytesRef(Integer.toString(between(1, 100))));
                            }
                            builder.endPositionEntry();
                        }
                    }
                }
            }
            page = new Page(builder1.build(), builder2.build(), builder3.build());
        }
        final int emitBatchSize = between(positions, 10 * 1024);
        var groupSpecs = List.of(
            new BlockHash.GroupSpec(0, ElementType.BYTES_REF),
            new BlockHash.GroupSpec(1, ElementType.BYTES_REF),
            new BlockHash.GroupSpec(2, ElementType.BYTES_REF)
        );
        record Output(int offset, IntBlock block, IntVector vector) implements Releasable {
            @Override
            public void close() {
                Releasables.close(block, vector);
            }
        }
        List<Output> output1 = new ArrayList<>();
        List<Output> output2 = new ArrayList<>();
        try (
            BlockHash hash1 = new BytesRef3BlockHash(blockFactory, 0, 1, 2, emitBatchSize);
            BlockHash hash2 = new PackedValuesBlockHash(groupSpecs, blockFactory, emitBatchSize)
        ) {
            hash1.add(page, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    groupIds.incRef();
                    output1.add(new Output(positionOffset, groupIds, null));
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    groupIds.incRef();
                    output1.add(new Output(positionOffset, groupIds, null));
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    groupIds.incRef();
                    output1.add(new Output(positionOffset, null, groupIds));
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });
            hash2.add(page, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    groupIds.incRef();
                    output2.add(new Output(positionOffset, groupIds, null));
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    groupIds.incRef();
                    output2.add(new Output(positionOffset, groupIds, null));
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    groupIds.incRef();
                    output2.add(new Output(positionOffset, null, groupIds));
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });
            assertThat(output1.size(), equalTo(output2.size()));
            for (int i = 0; i < output1.size(); i++) {
                Output o1 = output1.get(i);
                Output o2 = output2.get(i);
                assertThat(o1.offset, equalTo(o2.offset));
                if (o1.vector != null) {
                    assertNull(o1.block);
                    assertThat(o1.vector, equalTo(o2.vector != null ? o2.vector : o2.block.asVector()));
                } else {
                    assertNull(o2.vector);
                    assertThat(o1.block, equalTo(o2.block));
                }
            }
        } finally {
            Releasables.close(output1);
            Releasables.close(output2);
            page.releaseBlocks();
        }
    }

    public void testTimeSeriesBlockHash() throws Exception {
        long endTime = randomLongBetween(10_000_000, 20_000_000);
        var hash1 = new TimeSeriesBlockHash(0, 1, false, randomBoolean(), blockFactory);
        var hash2 = BlockHash.build(
            List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF), new BlockHash.GroupSpec(1, ElementType.LONG)),
            blockFactory,
            32 * 1024,
            forcePackedHash
        );
        int numPages = between(1, 100);
        int globalTsid = -1;
        long timestamp = endTime;
        try (hash1; hash2) {
            for (int p = 0; p < numPages; p++) {
                int numRows = between(1, 1000);
                if (randomBoolean()) {
                    timestamp -= between(0, 100);
                }
                try (
                    BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(numRows);
                    IntVector.Builder ordinalBuilder = blockFactory.newIntVectorBuilder(numRows);
                    LongVector.Builder timestampsBuilder = blockFactory.newLongVectorBuilder(numRows)
                ) {
                    int perPageOrd = -1;
                    for (int i = 0; i < numRows; i++) {
                        boolean newGroup = globalTsid == -1 || randomInt(100) < 10;
                        if (newGroup) {
                            globalTsid++;
                            timestamp = endTime;
                            if (randomBoolean()) {
                                timestamp -= between(0, 1000);
                            }
                        }
                        if (perPageOrd == -1 || newGroup) {
                            perPageOrd++;
                            dictBuilder.appendBytesRef(new BytesRef(String.format(Locale.ROOT, "id-%06d", globalTsid)));
                        }
                        ordinalBuilder.appendInt(perPageOrd);
                        if (randomInt(100) < 20) {
                            timestamp -= between(1, 10);
                        }
                        timestampsBuilder.appendLong(timestamp);
                    }
                    try (
                        var tsidBlock = new OrdinalBytesRefVector(ordinalBuilder.build(), dictBuilder.build()).asBlock();
                        var timestampBlock = timestampsBuilder.build().asBlock()
                    ) {
                        Page page = new Page(tsidBlock, timestampBlock);
                        IntVector[] ords1 = new IntVector[1];
                        hash1.add(page, new GroupingAggregatorFunction.AddInput() {
                            @Override
                            public void add(int positionOffset, IntArrayBlock groupIds) {
                                throw new AssertionError("time-series block hash should emit a vector");
                            }

                            @Override
                            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                                throw new AssertionError("time-series block hash should emit a vector");
                            }

                            @Override
                            public void add(int positionOffset, IntVector groupIds) {
                                groupIds.incRef();
                                ords1[0] = groupIds;
                            }

                            @Override
                            public void close() {

                            }
                        });
                        IntVector[] ords2 = new IntVector[1];
                        hash2.add(page, new GroupingAggregatorFunction.AddInput() {
                            private void addBlock(int positionOffset, IntBlock groupIds) {
                                // TODO: check why PackedValuesBlockHash doesn't emit a vector?
                                IntVector vector = groupIds.asVector();
                                assertNotNull("should emit a vector", vector);
                                vector.incRef();
                                ords2[0] = vector;
                            }

                            @Override
                            public void add(int positionOffset, IntArrayBlock groupIds) {
                                addBlock(positionOffset, groupIds);
                            }

                            @Override
                            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                                addBlock(positionOffset, groupIds);
                            }

                            @Override
                            public void add(int positionOffset, IntVector groupIds) {
                                groupIds.incRef();
                                ords2[0] = groupIds;
                            }

                            @Override
                            public void close() {

                            }
                        });
                        try {
                            assertThat("input=" + page, ords1[0], equalTo(ords2[0]));
                        } finally {
                            Releasables.close(ords1[0], ords2[0]);
                        }
                    }
                }
            }
            Block[] keys1 = null;
            Block[] keys2 = null;
            try (IntVector nonEmpty1 = hash1.nonEmpty(); IntVector nonEmpty2 = hash2.nonEmpty()) {
                keys1 = hash1.getKeys(nonEmpty1);
                keys2 = hash2.getKeys(nonEmpty2);
                assertThat(keys1, equalTo(keys2));
            } finally {
                Releasables.close(keys1);
                Releasables.close(keys2);
            }
        }
    }

    public void testTimeSeriesBlockHashReverseOutput() throws Exception {
        long endTime = randomLongBetween(10_000_000, 20_000_000);
        var hash1 = new TimeSeriesBlockHash(0, 1, false, randomBoolean(), blockFactory);
        var hash2 = new TimeSeriesBlockHash(0, 1, true, randomBoolean(), blockFactory);
        int numPages = between(1, 100);
        int globalTsid = -1;
        long timestamp = endTime;
        try (hash1; hash2) {
            for (int p = 0; p < numPages; p++) {
                int numRows = between(1, 1000);
                if (randomBoolean()) {
                    timestamp -= between(0, 100);
                }
                try (
                    BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(numRows);
                    IntVector.Builder ordinalBuilder = blockFactory.newIntVectorBuilder(numRows);
                    LongVector.Builder timestampsBuilder = blockFactory.newLongVectorBuilder(numRows)
                ) {
                    int perPageOrd = -1;
                    for (int i = 0; i < numRows; i++) {
                        boolean newGroup = globalTsid == -1 || randomInt(100) < 10;
                        if (newGroup) {
                            globalTsid++;
                            timestamp = endTime;
                            if (randomBoolean()) {
                                timestamp -= between(0, 1000);
                            }
                        }
                        if (perPageOrd == -1 || newGroup) {
                            perPageOrd++;
                            dictBuilder.appendBytesRef(new BytesRef(String.format(Locale.ROOT, "id-%06d", globalTsid)));
                        }
                        ordinalBuilder.appendInt(perPageOrd);
                        if (randomInt(100) < 20) {
                            timestamp -= between(1, 10);
                        }
                        timestampsBuilder.appendLong(timestamp);
                    }
                    try (
                        var tsidBlock = new OrdinalBytesRefVector(ordinalBuilder.build(), dictBuilder.build()).asBlock();
                        var timestampBlock = timestampsBuilder.build().asBlock()
                    ) {
                        Page page = new Page(tsidBlock, timestampBlock);
                        IntVector[] ords1 = new IntVector[1];
                        hash1.add(page, new GroupingAggregatorFunction.AddInput() {
                            @Override
                            public void add(int positionOffset, IntArrayBlock groupIds) {
                                throw new AssertionError("time-series block hash should emit a vector");
                            }

                            @Override
                            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                                throw new AssertionError("time-series block hash should emit a vector");
                            }

                            @Override
                            public void add(int positionOffset, IntVector groupIds) {
                                groupIds.incRef();
                                ords1[0] = groupIds;
                            }

                            @Override
                            public void close() {

                            }
                        });
                        IntVector[] ords2 = new IntVector[1];
                        hash2.add(page, new GroupingAggregatorFunction.AddInput() {
                            @Override
                            public void add(int positionOffset, IntArrayBlock groupIds) {
                                throw new AssertionError("time-series block hash should emit a vector");
                            }

                            @Override
                            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                                throw new AssertionError("time-series block hash should emit a vector");
                            }

                            @Override
                            public void add(int positionOffset, IntVector groupIds) {
                                groupIds.incRef();
                                ords2[0] = groupIds;
                            }

                            @Override
                            public void close() {

                            }
                        });
                        try {
                            assertThat("input=" + page, ords1[0], equalTo(ords2[0]));
                        } finally {
                            Releasables.close(ords1[0], ords2[0]);
                        }
                    }
                }
            }
            Block[] keys1 = null;
            Block[] keys2 = null;
            try (IntVector nonEmpty1 = hash1.nonEmpty(); IntVector nonEmpty2 = hash2.nonEmpty()) {
                keys1 = hash1.getKeys(nonEmpty1);
                keys2 = hash2.getKeys(nonEmpty2);
                // reverseOutput should swap the two key blocks
                assertThat(keys1.length, equalTo(2));
                assertThat(keys2.length, equalTo(2));
                assertThat(keys1[0], equalTo(keys2[1]));
                assertThat(keys1[1], equalTo(keys2[0]));
            } finally {
                Releasables.close(keys1);
                Releasables.close(keys2);
            }
        }
    }

    /**
     * The page-local tsid dictionary built for a chunked page must contain each referenced tsid exactly once, even when
     * the tsids referenced by the page are not contiguous by group id (which happens, for example, for groups added by
     * window expansion). This verifies the ordinals are remapped through a lookup rather than relying on the previous
     * ordinal of the iteration.
     */
    public void testTimeSeriesBlockHashChunkedPageDeduplicatesUnsortedTsids() {
        BytesRef tsidA = new BytesRef("id-a");
        BytesRef tsidB = new BytesRef("id-b");
        int pairs = 10;
        try (var hash = new TimeSeriesBlockHash(0, 1, false, false, blockFactory)) {
            // Interleave two tsids across single-row pages so the tsid ordinals alternate by group id (A, B, A, B, ...).
            for (int t = 0; t < pairs; t++) {
                addSingleTsidRow(hash, tsidA, t);
                addSingleTsidRow(hash, tsidB, t);
            }
            int totalGroups = 2 * pairs;
            int pageSize = totalGroups - 1; // a strict subset, so we take the chunked page-local dictionary path
            Block[] keys = null;
            try (IntVector selected = blockFactory.newIntRangeVector(0, pageSize)) {
                keys = hash.getKeys(selected);
                BytesRefBlock tsids = (BytesRefBlock) keys[0];
                BytesRef scratch = new BytesRef();
                for (int p = 0; p < pageSize; p++) {
                    assertThat(tsids.getBytesRef(p, scratch), equalTo(p % 2 == 0 ? tsidA : tsidB));
                }
                OrdinalBytesRefBlock ordinals = tsids.asOrdinals();
                assertNotNull("a dense page should be ordinal-encoded", ordinals);
                assertThat(
                    "page-local dictionary must hold each referenced tsid once",
                    ordinals.getDictionaryVector().getPositionCount(),
                    equalTo(2)
                );
            } finally {
                Releasables.close(keys);
            }
        }
    }

    private void addSingleTsidRow(TimeSeriesBlockHash hash, BytesRef tsid, long timestamp) {
        try (
            BytesRefVector.Builder tsidBuilder = blockFactory.newBytesRefVectorBuilder(1);
            LongVector.Builder timestampBuilder = blockFactory.newLongVectorBuilder(1)
        ) {
            tsidBuilder.appendBytesRef(tsid);
            timestampBuilder.appendLong(timestamp);
            try (var tsidBlock = tsidBuilder.build().asBlock(); var timestampBlock = timestampBuilder.build().asBlock()) {
                hash.add(new Page(tsidBlock, timestampBlock), new GroupingAggregatorFunction.AddInput() {
                    @Override
                    public void add(int positionOffset, IntArrayBlock groupIds) {}

                    @Override
                    public void add(int positionOffset, IntBigArrayBlock groupIds) {}

                    @Override
                    public void add(int positionOffset, IntVector groupIds) {}

                    @Override
                    public void close() {}
                });
            }
        }
    }

    /**
     * Hash some values into a single block of group ids. If the hash produces
     * more than one block of group ids this will fail.
     */
    private void hash(Consumer<OrdsAndKeys> callback, Block.Builder... values) {
        hash(callback, Block.Builder.buildAll(values));
    }

    /**
     * Hash some values into a single block of group ids. If the hash produces
     * more than one block of group ids this will fail.
     */
    private void hash(Consumer<OrdsAndKeys> callback, Block... values) {
        boolean[] called = new boolean[] { false };
        try (BlockHash hash = buildBlockHash(16 * 1024, values)) {
            hash(true, hash, ordsAndKeys -> {
                if (called[0]) {
                    throw new IllegalStateException("hash produced more than one block");
                }
                called[0] = true;
                callback.accept(ordsAndKeys);
                if (hash instanceof LongLongBlockHash == false
                    && hash instanceof BytesRefLongBlockHash == false
                    && hash instanceof BytesRef3BlockHash == false) {
                    try (ReleasableIterator<IntBlock> lookup = hash.lookup(new Page(values), ByteSizeValue.ofKb(between(1, 100)))) {
                        assertThat(lookup.hasNext(), equalTo(true));
                        try (IntBlock ords = lookup.next()) {
                            assertThat(ords, equalTo(ordsAndKeys.ords()));
                        }
                    }
                }
            }, values);
        } finally {
            Releasables.close(values);
        }
    }

    private void hash(Consumer<OrdsAndKeys> callback, int emitBatchSize, Block.Builder... values) {
        Block[] blocks = Block.Builder.buildAll(values);
        try (BlockHash hash = buildBlockHash(emitBatchSize, blocks)) {
            hash(true, hash, callback, blocks);
        } finally {
            Releasables.closeExpectNoException(blocks);
        }
    }

    private BlockHash buildBlockHash(int emitBatchSize, Block... values) {
        List<BlockHash.GroupSpec> specs = new ArrayList<>(values.length);
        for (int c = 0; c < values.length; c++) {
            specs.add(new BlockHash.GroupSpec(c, values[c].elementType()));
        }
        return forcePackedHash
            ? new PackedValuesBlockHash(specs, blockFactory, emitBatchSize)
            : BlockHash.build(specs, blockFactory, emitBatchSize, true);
    }

    public void testConstant() {
        try (
            var hash = new BytesRefLongBlockHash(blockFactory, 0, 1, false, randomIntBetween(1, 1000));
            var bytesHash = new BytesRefBlockHash(0, blockFactory)
        ) {
            int iters = between(1, 20);
            for (int i = 0; i < iters; i++) {
                final BytesRefBlock bytes;
                final LongBlock longs;
                final boolean constantInput = randomBoolean();
                final int positions = randomIntBetween(1, 100);
                if (constantInput) {
                    bytes = blockFactory.newConstantBytesRefBlockWith(new BytesRef(randomAlphaOfLength(10)), positions);
                    if (randomBoolean()) {
                        try (IntVector hashIds = bytesHash.add(bytes.asVector())) {
                            assertTrue(hashIds.isConstant());
                        }
                    }
                    if (randomBoolean()) {
                        longs = blockFactory.newConstantLongBlockWith(randomNonNegativeLong(), positions);
                    } else {
                        long value = randomNonNegativeLong();
                        try (var builder = blockFactory.newLongVectorFixedBuilder(positions)) {
                            for (int p = 0; p < positions; p++) {
                                builder.appendLong(value);
                            }
                            longs = builder.build().asBlock();
                        }
                    }
                } else {
                    try (var builder = blockFactory.newBytesRefBlockBuilder(positions)) {
                        for (int p = 0; p < positions; p++) {
                            builder.appendBytesRef(new BytesRef(randomAlphaOfLength(10)));
                        }
                        bytes = builder.build();
                    }
                    try (var builder = blockFactory.newLongVectorFixedBuilder(positions)) {
                        for (int p = 0; p < positions; p++) {
                            builder.appendLong(randomNonNegativeLong());
                        }
                        longs = builder.build().asBlock();
                    }
                }
                try (Page page = new Page(bytes, longs)) {
                    hash.add(page, new GroupingAggregatorFunction.AddInput() {
                        @Override
                        public void add(int positionOffset, IntArrayBlock groupIds) {
                            fail("should not call IntArrayBlock");
                        }

                        @Override
                        public void add(int positionOffset, IntBigArrayBlock groupIds) {
                            fail("should not call IntBigArrayBlock");
                        }

                        @Override
                        public void add(int positionOffset, IntVector groupIds) {
                            if (constantInput) {
                                assertTrue(groupIds.isConstant());
                            }
                        }

                        @Override
                        public void close() {

                        }
                    });
                }
            }
        }
    }
}
