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
import org.elasticsearch.xpack.esql.core.util.Holder;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
        int[] values1 = new int[] { 0, 1, 0, 1, 0, 1 };
        long[] values2 = new long[] { 0, 0, 0, 1, 1, 1 };
        Object[][] expectedKeys = { new Object[] { 0, 0L }, new Object[] { 1, 0L }, new Object[] { 1, 1L }, new Object[] { 0, 1L } };
        hash(ordsAndKeys -> {
            assertThat(ordsAndKeys.description(), startsWith("PackedValuesBlockHash{groups=[0:INT, 1:LONG], entries=4, size="));
            assertThat(ordsAndKeys.description(), endsWith("b}"));
            assertOrds(ordsAndKeys.ords(), 0, 1, 0, 2, 3, 2);
            assertKeys(ordsAndKeys.keys(), expectedKeys);
        },
            blockFactory.newIntArrayVector(values1, values1.length).asBlock(),
            blockFactory.newLongArrayVector(values2, values2.length).asBlock()
        );
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
                            : "BytesRefLongBlockHash{keys=[BytesRefKey[channel=1], LongKey[channel=0]], entries=4, size="
                    )
                );
                assertThat(ordsAndKeys.description(), endsWith("b}"));
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

            hash((OrdsAndKeys ordsAndKeys) -> {
                if (forcePackedHash) {
                    assertThat(
                        ordsAndKeys.description(),
                        startsWith("PackedValuesBlockHash{groups=[0:LONG, 1:BYTES_REF], entries=5, size=")
                    );
                    assertThat(ordsAndKeys.description(), endsWith("b}"));
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
                } else {
                    assertThat(
                        ordsAndKeys.description(),
                        startsWith("BytesRefLongBlockHash{keys=[BytesRefKey[channel=1], LongKey[channel=0]], entries=3, size=")
                    );
                    assertThat(ordsAndKeys.description(), endsWith("b}"));
                    assertOrds(ordsAndKeys.ords(), 0, null, 1, 2, null);
                    assertKeys(
                        ordsAndKeys.keys(),
                        new Object[][] { new Object[] { 1L, "cat" }, new Object[] { 0L, "dog" }, new Object[] { 0L, null } }
                    );
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 3)));
                }
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

            hash((OrdsAndKeys ordsAndKeys) -> {
                if (forcePackedHash) {
                    assertThat(
                        ordsAndKeys.description(),
                        startsWith("PackedValuesBlockHash{groups=[0:LONG, 1:BYTES_REF], entries=10, size=")
                    );
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
                } else {
                    assertThat(
                        ordsAndKeys.description(),
                        equalTo("BytesRefLongBlockHash{keys=[BytesRefKey[channel=1], LongKey[channel=0]], entries=9, size=483b}")
                    );
                    assertOrds(
                        ordsAndKeys.ords(),
                        new int[] { 0, 1, 2, 3 },
                        new int[] { 0, 1 },
                        new int[] { 0, 2 },
                        new int[] { 0 },
                        null,
                        new int[] { 4 },
                        new int[] { 0 },
                        new int[] { 0, 1, 2, 3 },
                        new int[] { 5, 6, 7, 0, 1, 8 }
                    );
                    assertKeys(
                        ordsAndKeys.keys(),
                        new Object[][] {
                            new Object[] { 1L, "a" },
                            new Object[] { 2L, "a" },
                            new Object[] { 1L, "b" },
                            new Object[] { 2L, "b" },
                            new Object[] { 1L, null },
                            new Object[] { 1L, "c" },
                            new Object[] { 2L, "c" },
                            new Object[] { 3L, "c" },
                            new Object[] { 3L, "a" }, }
                    );
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, 9)));
                }
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
            hash(ordsAndKeys -> {
                int start = expectedEntries[0];
                expectedEntries[0] = Math.min(expectedEntries[0] + pageSize, v1.length * v2.length);
                assertThat(
                    ordsAndKeys.description(),
                    forcePackedHash
                        ? startsWith("PackedValuesBlockHash{groups=[0:LONG, 1:BYTES_REF], entries=" + expectedEntries[0] + ", size=")
                        : startsWith(
                            "BytesRefLongBlockHash{keys=[BytesRefKey[channel=1], LongKey[channel=0]], entries="
                                + expectedEntries[0]
                                + ", size="
                        )
                );
                assertOrds(ordsAndKeys.ords(), IntStream.range(start, expectedEntries[0]).toArray());
                assertKeys(
                    ordsAndKeys.keys(),
                    IntStream.range(0, expectedEntries[0])
                        .mapToObj(
                            i -> forcePackedHash
                                ? new Object[] { v1[i / v2.length], v2[i % v2.length] }
                                : new Object[] { v1[i % v1.length], v2[i / v1.length] }
                        )
                        .toArray(l -> new Object[l][])
                );
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(0, expectedEntries[0])));
            }, pageSize, b1, b2);

            assertThat("misconfigured test", expectedEntries[0], greaterThan(0));
        }
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
            assertThat(output1.size(), equalTo(output1.size()));
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
        var hash1 = new TimeSeriesBlockHash(0, 1, blockFactory);
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
                        Holder<IntVector> ords1 = new Holder<>();
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
                                ords1.set(groupIds);
                            }

                            @Override
                            public void close() {

                            }
                        });
                        Holder<IntVector> ords2 = new Holder<>();
                        hash2.add(page, new GroupingAggregatorFunction.AddInput() {
                            private void addBlock(int positionOffset, IntBlock groupIds) {
                                // TODO: check why PackedValuesBlockHash doesn't emit a vector?
                                IntVector vector = groupIds.asVector();
                                assertNotNull("should emit a vector", vector);
                                vector.incRef();
                                ords2.set(vector);
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
                                ords2.set(groupIds);
                            }

                            @Override
                            public void close() {

                            }
                        });
                        try {
                            assertThat("input=" + page, ords1.get(), equalTo(ords2.get()));
                        } finally {
                            Releasables.close(ords1.get(), ords2.get());
                        }
                    }
                }
            }
            Block[] keys1 = null;
            Block[] keys2 = null;
            try {
                keys1 = hash1.getKeys();
                keys2 = hash2.getKeys();
                assertThat(keys1, equalTo(keys2));
            } finally {
                Releasables.close(keys1);
                Releasables.close(keys2);
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
}
