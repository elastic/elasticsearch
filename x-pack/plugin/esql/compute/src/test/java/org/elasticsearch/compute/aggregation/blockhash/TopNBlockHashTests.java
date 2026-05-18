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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class TopNBlockHashTests extends BlockHashTestCase {

    private static final int LIMIT_TWO = 2;
    private static final int LIMIT_HIGH = 10000;

    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        // TODO: Uncomment this "true" when implemented
        for (boolean forcePackedHash : new boolean[] { /*true,*/false }) {
            for (boolean asc : new boolean[] { true, false }) {
                for (boolean nullsFirst : new boolean[] { true, false }) {
                    for (int limit : new int[] { LIMIT_TWO, LIMIT_HIGH }) {
                        params.add(new Object[] { forcePackedHash, asc, nullsFirst, limit });
                    }
                }
            }
        }

        return params;
    }

    private final boolean forcePackedHash;
    private final boolean asc;
    private final boolean nullsFirst;
    private final int limit;

    public TopNBlockHashTests(
        @Name("forcePackedHash") boolean forcePackedHash,
        @Name("asc") boolean asc,
        @Name("nullsFirst") boolean nullsFirst,
        @Name("limit") int limit
    ) {
        this.forcePackedHash = forcePackedHash;
        this.asc = asc;
        this.nullsFirst = nullsFirst;
        this.limit = limit;
    }

    public void testLongHash() {
        long[] values = new long[] { 2, 1, 4, 2, 4, 1, 3, 4 };

        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                // TODO: Not tested yet
            } else {
                assertThat(
                    ordsAndKeys.description(),
                    equalTo("LongTopNBlockHash{channel=0, " + topNParametersString(4, 0) + ", hasNull=false}")
                );
                if (limit == LIMIT_HIGH) {
                    assertKeys(ordsAndKeys.keys(), 2L, 1L, 4L, 3L);
                    assertOrds(ordsAndKeys.ords(), 1, 2, 3, 1, 3, 2, 4, 3);
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(1, 5)));
                } else {
                    if (asc) {
                        assertKeys(ordsAndKeys.keys(), 2L, 1L);
                        assertOrds(ordsAndKeys.ords(), 1, 2, null, 1, null, 2, null, null);
                        assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                    } else {
                        assertKeys(ordsAndKeys.keys(), 4L, 3L);
                        assertOrds(ordsAndKeys.ords(), null, null, 1, null, 1, null, 2, 1);
                        assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                    }
                }
            }
        }, blockFactory.newLongArrayVector(values, values.length).asBlock());
    }

    public void testLongHashBatched() {
        long[][] arrays = { new long[] { 2, 1, 4, 2 }, new long[] { 4, 1, 3, 4 } };

        hashBatchesCallbackOnLast(ordsAndKeys -> {
            if (forcePackedHash) {
                // TODO: Not tested yet
            } else {
                assertThat(
                    ordsAndKeys.description(),
                    equalTo("LongTopNBlockHash{channel=0, " + topNParametersString(4, asc ? 0 : 1) + ", hasNull=false}")
                );
                if (limit == LIMIT_HIGH) {
                    assertKeys(ordsAndKeys.keys(), 2L, 1L, 4L, 3L);
                    assertOrds(ordsAndKeys.ords(), 3, 2, 4, 3);
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(1, 5)));
                } else {
                    if (asc) {
                        assertKeys(ordsAndKeys.keys(), 2L, 1L);
                        assertOrds(ordsAndKeys.ords(), null, 2, null, null);
                        assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                    } else {
                        assertKeys(ordsAndKeys.keys(), 4L, 3L);
                        assertOrds(ordsAndKeys.ords(), 2, null, 3, 2);
                        assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(2, 3)));
                    }
                }
            }
        },
            Arrays.stream(arrays)
                .map(array -> new Block[] { blockFactory.newLongArrayVector(array, array.length).asBlock() })
                .toArray(Block[][]::new)
        );
    }

    public void testLongHashWithNulls() {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(4)) {
            builder.appendLong(0);
            builder.appendNull();
            builder.appendLong(2);
            builder.appendNull();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    // TODO: Not tested yet
                } else {
                    boolean hasTwoNonNullValues = nullsFirst == false || limit == LIMIT_HIGH;
                    boolean hasNull = nullsFirst || limit == LIMIT_HIGH;
                    assertThat(
                        ordsAndKeys.description(),
                        equalTo(
                            "LongTopNBlockHash{channel=0, "
                                + topNParametersString(hasTwoNonNullValues ? 2 : 1, 0)
                                + ", hasNull="
                                + hasNull
                                + "}"
                        )
                    );
                    if (limit == LIMIT_HIGH) {
                        assertKeys(ordsAndKeys.keys(), null, 0L, 2L);
                        assertOrds(ordsAndKeys.ords(), 1, 0, 2, 0);
                        assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1, 2)));
                    } else {
                        if (nullsFirst) {
                            if (asc) {
                                assertKeys(ordsAndKeys.keys(), null, 0L);
                                assertOrds(ordsAndKeys.ords(), 1, 0, null, 0);
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1)));
                            } else {
                                assertKeys(ordsAndKeys.keys(), null, 2L);
                                assertOrds(ordsAndKeys.ords(), null, 0, 1, 0);
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1)));
                            }
                        } else {
                            assertKeys(ordsAndKeys.keys(), 0L, 2L);
                            assertOrds(ordsAndKeys.ords(), 1, null, 2, null);
                            assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                        }
                    }
                }
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
                    // TODO: Not tested yet
                } else {
                    if (limit == LIMIT_HIGH) {
                        assertThat(
                            ordsAndKeys.description(),
                            equalTo("LongTopNBlockHash{channel=0, " + topNParametersString(3, 0) + ", hasNull=true}")
                        );
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
                    } else {
                        assertThat(
                            ordsAndKeys.description(),
                            equalTo(
                                "LongTopNBlockHash{channel=0, "
                                    + topNParametersString(nullsFirst ? 1 : 2, 0)
                                    + ", hasNull="
                                    + nullsFirst
                                    + "}"
                            )
                        );
                        if (nullsFirst) {
                            if (asc) {
                                assertKeys(ordsAndKeys.keys(), null, 1L);
                                assertOrds(
                                    ordsAndKeys.ords(),
                                    new int[] { 1 },
                                    new int[] { 1 },
                                    new int[] { 1 },
                                    null,
                                    new int[] { 0 },
                                    new int[] { 1 }
                                );
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1)));
                            } else {
                                assertKeys(ordsAndKeys.keys(), null, 3L);
                                assertOrds(
                                    ordsAndKeys.ords(),
                                    null,
                                    new int[] { 1 },
                                    null,
                                    new int[] { 1 },
                                    new int[] { 0 },
                                    new int[] { 1 }
                                );
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1)));
                            }
                        } else {
                            if (asc) {
                                assertKeys(ordsAndKeys.keys(), 1L, 2L);
                                assertOrds(
                                    ordsAndKeys.ords(),
                                    new int[] { 1 },
                                    new int[] { 1, 2 },
                                    new int[] { 1 },
                                    null,
                                    null,
                                    new int[] { 2, 1 }
                                );
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                            } else {
                                assertKeys(ordsAndKeys.keys(), 2L, 3L);
                                assertOrds(ordsAndKeys.ords(), null, new int[] { 1, 2 }, null, new int[] { 2 }, null, new int[] { 2, 1 });
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                            }
                        }
                    }
                }
            }, builder);
        }
    }

    public void testBytesRefHash() {
        String[] values = { "b", "a", "d", "b", "d", "a", "c", "d" };

        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                // TODO: Not tested yet
            } else {
                assertThat(
                    ordsAndKeys.description(),
                    equalTo("BytesRefTopNBlockHash{channel=0, " + topNParametersString(4, 0) + ", hasNull=false}")
                );
                if (limit == LIMIT_HIGH) {
                    assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { "b", "a", "d", "c" });
                    assertOrds(ordsAndKeys.ords(), 1, 2, 3, 1, 3, 2, 4, 3);
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(1, 5)));
                } else {
                    if (asc) {
                        assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { "b", "a" });
                        assertOrds(ordsAndKeys.ords(), 1, 2, null, 1, null, 2, null, null);
                        assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                    } else {
                        assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { "d", "c" });
                        assertOrds(ordsAndKeys.ords(), null, null, 1, null, 1, null, 2, 1);
                        assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                    }
                }
            }
        }, bytesRefBlock(values));
    }

    public void testBytesRefHashWithNulls() {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(4)) {
            builder.appendBytesRef(new BytesRef("a"));
            builder.appendNull();
            builder.appendBytesRef(new BytesRef("c"));
            builder.appendNull();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    // TODO: Not tested yet
                } else {
                    boolean hasTwoNonNullValues = nullsFirst == false || limit == LIMIT_HIGH;
                    boolean hasNull = nullsFirst || limit == LIMIT_HIGH;
                    assertThat(
                        ordsAndKeys.description(),
                        equalTo(
                            "BytesRefTopNBlockHash{channel=0, "
                                + topNParametersString(hasTwoNonNullValues ? 2 : 1, 0)
                                + ", hasNull="
                                + hasNull
                                + "}"
                        )
                    );
                    if (limit == LIMIT_HIGH) {
                        assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { null, "a", "c" });
                        assertOrds(ordsAndKeys.ords(), 1, 0, 2, 0);
                        assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1, 2)));
                    } else {
                        if (nullsFirst) {
                            if (asc) {
                                assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { null, "a" });
                                assertOrds(ordsAndKeys.ords(), 1, 0, null, 0);
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1)));
                            } else {
                                assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { null, "c" });
                                assertOrds(ordsAndKeys.ords(), null, 0, 1, 0);
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1)));
                            }
                        } else {
                            assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { "a", "c" });
                            assertOrds(ordsAndKeys.ords(), 1, null, 2, null);
                            assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                        }
                    }
                }
            }, builder);
        }
    }

    public void testBytesRefHashWithMultiValuedFields() {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(8)) {
            builder.appendBytesRef(new BytesRef("a"));
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("a"));
            builder.appendBytesRef(new BytesRef("b"));
            builder.appendBytesRef(new BytesRef("c"));
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("a"));
            builder.appendBytesRef(new BytesRef("a"));
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("c"));
            builder.endPositionEntry();
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("c"));
            builder.appendBytesRef(new BytesRef("b"));
            builder.appendBytesRef(new BytesRef("a"));
            builder.endPositionEntry();

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    // TODO: Not tested yet
                } else {
                    if (limit == LIMIT_HIGH) {
                        assertThat(
                            ordsAndKeys.description(),
                            equalTo("BytesRefTopNBlockHash{channel=0, " + topNParametersString(3, 0) + ", hasNull=true}")
                        );
                        assertOrds(
                            ordsAndKeys.ords(),
                            new int[] { 1 },
                            new int[] { 1, 2, 3 },
                            new int[] { 1 },
                            new int[] { 3 },
                            new int[] { 0 },
                            new int[] { 3, 2, 1 }
                        );
                        assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { null, "a", "b", "c" });
                    } else {
                        assertThat(
                            ordsAndKeys.description(),
                            equalTo(
                                "BytesRefTopNBlockHash{channel=0, "
                                    + topNParametersString(nullsFirst ? 1 : 2, 0)
                                    + ", hasNull="
                                    + nullsFirst
                                    + "}"
                            )
                        );
                        if (nullsFirst) {
                            if (asc) {
                                assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { null, "a" });
                                assertOrds(
                                    ordsAndKeys.ords(),
                                    new int[] { 1 },
                                    new int[] { 1 },
                                    new int[] { 1 },
                                    null,
                                    new int[] { 0 },
                                    new int[] { 1 }
                                );
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1)));
                            } else {
                                assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { null, "c" });
                                assertOrds(
                                    ordsAndKeys.ords(),
                                    null,
                                    new int[] { 1 },
                                    null,
                                    new int[] { 1 },
                                    new int[] { 0 },
                                    new int[] { 1 }
                                );
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1)));
                            }
                        } else {
                            if (asc) {
                                assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { "a", "b" });
                                assertOrds(
                                    ordsAndKeys.ords(),
                                    new int[] { 1 },
                                    new int[] { 1, 2 },
                                    new int[] { 1 },
                                    null,
                                    null,
                                    new int[] { 2, 1 }
                                );
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                            } else {
                                assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { "b", "c" });
                                assertOrds(ordsAndKeys.ords(), null, new int[] { 1, 2 }, null, new int[] { 2 }, null, new int[] { 2, 1 });
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                            }
                        }
                    }
                }
            }, builder);
        }
    }

    public void testBytesRefOrdinalsHash() {
        // Same logical input as testBytesRefHash ("b","a","d","b","d","a","c","d") but fed via an
        // OrdinalBytesRefVector. The TopN groupings (which keys survive, which rows hit which group) match the
        // plain path; the only observable difference is that group ids follow the *dictionary* order rather than
        // the per-row first-encountered order — which is the same convention {@link BytesRefBlockHash} uses for
        // its ordinal fast path (see {@code BlockHashTests.testBasicOrdinals}).
        String[] dictionary = { "a", "b", "c", "d" };
        int[] ordinals = { 1, 0, 3, 1, 3, 0, 2, 3 };

        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                // TODO: Not tested yet
            } else {
                assertThat(
                    ordsAndKeys.description(),
                    equalTo("BytesRefTopNBlockHash{channel=0, " + topNParametersString(4, 0) + ", hasNull=false}")
                );
                if (limit == LIMIT_HIGH) {
                    // Dictionary order: a=1, b=2, c=3, d=4
                    assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { "a", "b", "c", "d" });
                    assertOrds(ordsAndKeys.ords(), 2, 1, 4, 2, 4, 1, 3, 4);
                    assertThat(ordsAndKeys.nonEmpty(), equalTo(intRange(1, 5)));
                } else {
                    if (asc) {
                        // Top 2 ascending: a, b. Dictionary order assigns a=1, b=2.
                        assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { "a", "b" });
                        assertOrds(ordsAndKeys.ords(), 2, 1, null, 2, null, 1, null, null);
                        assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                    } else {
                        // Top 2 descending: c, d. Dictionary order assigns c=1, d=2.
                        assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { "c", "d" });
                        assertOrds(ordsAndKeys.ords(), null, null, 2, null, 2, null, 1, 2);
                        assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                    }
                }
            }
        }, ordinalVector(ordinals, dictionary).asBlock());
    }

    public void testBytesRefOrdinalsBlockWithNulls() {
        // Mirrors testBytesRefHashWithNulls but the input is an OrdinalBytesRefBlock with nulls in the
        // ordinals IntBlock (the dictionary itself is null-free and shared).
        String[] dictionary = { "a", "c" };
        // null positions are encoded as nulls in the ordinals IntBlock; non-null positions reference the dictionary.
        try (
            IntBlock.Builder ords = blockFactory.newIntBlockBuilder(4);
            BytesRefVector.Builder bytes = blockFactory.newBytesRefVectorBuilder(dictionary.length)
        ) {
            ords.appendInt(0);
            ords.appendNull();
            ords.appendInt(1);
            ords.appendNull();
            for (String v : dictionary) {
                bytes.appendBytesRef(new BytesRef(v));
            }
            OrdinalBytesRefBlock block = new OrdinalBytesRefBlock(ords.build(), bytes.build());

            hash(ordsAndKeys -> {
                if (forcePackedHash) {
                    // TODO: Not tested yet
                } else {
                    boolean hasTwoNonNullValues = nullsFirst == false || limit == LIMIT_HIGH;
                    boolean hasNull = nullsFirst || limit == LIMIT_HIGH;
                    assertThat(
                        ordsAndKeys.description(),
                        equalTo(
                            "BytesRefTopNBlockHash{channel=0, "
                                + topNParametersString(hasTwoNonNullValues ? 2 : 1, 0)
                                + ", hasNull="
                                + hasNull
                                + "}"
                        )
                    );
                    if (limit == LIMIT_HIGH) {
                        assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { null, "a", "c" });
                        assertOrds(ordsAndKeys.ords(), 1, 0, 2, 0);
                        assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1, 2)));
                    } else {
                        if (nullsFirst) {
                            if (asc) {
                                assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { null, "a" });
                                assertOrds(ordsAndKeys.ords(), 1, 0, null, 0);
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1)));
                            } else {
                                assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { null, "c" });
                                assertOrds(ordsAndKeys.ords(), null, 0, 1, 0);
                                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(0, 1)));
                            }
                        } else {
                            assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { "a", "c" });
                            assertOrds(ordsAndKeys.ords(), 1, null, 2, null);
                            assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1, 2)));
                        }
                    }
                }
            }, block);
        }
    }

    /**
     * The dictionary contains entries that the ordinals never reference. A naive ordinal fast path that ranks the
     * full dictionary would let those phantom values into the TopN and silently corrupt the result. We feed
     * {@code "a", "a"} (the only referenced entry), keeping {@code "z"} in the dictionary as bait — the assertions
     * are identical to feeding the same logical values through the plain path.
     */
    public void testBytesRefOrdinalsHashUnreferencedDictEntries() {
        String[] dictionary = { "a", "z" };
        int[] ordinals = { 0, 0 };

        hash(ordsAndKeys -> {
            if (forcePackedHash) {
                // TODO: Not tested yet
            } else {
                assertThat(
                    ordsAndKeys.description(),
                    equalTo("BytesRefTopNBlockHash{channel=0, " + topNParametersString(1, 0) + ", hasNull=false}")
                );
                assertKeys(ordsAndKeys.keys(), (Object[]) new String[] { "a" });
                assertOrds(ordsAndKeys.ords(), 1, 1);
                assertThat(ordsAndKeys.nonEmpty(), equalTo(intVector(1)));
            }
        }, ordinalVector(ordinals, dictionary).asBlock());
    }

    private OrdinalBytesRefVector ordinalVector(int[] ordinals, String[] dictionary) {
        try (
            IntVector.Builder ords = blockFactory.newIntVectorFixedBuilder(ordinals.length);
            BytesRefVector.Builder bytes = blockFactory.newBytesRefVectorBuilder(dictionary.length)
        ) {
            for (int o : ordinals) {
                ords.appendInt(o);
            }
            for (String v : dictionary) {
                bytes.appendBytesRef(new BytesRef(v));
            }
            return new OrdinalBytesRefVector(ords.build(), bytes.build());
        }
    }

    private BytesRefBlock bytesRefBlock(String[] values) {
        try (BytesRefBlock.Builder b = blockFactory.newBytesRefBlockBuilder(values.length)) {
            for (String v : values) {
                if (v == null) {
                    b.appendNull();
                } else {
                    b.appendBytesRef(new BytesRef(v));
                }
            }
            return b.build();
        }
    }

    // TODO: Test adding multiple blocks, as it triggers different logics like:
    // - Keeping older unused ords
    // - Returning nonEmpty ords greater than 1

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
                try (ReleasableIterator<IntBlock> lookup = hash.lookup(new Page(values), ByteSizeValue.ofKb(between(1, 100)))) {
                    assertThat(lookup.hasNext(), equalTo(true));
                    try (IntBlock ords = lookup.next()) {
                        assertThat(ords, equalTo(ordsAndKeys.ords()));
                    }
                }
            }, values);
        } finally {
            Releasables.close(values);
        }
    }

    // TODO: Randomize this instead?
    /**
     * Hashes multiple separated batches of values.
     *
     * @param callback Callback with the OrdsAndKeys for the last batch
     */
    private void hashBatchesCallbackOnLast(Consumer<OrdsAndKeys> callback, Block[]... batches) {
        // Ensure all batches share the same specs
        assertThat(batches.length, greaterThan(0));
        for (Block[] batch : batches) {
            assertThat(batch.length, equalTo(batches[0].length));
            for (int i = 0; i < batch.length; i++) {
                assertThat(batches[0][i].elementType(), equalTo(batch[i].elementType()));
            }
        }

        boolean[] called = new boolean[] { false };
        try (BlockHash hash = buildBlockHash(16 * 1024, batches[0])) {
            for (Block[] batch : batches) {
                called[0] = false;
                hash(true, hash, ordsAndKeys -> {
                    if (called[0]) {
                        throw new IllegalStateException("hash produced more than one block");
                    }
                    called[0] = true;
                    if (batch == batches[batches.length - 1]) {
                        callback.accept(ordsAndKeys);
                    }
                    try (ReleasableIterator<IntBlock> lookup = hash.lookup(new Page(batch), ByteSizeValue.ofKb(between(1, 100)))) {
                        assertThat(lookup.hasNext(), equalTo(true));
                        try (IntBlock ords = lookup.next()) {
                            assertThat(ords, equalTo(ordsAndKeys.ords()));
                        }
                    }
                }, batch);
            }
        } finally {
            Releasables.close(Arrays.stream(batches).flatMap(Arrays::stream).toList());
        }
    }

    private BlockHash buildBlockHash(int emitBatchSize, Block... values) {
        List<BlockHash.GroupSpec> specs = new ArrayList<>(values.length);
        for (int c = 0; c < values.length; c++) {
            specs.add(new BlockHash.GroupSpec(c, values[c].elementType(), null, topNDef(c)));
        }
        assert forcePackedHash == false : "Packed TopN hash not implemented yet";
        /*return forcePackedHash
            ? new PackedValuesBlockHash(specs, blockFactory, emitBatchSize)
            : BlockHash.build(specs, blockFactory, emitBatchSize, true);*/

        BlockHash.GroupSpec spec = specs.get(0);
        if (spec.elementType() == ElementType.BYTES_REF) {
            return new BytesRefTopNBlockHash(spec.channel(), asc, nullsFirst, limit, blockFactory);
        }
        return new LongTopNBlockHash(spec.channel(), asc, nullsFirst, limit, blockFactory);
    }

    /**
     * Returns the common toString() part of the TopNBlockHash using the test parameters.
     */
    private String topNParametersString(int differentValues, int unusedInsertedValues) {
        return "asc="
            + asc
            + ", nullsFirst="
            + nullsFirst
            + ", limit="
            + limit
            + ", entries="
            + Math.min(differentValues, limit + unusedInsertedValues);
    }

    private BlockHash.TopNDef topNDef(int order) {
        return new BlockHash.TopNDef(order, asc, nullsFirst, limit);
    }
}
