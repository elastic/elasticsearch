/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** The bytes a block's values spell. It is counted rather than estimated, so these compare it against the values that went in. */
public class ValueBytesTests extends ESTestCase {

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    public void testTheLengthsOfTheValuesThatWentIn() {
        final String[] values = { "a", "bb", "", "four", "café" };
        long expected = 0;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(values.length)) {
            for (String value : values) {
                final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                expected += bytes.length;
                builder.appendBytesRef(new BytesRef(bytes));
            }
            try (BytesRefBlock block = builder.build()) {
                assertEquals(expected, ValueBytes.of(block));
            }
        }
    }

    public void testEveryValueOfAPositionCounts() {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("one"));
            builder.appendBytesRef(new BytesRef("two"));
            builder.endPositionEntry();
            builder.appendBytesRef(new BytesRef("three"));
            try (BytesRefBlock block = builder.build()) {
                assertEquals(3 + 3 + 5, ValueBytes.of(block));
            }
        }
    }

    public void testAPositionHoldingNothingSpellsNothing() {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.appendNull();
            builder.appendBytesRef(new BytesRef("four"));
            try (BytesRefBlock block = builder.build()) {
                assertEquals(4, ValueBytes.of(block));
            }
        }
        try (BytesRefBlock block = (BytesRefBlock) blockFactory.newConstantNullBlock(7)) {
            assertEquals(0, ValueBytes.of(block));
        }
    }

    public void testAValueOfAFixedWidthIsCountedByIt() {
        try (IntBlock block = blockFactory.newIntArrayVector(new int[] { 1, 2, 3 }, 3).asBlock()) {
            assertEquals(3 * Integer.BYTES, ValueBytes.of(block));
        }
        try (LongBlock block = blockFactory.newLongArrayVector(new long[] { 1, 2 }, 2).asBlock()) {
            assertEquals(2 * Long.BYTES, ValueBytes.of(block));
        }
        try (BooleanBlock block = blockFactory.newBooleanArrayVector(new boolean[] { true, false }, 2).asBlock()) {
            assertEquals(2 * Byte.BYTES, ValueBytes.of(block));
        }
    }

    public void testAColumnReadAgainstADictionarySpellsTheTermsItsPositionsName() {
        final BytesRef[] terms = { new BytesRef("alpha"), new BytesRef("be"), new BytesRef("") };
        final int[] named = { 0, 1, 1, 2, 0 };
        final BytesRefVector dictionary = blockFactory.newBytesRefArrayVector(bytesRefArray(terms), terms.length);
        final IntVector ordinals = blockFactory.newIntArrayVector(named, named.length);
        // The vector takes the two over, so closing the block is what releases them.
        try (BytesRefBlock block = new OrdinalBytesRefVector(ordinals, dictionary).asBlock()) {
            assertEquals(5 + 2 + 2 + 0 + 5, ValueBytes.of(block));
        }
    }

    /**
     * A dictionary can hold a whole segment's terms while a block names a few of them. The terms the page does not
     * name are then not read, and the volume comes out the same.
     */
    public void testADictionaryOfMoreTermsThanTheBlockNamesIsNotMeasured() {
        final int terms = 1000;
        final BytesRef[] dictionaryTerms = new BytesRef[terms];
        for (int term = 0; term < terms; term++) {
            dictionaryTerms[term] = new BytesRef("term" + term);
        }
        final int[] named = { 7, 7, 999 };
        final BytesRefVector dictionary = blockFactory.newBytesRefArrayVector(bytesRefArray(dictionaryTerms), terms);
        final IntVector ordinals = blockFactory.newIntArrayVector(named, named.length);
        try (BytesRefBlock block = new OrdinalBytesRefVector(ordinals, dictionary).asBlock()) {
            long expected = 0;
            for (int ordinal : named) {
                expected += dictionaryTerms[ordinal].length;
            }
            assertEquals(expected, ValueBytes.of(block));
        }
    }

    /**
     * Whatever shape a column arrives in, the volume is what its values spell. A shape that answers for all of its
     * values at once is taken at its word, so it has to agree with walking them.
     */
    public void testEveryShapeOfAColumnSpellsTheSame() {
        final RandomBlock random = RandomBlock.randomBlock(
            blockFactory,
            ElementType.BYTES_REF,
            between(1, 1000),
            randomBoolean(),
            0,
            between(1, 3),
            0,
            0 // a repeated value is recorded as one of them, and this counts the values that went in
        );
        try (Block block = random.block()) {
            long expected = 0;
            for (List<Object> position : random.values()) {
                if (position == null) {
                    continue;
                }
                for (Object value : position) {
                    expected += ((BytesRef) value).length;
                }
            }
            assertEquals(expected, ValueBytes.of(block));
            // The same values read as one of them a position, which is a different shape of the same volume.
            try (Block expanded = block.expand()) {
                assertEquals(expected, ValueBytes.of(expanded));
            }
        }
    }

    private BytesRefArray bytesRefArray(BytesRef[] terms) {
        final BytesRefArray array = new BytesRefArray(terms.length, BigArrays.NON_RECYCLING_INSTANCE);
        for (BytesRef term : terms) {
            array.append(term);
        }
        return array;
    }
}
