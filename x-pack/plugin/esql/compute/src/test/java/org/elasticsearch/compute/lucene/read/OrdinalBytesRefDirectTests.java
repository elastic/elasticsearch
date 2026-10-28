/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The block a page of a string column becomes when it arrives already named by ordinals. Every shape a page can
 * take is checked against the values the positions should hold, since the builder is what turns one into the
 * other and nothing else reads the page.
 */
public class OrdinalBytesRefDirectTests extends ComputeTestCase {

    public void testOneValueAPosition() {
        assertBlock(
            new int[] { 0, 1, 0, 2 },
            4,
            null,
            4,
            new String[] { "alpha", "bravo", "charlie" },
            List.of(List.of("alpha"), List.of("bravo"), List.of("alpha"), List.of("charlie"))
        );
    }

    public void testSeveralValuesAPosition() {
        assertBlock(
            new int[] { 0, 1, 2, 0 },
            4,
            new int[] { 2, 1, 1 },
            3,
            new String[] { "alpha", "bravo", "charlie" },
            List.of(List.of("alpha", "bravo"), List.of("charlie"), List.of("alpha"))
        );
    }

    /** A position holding nothing, which a document whose every slot is null gives. */
    public void testPositionsHoldingNothing() {
        assertBlock(
            new int[] { 0, 1 },
            2,
            new int[] { 1, 0, 1, 0 },
            4,
            new String[] { "alpha", "bravo" },
            Arrays.asList(List.of("alpha"), null, List.of("bravo"), null)
        );
    }

    /** No value in the whole page, so there is nothing for the dictionary to hold either. */
    public void testNoValuesAtAll() {
        assertBlock(new int[0], 0, new int[] { 0, 0, 0 }, 3, new String[0], Arrays.asList(null, null, null));
    }

    /** One distinct value named by every position, the shape a column in term order gives. */
    public void testOneDistinctValue() {
        assertBlock(
            new int[] { 0, 0, 0, 0, 0 },
            5,
            null,
            5,
            new String[] { "only" },
            List.of(List.of("only"), List.of("only"), List.of("only"), List.of("only"), List.of("only"))
        );
    }

    private void assertBlock(
        int[] ordinals,
        int valueCount,
        int[] valueCounts,
        int positionCount,
        String[] dictionary,
        List<List<String>> expected
    ) {
        final BlockFactory factory = blockFactory();
        final DelegatingBlockLoaderFactory loaderFactory = new DelegatingBlockLoaderFactory(factory) {
            @Override
            public Block constantNulls(int count) {
                return factory.newConstantNullBlock(count);
            }
        };
        final BytesRef[] terms = new BytesRef[dictionary.length];
        for (int i = 0; i < dictionary.length; i++) {
            terms[i] = new BytesRef(dictionary[i]);
        }
        try (
            Block block = (Block) loaderFactory.buildOrdinalBytesRefDirect(
                ordinals,
                valueCount,
                valueCounts,
                positionCount,
                terms,
                terms.length
            )
        ) {
            assertEquals("positions", positionCount, block.getPositionCount());
            final BytesRefBlock bytes = (BytesRefBlock) block;
            final BytesRef scratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                if (expected.get(p) == null) {
                    assertTrue("position " + p + " holds nothing", bytes.isNull(p));
                    continue;
                }
                assertFalse("position " + p + " holds something", bytes.isNull(p));
                final List<String> held = new ArrayList<>();
                final int first = bytes.getFirstValueIndex(p);
                for (int v = 0; v < bytes.getValueCount(p); v++) {
                    held.add(bytes.getBytesRef(first + v, scratch).utf8ToString());
                }
                assertEquals("position " + p, expected.get(p), held);
            }
        }
    }
}
