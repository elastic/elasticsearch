/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DictionaryOrdinalsTests extends BlockHashTestCase {

    public void testMarkReferencedVector() {
        try (IntVector ords = intVector(3, 1, 3, 1, 4)) {
            boolean[] referenced = new boolean[6];
            assertThat(DictionaryOrdinals.markReferenced(ords, referenced), equalTo(3));
            assertArrayEquals(new boolean[] { false, true, false, true, true, false }, referenced);
        }
    }

    /**
     * Null positions hold a placeholder value slot (ordinal 0 here); only the values of each position count.
     */
    public void testMarkReferencedBlock() {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(5)) {
            builder.appendInt(2);
            builder.appendNull();
            builder.beginPositionEntry().appendInt(1).appendInt(3).appendInt(1).endPositionEntry();
            builder.appendNull();
            builder.appendInt(2);
            try (IntBlock ords = builder.build()) {
                boolean[] referenced = new boolean[4];
                assertThat(DictionaryOrdinals.markReferenced(ords, referenced), equalTo(3));
                assertArrayEquals(new boolean[] { false, true, true, true }, referenced);
            }
        }
    }

    /**
     * {@link OrdinalBytesRefBlock#keepMask} keeps the dictionary; a masked-out position must not mark its ordinal.
     */
    public void testMarkReferencedMaskedBlock() {
        try (
            IntBlock.Builder ords = blockFactory.newIntBlockBuilder(4);
            BooleanVector.Builder maskBuilder = blockFactory.newBooleanVectorFixedBuilder(4)
        ) {
            ords.appendInt(1).appendInt(2).appendInt(1).appendInt(0);
            maskBuilder.appendBoolean(true).appendBoolean(true).appendBoolean(true).appendBoolean(false);
            try (
                OrdinalBytesRefBlock block = new OrdinalBytesRefBlock(ords.build(), bytesVector("ghost", "a", "b"));
                BooleanVector mask = maskBuilder.build();
                BytesRefBlock masked = block.keepMask(mask)
            ) {
                OrdinalBytesRefBlock maskedOrdinals = masked.asOrdinals();
                assertThat(maskedOrdinals, notNullValue());
                boolean[] referenced = new boolean[3];
                assertThat(DictionaryOrdinals.markReferenced(maskedOrdinals.getOrdinalsBlock(), referenced), equalTo(2));
                assertArrayEquals(new boolean[] { false, true, true }, referenced);
            }
        }
    }

    public void testReferencedValues() {
        boolean[] referenced = { false, true, false, true, true };
        try (
            BytesRefVector dictionary = bytesVector("d0", "d1", "d2", "d3", "d4");
            BytesRefVector values = DictionaryOrdinals.referencedValues(blockFactory, dictionary, referenced, 3)
        ) {
            assertThat(strings(values), equalTo(List.of("d1", "d3", "d4")));
        }
    }

    public void testDistinctVectorFirstSeenOrder() {
        // Ordinal 0 sits in the hash map's dedicated empty-key slot.
        int[] values = { 7, 0, 7, 7, 0, 5, 5, 7, 0, 5, 0, 7 };
        try (IntVector ords = intVector(values); BytesRefVector dictionary = bytesVector("d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7")) {
            DictionaryOrdinals.Distinct distinct = DictionaryOrdinals.distinct(ords);
            assertThat(distinct, notNullValue());
            assertThat(distinct.size(), equalTo(3));
            int[] expectedLocalIds = { 0, 1, 0, 0, 1, 2, 2, 0, 1, 2, 1, 0 };
            for (int p = 0; p < values.length; p++) {
                assertThat(distinct.localId(p, values[p]), equalTo(expectedLocalIds[p]));
            }
            try (BytesRefVector distinctValues = distinct.values(blockFactory, dictionary)) {
                assertThat(strings(distinctValues), equalTo(List.of("d7", "d0", "d5")));
            }
            try (IntVector hashIds = intVector(10, 20, 30); IntVector mapped = distinct.mapLocalIds(blockFactory, hashIds)) {
                assertThat(ints(mapped), equalTo(List.of(10, 20, 10, 10, 20, 30, 30, 10, 20, 30, 20, 10)));
            }
        }
    }

    public void testDistinctVectorSingleValueIsConstant() {
        try (IntVector ords = intVector(4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4)) {
            DictionaryOrdinals.Distinct distinct = DictionaryOrdinals.distinct(ords);
            assertThat(distinct, notNullValue());
            try (IntVector hashIds = intVector(42); IntVector mapped = distinct.mapLocalIds(blockFactory, hashIds)) {
                assertTrue(mapped.isConstant());
                assertThat(mapped.getInt(0), equalTo(42));
            }
        }
    }

    public void testDistinctVectorTooFewValues() {
        try (IntVector ords = intVector(1, 1, 1, 1, 1, 1, 1, 1, 1)) {
            assertThat(DictionaryOrdinals.distinct(ords), nullValue());
        }
    }

    /**
     * A page is worth deduplicating only while at most half its values are distinct.
     */
    public void testDistinctVectorLimit() {
        int positions = between(DictionaryOrdinals.MIN_VALUES, DictionaryOrdinals.EARLY_CHECK_VALUES - 1);
        int maxDistinct = positions / DictionaryOrdinals.MIN_AVERAGE_REPEATS;
        try (IntVector ords = cycle(positions, maxDistinct)) {
            assertThat(DictionaryOrdinals.distinct(ords), notNullValue());
        }
        try (IntVector ords = cycle(positions, maxDistinct + 1)) {
            assertThat(DictionaryOrdinals.distinct(ords), nullValue());
        }
    }

    /**
     * A page whose first {@link DictionaryOrdinals#EARLY_CHECK_VALUES} values are mostly distinct is given up on, even
     * if the rest of it would bring the ratio down.
     */
    public void testDistinctVectorEarlyCheck() {
        int prefix = DictionaryOrdinals.EARLY_CHECK_VALUES;
        int positions = prefix * 10;
        int prefixDistinct = prefix / DictionaryOrdinals.MIN_AVERAGE_REPEATS;
        try (IntVector ords = cycle(positions, prefixDistinct)) {
            assertThat(DictionaryOrdinals.distinct(ords), notNullValue());
        }
        try (IntVector ords = cycle(positions, prefixDistinct + 1)) {
            assertThat(DictionaryOrdinals.distinct(ords), nullValue());
        }
    }

    public void testDistinctBlock() {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(12)) {
            for (int i = 0; i < 4; i++) {
                builder.appendInt(9);
                builder.appendNull();
                builder.beginPositionEntry().appendInt(3).appendInt(9).appendInt(3).endPositionEntry();
            }
            try (IntBlock ords = builder.build()) {
                DictionaryOrdinals.Distinct distinct = DictionaryOrdinals.distinct(ords);
                assertThat(distinct, notNullValue());
                assertThat(distinct.size(), equalTo(2));
                for (int i = 0; i < 4; i++) {
                    // single-valued positions read the recorded id, multivalued ones look the ordinal up
                    assertThat(distinct.localId(3 * i, 9), equalTo(0));
                    assertThat(distinct.localId(3 * i + 2, 3), equalTo(1));
                    assertThat(distinct.localId(3 * i + 2, 9), equalTo(0));
                }
            }
        }
    }

    /**
     * Nulls add no values: a block with fewer than {@link DictionaryOrdinals#MIN_VALUES} values is not deduplicated.
     */
    public void testDistinctBlockCountsValuesNotPositions() {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(20)) {
            for (int i = 0; i < 20; i++) {
                if (i < DictionaryOrdinals.MIN_VALUES - 1) {
                    builder.appendInt(1);
                } else {
                    builder.appendNull();
                }
            }
            try (IntBlock ords = builder.build()) {
                assertThat(DictionaryOrdinals.distinct(ords), nullValue());
            }
        }
    }

    public void testDistinctBlockLimit() {
        int values = between(DictionaryOrdinals.MIN_VALUES, DictionaryOrdinals.EARLY_CHECK_VALUES - 1);
        int maxDistinct = values / DictionaryOrdinals.MIN_AVERAGE_REPEATS;
        try (IntVector ords = cycle(values, maxDistinct)) {
            assertThat(DictionaryOrdinals.distinct(ords.asBlock()), notNullValue());
        }
        try (IntVector ords = cycle(values, maxDistinct + 1)) {
            assertThat(DictionaryOrdinals.distinct(ords.asBlock()), nullValue());
        }
    }

    /**
     * {@code positions} ordinals cycling through {@code distinct} values.
     */
    private IntVector cycle(int positions, int distinct) {
        try (IntVector.Builder builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                builder.appendInt(p % distinct);
            }
            return builder.build();
        }
    }

    private BytesRefVector bytesVector(String... values) {
        try (BytesRefVector.Builder builder = blockFactory.newBytesRefVectorBuilder(values.length)) {
            for (String v : values) {
                builder.appendBytesRef(new BytesRef(v));
            }
            return builder.build();
        }
    }

    private static List<String> strings(BytesRefVector vector) {
        List<String> result = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        for (int p = 0; p < vector.getPositionCount(); p++) {
            result.add(vector.getBytesRef(p, scratch).utf8ToString());
        }
        return result;
    }

    private static List<Integer> ints(IntVector vector) {
        List<Integer> result = new ArrayList<>();
        for (int p = 0; p < vector.getPositionCount(); p++) {
            result.add(vector.getInt(p));
        }
        return result;
    }
}
