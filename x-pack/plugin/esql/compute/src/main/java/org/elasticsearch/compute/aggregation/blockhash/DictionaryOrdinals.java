/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import com.carrotsearch.hppc.IntIntHashMap;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;

import java.util.Arrays;

/**
 * Helpers for hashing an ordinal-encoded {@code BYTES_REF} grouping key by the dictionary entries a page references.
 * <p>
 *     A dictionary can hold entries the page does not reference: Parquet attaches the whole row-group dictionary to every
 *     page, and {@link OrdinalBytesRefBlock#keepMask} or a slice keep the full dictionary. Those entries must not enter a
 *     hash, both because they would become groups and because they would be hashed for nothing.
 * </p>
 * <p>
 *     Ordinals are always read position by position: a null position or a position dropped by
 *     {@link OrdinalBytesRefBlock#keepMask} can still hold a value slot whose ordinal the page does not reference.
 * </p>
 */
final class DictionaryOrdinals {
    /**
     * Pages with fewer values than this are hashed value by value: there is too little to save. Matches the value
     * floor of {@link OrdinalBytesRefBlock#isDense(long, long)}.
     */
    static final int MIN_VALUES = 10;
    /**
     * Hashing each distinct entry once only pays off when the page uses each one at least this many times on average.
     * Matches the ratio of {@link OrdinalBytesRefBlock#isDense(long, long)}.
     */
    static final int MIN_AVERAGE_REPEATS = 2;
    /**
     * After this many values, a page whose values are still mostly distinct is assumed to be high-cardinality and given
     * up on, so such a page pays a bounded scan before it is hashed value by value.
     */
    static final int EARLY_CHECK_VALUES = 128;

    private DictionaryOrdinals() {}

    /**
     * Flags in {@code referenced} the dictionary ordinals {@code ords} uses.
     * <p>
     *     {@code referenced} is sized by the dictionary, which can be far larger than the page. Callers either bound it,
     *     as the {@link OrdinalBytesRefBlock#isDense density} gate does, or accept the cost.
     * </p>
     * @return the number of distinct ordinals flagged, so every entry is referenced iff it equals {@code referenced.length}
     */
    static int markReferenced(IntVector ords, boolean[] referenced) {
        int count = 0;
        for (int p = 0; p < ords.getPositionCount(); p++) {
            int ord = ords.getInt(p);
            if (referenced[ord] == false) {
                referenced[ord] = true;
                count++;
            }
        }
        return count;
    }

    /**
     * Flags in {@code referenced} the dictionary ordinals the positions of {@code ords} use, including every value of a
     * multivalued position. See {@link #markReferenced(IntVector, boolean[])}.
     */
    static int markReferenced(IntBlock ords, boolean[] referenced) {
        int count = 0;
        for (int p = 0; p < ords.getPositionCount(); p++) {
            int start = ords.getFirstValueIndex(p);
            int end = start + ords.getValueCount(p);
            for (int i = start; i < end; i++) {
                int ord = ords.getInt(i);
                if (referenced[ord] == false) {
                    referenced[ord] = true;
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * A vector of the dictionary entries flagged in {@code referenced}, in dictionary order, reading through
     * {@code dictionary}. Hashing it instead of looking each entry up keeps the hash's batched prefetching. It holds a
     * reference to {@code dictionary}, so close it after use. Hash it value by value: it is an ordinal vector, but
     * routing it back through the ordinal paths would only mark the same entries again.
     *
     * @param referencedCount the number of flagged entries, as returned by {@code markReferenced}
     */
    static BytesRefVector referencedValues(
        BlockFactory blockFactory,
        BytesRefVector dictionary,
        boolean[] referenced,
        int referencedCount
    ) {
        int[] ords = new int[referencedCount];
        int v = 0;
        for (int ord = 0; ord < referenced.length; ord++) {
            if (referenced[ord]) {
                ords[v++] = ord;
            }
        }
        assert v == referencedCount : "expected [" + referencedCount + "] referenced entries but found [" + v + "]";
        return ordinalView(blockFactory, ords, referencedCount, dictionary);
    }

    private static BytesRefVector ordinalView(BlockFactory blockFactory, int[] ords, int count, BytesRefVector dictionary) {
        IntVector viewOrds = blockFactory.newIntArrayVector(ords, count);
        dictionary.incRef();
        return new OrdinalBytesRefVector(viewOrds, dictionary);
    }

    /**
     * Collects the distinct ordinals of a page that is not {@link OrdinalBytesRefVector#isDense dense}, if the page
     * repeats them enough to hash each one once instead of hashing every position.
     *
     * @return the distinct ordinals in first-seen order, or {@code null} if the page should be hashed value by value
     */
    static Distinct distinct(IntVector ords) {
        int positions = ords.getPositionCount();
        if (positions < MIN_VALUES) {
            return null;
        }
        Distinct distinct = new Distinct(positions, positions);
        for (int p = 0; p < positions; p++) {
            distinct.positionIds[p] = distinct.add(ords.getInt(p));
            if (distinct.tooMany(p + 1)) {
                return null;
            }
        }
        return distinct;
    }

    /**
     * Like {@link #distinct(IntVector)} for a block: null positions add nothing and every value of a multivalued
     * position counts. Callers map values with {@link Distinct#localId(int, int)}.
     */
    static Distinct distinct(IntBlock ords) {
        int values = ords.getTotalValueCount();
        if (values < MIN_VALUES) {
            return null;
        }
        int positions = ords.getPositionCount();
        Distinct distinct = new Distinct(values, positions);
        int seen = 0;
        for (int p = 0; p < positions; p++) {
            int start = ords.getFirstValueIndex(p);
            int valueCount = ords.getValueCount(p);
            if (valueCount == 1) {
                distinct.positionIds[p] = distinct.add(ords.getInt(start));
                if (distinct.tooMany(++seen)) {
                    return null;
                }
                continue;
            }
            distinct.positionIds[p] = UNRECORDED;
            for (int i = start; i < start + valueCount; i++) {
                distinct.add(ords.getInt(i));
                if (distinct.tooMany(++seen)) {
                    return null;
                }
            }
        }
        return distinct;
    }

    private static final int UNRECORDED = -1;

    /**
     * The distinct ordinals of a page, in the order the page first uses them. Hashing them in this order assigns the
     * same group ids as hashing the page value by value, while hashing each value only once.
     * <p>
     *     Two ordinals count as two values. Single-valued positions group correctly either way, but deduplicating a
     *     multivalued position by ordinal assumes its dictionary has distinct entries, as the dense dictionary path
     *     does.
     * </p>
     * <p>
     *     Memory is bounded by the page, not the dictionary: {@link #distinct} gives up once more than half the page's
     *     values are distinct.
     * </p>
     */
    static final class Distinct {
        private final IntIntHashMap localIds = new IntIntHashMap();
        private final int maxDistinct;
        /**
         * Each single-valued position's local id; {@link #UNRECORDED} for null and multivalued positions.
         */
        private final int[] positionIds;
        private int[] ords = new int[16];

        private Distinct(int values, int positions) {
            this.maxDistinct = values / MIN_AVERAGE_REPEATS;
            this.positionIds = new int[positions];
        }

        private int add(int ord) {
            int slot = localIds.indexOf(ord);
            if (localIds.indexExists(slot)) {
                return localIds.indexGet(slot);
            }
            int localId = localIds.size();
            localIds.indexInsert(slot, ord, localId);
            if (localId == ords.length) {
                ords = Arrays.copyOf(ords, ords.length * 2);
            }
            ords[localId] = ord;
            return localId;
        }

        private boolean tooMany(int seen) {
            int count = localIds.size();
            return count > maxDistinct || (seen == EARLY_CHECK_VALUES && count * MIN_AVERAGE_REPEATS > EARLY_CHECK_VALUES);
        }

        int size() {
            return localIds.size();
        }

        /**
         * Index among the distinct ordinals of {@code ord}, a value of {@code position}; {@code ord} must have been
         * collected. Single-valued positions read the id {@link #distinct} recorded, others look the ordinal up.
         */
        int localId(int position, int ord) {
            int localId = positionIds[position];
            if (localId != UNRECORDED) {
                assert ords[localId] == ord : "position [" + position + "] holds ordinal [" + ords[localId] + "] not [" + ord + "]";
                return localId;
            }
            int slot = localIds.indexOf(ord);
            assert localIds.indexExists(slot) : "ordinal [" + ord + "] was not collected";
            return localIds.indexGet(slot);
        }

        /**
         * A vector of the distinct values in first-seen order, reading through {@code dictionary}. It holds a reference
         * to {@code dictionary}, so close it after use. Hash it value by value: it is an ordinal vector, but routing it
         * back through the ordinal paths would only collect the same ordinals again.
         */
        BytesRefVector values(BlockFactory blockFactory, BytesRefVector dictionary) {
            return ordinalView(blockFactory, ords, size(), dictionary);
        }

        /**
         * Maps each position of the vector {@link DictionaryOrdinals#distinct(IntVector)} scanned to the id the hash gave
         * its value. Call it once: the local ids become the result's backing array.
         *
         * @param hashIds the hash id of each distinct value, in first-seen order
         */
        IntVector mapLocalIds(BlockFactory blockFactory, IntVector hashIds) {
            int positions = positionIds.length;
            if (size() == 1) {
                return blockFactory.newConstantIntVector(hashIds.getInt(0), positions);
            }
            for (int p = 0; p < positions; p++) {
                positionIds[p] = hashIds.getInt(positionIds[p]);
            }
            return blockFactory.newIntArrayVector(positionIds, positions);
        }
    }
}
