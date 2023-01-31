/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.util.ArrayUtil;

import java.util.Arrays;

/**
 * Custom implementation of a bitset for fast item set deduplication.
 *
 * Unfortunately other {@code BitSet} implementation, e.g. java.util,
 * lack a subset check.
 *
 * For this implementation I took the code from {@code BitSet}, removed
 * unnecessary parts and added additional functionality like the subset check.
 * Cardinality - the number of set bits == number of items - is used a lot.
 * The original {@code BitSet} uses a scan, this implementation uses
 * a counter for faster retrieval.
 */
class ItemSetBitSet implements Cloneable {

    public enum SetRelation {
        DISJOINT_OR_INTERSECT, // we don't care if disjoint or intersect
        SUB_SET,
        SUPER_SET,
        EQUAL
    }

    // taken from {@code BitSet}
    private static final int ADDRESS_BITS_PER_WORD = 6;
    private static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;

    /* Used to shift left or right for a partial word mask */
    private static final long WORD_MASK = 0xffffffffffffffffL;

    // allow direct access for transaction lookup table
    long[] words;
    transient int wordsInUse = 0;
    private int cardinality = 0;

    ItemSetBitSet() {
        initWords(BITS_PER_WORD);
    }

    ItemSetBitSet(int nbits) {
        // nbits can't be negative; size 0 is OK
        if (nbits < 0) throw new NegativeArraySizeException("nbits < 0: " + nbits);

        initWords(nbits);
    }

    void reset(ItemSetBitSet bitSet) {
        words = ArrayUtil.grow(words, bitSet.wordsInUse);
        System.arraycopy(bitSet.words, 0, this.words, 0, bitSet.wordsInUse);
        this.cardinality = bitSet.cardinality;
        this.wordsInUse = bitSet.wordsInUse;
    }

    void set(int bitIndex) {
        if (bitIndex < 0) throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

        int wordIndex = wordIndex(bitIndex);
        expandTo(wordIndex);

        final long oldWord = words[wordIndex];
        words[wordIndex] |= (1L << bitIndex); // Restores invariants

        if (oldWord != words[wordIndex]) {
            cardinality++;
        }
    }

    boolean get(int bitIndex) {
        if (bitIndex < 0) throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

        int wordIndex = wordIndex(bitIndex);
        return (wordIndex < wordsInUse) && ((words[wordIndex] & (1L << bitIndex)) != 0);
    }

    void clear(int bitIndex) {
        if (bitIndex < 0) throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

        int wordIndex = wordIndex(bitIndex);
        if (wordIndex >= wordsInUse) return;

        final long oldWord = words[wordIndex];

        words[wordIndex] &= ~(1L << bitIndex);
        if (oldWord != words[wordIndex]) {
            cardinality--;
        }
        recalculateWordsInUse();
    }

    public void clear() {
        while (wordsInUse > 0) {
            words[--wordsInUse] = 0;
        }
        cardinality = 0;
    }

    /**
     * Returns true if the specified {@code ItemBitSet} is a subset of this
     * set.
     *
     * @param set {@code ItemBitSet} to check
     * @return true if the given set is a subset of this set
     */
    boolean isSubset(ItemSetBitSet set) {
        if (wordsInUse > set.wordsInUse) {
            return false;
        }

        for (int i = wordsInUse - 1; i >= 0; i--) {
            if ((words[i] & set.words[i]) != words[i]) {
                return false;
            }
        }

        return true;
    }

    public SetRelation setRelation(ItemSetBitSet set) {
        // this method is performance critical, change carefully

        if (wordsInUse > set.wordsInUse || cardinality > set.cardinality) {
            for (int i = set.wordsInUse - 1; i >= 0; i--) {
                if ((set.words[i] & words[i]) != set.words[i]) {
                    return SetRelation.DISJOINT_OR_INTERSECT;
                }
            }
            return SetRelation.SUPER_SET;
        } else if (wordsInUse < set.wordsInUse || cardinality < set.cardinality) {
            for (int i = wordsInUse - 1; i >= 0; i--) {
                if ((words[i] & set.words[i]) != words[i]) {
                    return SetRelation.DISJOINT_OR_INTERSECT;
                }
            }
            return SetRelation.SUB_SET;
        }

        // both bitsets have the same wordsInUse and cardinality, so they can be either equal or not
        for (int i = wordsInUse - 1; i >= 0; i--) {
            if ((words[i] & set.words[i]) != words[i]) {
                return SetRelation.DISJOINT_OR_INTERSECT;
            }
        }

        return SetRelation.EQUAL;
    }

    int nextSetBit(int fromIndex) {
        if (fromIndex < 0) throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);

        int u = wordIndex(fromIndex);
        if (u >= wordsInUse) return -1;

        long word = words[u] & (WORD_MASK << fromIndex);

        while (true) {
            if (word != 0) return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            if (++u == wordsInUse) return -1;
            word = words[u];
        }
    }

    int cardinality() {
        return cardinality;
    }

    public static int compare(ItemSetBitSet a, ItemSetBitSet b) {
        if (a.cardinality != b.cardinality) {
            return a.cardinality > b.cardinality ? 1 : -1;
        }

        if (a.wordsInUse != b.wordsInUse) {
            return a.wordsInUse < b.wordsInUse ? 1 : -1;
        }

        int i = Arrays.mismatch(a.words, 0, a.wordsInUse, b.words, 0, b.wordsInUse);

        if (i == -1) {
            return 0;
        }

        return a.words[i] < b.words[i] ? 1 : -1;
    }

    @Override
    public Object clone() {
        trimToSize();

        try {
            ItemSetBitSet result = (ItemSetBitSet) super.clone();
            result.words = words.clone();
            return result;
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    @Override
    public String toString() {
        final int MAX_INITIAL_CAPACITY = Integer.MAX_VALUE - 8;
        int numBits = wordsInUse * BITS_PER_WORD;
        // Avoid overflow in the case of a humongous numBits
        int initialCapacity = (numBits <= (MAX_INITIAL_CAPACITY - 2) / 6) ? 6 * numBits + 2 : MAX_INITIAL_CAPACITY;
        StringBuilder b = new StringBuilder(initialCapacity);

        for (int i = 0; i < wordsInUse; ++i) {
            b.append(words[i]);
            b.append(" ");
        }

        return b.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        if (this == obj) {
            return true;
        }

        final ItemSetBitSet set = (ItemSetBitSet) obj;
        if (wordsInUse != set.wordsInUse) return false;

        // Check words in use by both BitSets
        for (int i = 0; i < wordsInUse; i++)
            if (words[i] != set.words[i]) return false;

        return true;
    }

    @Override
    public int hashCode() {
        // Arrays.hashCode does not support subarrays
        int result = 1;
        for (int i = 0; i < wordsInUse; i++) {
            int elementHash = (int) (words[i] ^ (words[i] >>> 32));
            result = 31 * result + elementHash;
        }

        return result;
    }

    private void trimToSize() {
        if (wordsInUse != words.length) {
            words = Arrays.copyOf(words, wordsInUse);
        }
    }

    private void initWords(int nbits) {
        words = new long[wordIndex(nbits - 1) + 1];
    }

    private void recalculateWordsInUse() {
        // Traverse the bitset until a used word is found
        int i;
        for (i = wordsInUse - 1; i >= 0; i--)
            if (words[i] != 0) break;

        wordsInUse = i + 1; // The new logical size
    }

    private void expandTo(int wordIndex) {
        int wordsRequired = wordIndex + 1;
        if (wordsInUse < wordsRequired) {
            words = ArrayUtil.grow(words, wordsRequired);
            wordsInUse = wordsRequired;
        }
    }

    private static int wordIndex(int bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }
}
