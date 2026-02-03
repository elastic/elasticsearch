/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import java.util.Set;
import java.util.function.ToIntFunction;

/**
 * A {@link ToIntFunction} that relies on input arrays of string keys and numeric values.
 * If the input {@link SubstringView} matches any key, the corresponding value is returned; otherwise - the function returns -1.
 * If the input value array is null, the function will act as a set membership test, returning 1 for any matching key and -1 for
 * non-matching keys.
 * It is optimized for fast lookups by using a hash table that ensures no collisions within the initial set of strings (thus no buckets
 * to search through) and fast array access.
 * In addition, it avoids allocation of new objects during lookups by using {@link SubstringView} as input as well as
 * the overhead related to boxing and unboxing of integers.
 * It does not attempt to be minimal (e.g., by relying on perfect hashing), but rather to be sparse, as we want to reduce the chance
 * of collisions on lookups as well, because collisions require equality checks.
 */
public final class StringToIntMapper_Experimental implements ToIntFunction<SubstringView> {

    public static final int MAX_SIZE = 128;

    private final SubstringView[] hashSet1;
    private final SubstringView[] hashSet2;
    private final int[] values1;
    private final int[] values2;
    private final boolean isSet;
    private final int hashFactor;

    public StringToIntMapper_Experimental(Set<String> keys) {
        this(keys.toArray(new String[0]), null);
    }

    public StringToIntMapper_Experimental(final String[] keys, int[] values) {
        int setSize = keys.length * 2;
        SubstringView[] tmpHashSet1 = null;
        SubstringView[] tmpHashSet2 = null;
        int[] tmpValues1 = null;
        int[] tmpValues2 = null;
        isSet = values == null;

        // First attempt: try with a single array (no collisions allowed)
        boolean singleArrayFailed = false;
        while (tmpHashSet1 == null && singleArrayFailed == false) {
            setSize += 23;
            if (setSize > MAX_SIZE) {
                // We couldn't fit the keys in a single array within the max size limit
                singleArrayFailed = true;
            } else {
                tmpHashSet1 = new SubstringView[setSize];
                tmpValues1 = isSet ? null : new int[setSize];
                for (int i = 0; i < keys.length; i++) {
                    SubstringView substring = new SubstringView(keys[i]);
                    int hash = internalHash(substring, setSize);
                    if (tmpHashSet1[hash] == null) {
                        tmpHashSet1[hash] = substring;
                        if (isSet == false) tmpValues1[hash] = values[i];
                    } else {
                        // Collision detected, can't use a single array
                        tmpHashSet1 = null;
                        break;
                    }
                }
            }
        }

        // Second attempt: try with two arrays (one collision per hash allowed)
        if (singleArrayFailed) {
            setSize = keys.length;
            while (tmpHashSet1 == null) {
                setSize += 23;
                if (setSize > MAX_SIZE) {
                    throw new IllegalArgumentException(
                        "Cannot generate a hash table for the provided set with at most one collision per "
                            + "slot without exceeding the maximum size of "
                            + MAX_SIZE
                    );
                }

                tmpHashSet1 = new SubstringView[setSize];
                tmpHashSet2 = new SubstringView[setSize];
                tmpValues1 = isSet ? null : new int[setSize];
                tmpValues2 = isSet ? null : new int[setSize];

                for (int i = 0; i < keys.length; i++) {
                    SubstringView substring = new SubstringView(keys[i]);
                    int hash = internalHash(substring, setSize);
                    if (tmpHashSet1[hash] == null) {
                        tmpHashSet1[hash] = substring;
                        if (isSet == false) tmpValues1[hash] = values[i];
                    } else if (tmpHashSet2[hash] == null) {
                        tmpHashSet2[hash] = substring;
                        if (isSet == false) tmpValues2[hash] = values[i];
                    } else {
                        // More than one collision for this hash slot, need to increase table size
                        tmpHashSet1 = null;
                        tmpHashSet2 = null;
                        break;
                    }
                }
            }
        }

        this.hashSet1 = tmpHashSet1;
        this.hashSet2 = tmpHashSet2;
        this.values1 = tmpValues1;
        this.values2 = tmpValues2;
        this.hashFactor = this.hashSet1.length;
    }

    /**
     * Calculates the hash code for the given SubstringView and returns its index in the hash table. Insertions and lookups must use the
     * exact same hash logic to ensure consistency.
     * @param str the SubstringView to calculate the hash for
     * @param hashFactor the size of the hash table to use for the hash calculation
     * @return the index in the hash table for the given SubstringView
     */
    private int internalHash(final SubstringView str, final int hashFactor) {
        // calculating hash, then ensuring it is non-negative and finally taking modulo to fit into the hash table size
        return (str.hashCode() & 0x7FFFFFFF) % hashFactor;
    }

    @Override
    public int applyAsInt(final SubstringView input) {
        int hash = internalHash(input, hashFactor);
        SubstringView match1 = hashSet1[hash];
        if (match1 != null && match1.equals(input)) {
            return isSet ? 1 : values1[hash];
        }
        if (hashSet2 != null) {
            SubstringView match2 = hashSet2[hash];
            if (match2 != null && match2.equals(input)) {
                return isSet ? 1 : values2[hash];
            }
        }
        return -1;
    }
}
