/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.util;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link Bits} implementation that combines two  {@link Bits} instances by and-ing them to provide a single merged view.
 */
public final class CombinedBits implements Bits {
    private final Bits first;
    private final Bits second;
    private final int length;

    public CombinedBits(Bits first, Bits second) {
        if (first.length() != second.length()) {
            throw new IllegalArgumentException("Provided bits have different lengths: " + first.length() + " != " + second.length());
        }
        this.first = first;
        this.second = second;
        this.length = first.length();
    }

    @Override
    public boolean get(int index) {
        return first.get(index) && second.get(index);
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public void applyMask(FixedBitSet bitSet, int offset) {
        first.applyMask(bitSet, offset);
        second.applyMask(bitSet, offset);
    }
}
