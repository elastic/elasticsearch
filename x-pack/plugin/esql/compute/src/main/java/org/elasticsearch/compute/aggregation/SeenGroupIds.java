/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;

public interface SeenGroupIds {
    /**
     * The grouping ids that have been seen already. This {@link BitArray} is
     * kept and mutated by the caller so make a copy if it's something you
     * need your own copy of it.
     */
    BitArray seenGroupIds(BigArrays bigArrays);

    record Empty() implements SeenGroupIds {
        @Override
        public BitArray seenGroupIds(BigArrays bigArrays) {
            return new BitArray(1, bigArrays);
        }
    }

    record Range(int from, int to) implements SeenGroupIds {
        @Override
        public BitArray seenGroupIds(BigArrays bigArrays) {
            BitArray seen = new BitArray(to - from, bigArrays);
            for (int i = from; i < to; i++) {
                seen.set(i);
            }
            return seen;
        }
    }
}
