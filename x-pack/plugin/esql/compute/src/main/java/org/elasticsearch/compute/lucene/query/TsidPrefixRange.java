/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

/**
 * The {@code _tsid}s whose leading {@code prefixBits} bits are in {@code [lower, upper)}.
 */
public record TsidPrefixRange(int prefixBits, int lower, int upper) {
    public TsidPrefixRange {
        if (prefixBits < 1 || prefixBits >= Integer.SIZE - 1) {
            throw new IllegalArgumentException("prefix bits must be in [1, 30]; got [" + prefixBits + "]");
        }
        if (lower < 0 || lower >= upper || upper > 1 << prefixBits) {
            throw new IllegalArgumentException("invalid range [" + lower + ", " + upper + ") for [" + prefixBits + "] prefix bits");
        }
    }
}
