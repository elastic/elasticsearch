/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * An ESCF column whose values are all booleans, stored as a value bitset (bit set = {@code true})
 * over little-endian 64-bit words. A present document's type byte is {@link SourceValueType#TRUE} or
 * {@link SourceValueType#FALSE} depending on its value bit. This is the only kind with no value {@code byte[]}.
 */
final class ElasticsearchBoolColumn extends ElasticsearchColumn {

    private final long[] valueBits;

    ElasticsearchBoolColumn(int columnIndex, int docCount, FixedBitSet absent, long[] valueBits) {
        super(columnIndex, docCount, absent);
        this.valueBits = valueBits;
    }

    @Override
    byte kind() {
        return ElasticsearchColumnKind.BOOL;
    }

    @Override
    byte typeByteForPresent(int d) {
        return bitSet(d) ? SourceValueType.TRUE : SourceValueType.FALSE;
    }

    @Override
    boolean getBooleanValue(int d) {
        return bitSet(d);
    }

    private boolean bitSet(int d) {
        return ((valueBits[d >>> 6] >>> (d & 63)) & 1L) != 0;
    }
}
