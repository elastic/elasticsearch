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
 * An column whose values are all booleans, held as the value bitset directly (bit set = {@code true}).
 * A present document's type byte is {@link SourceValueType#TRUE} or {@link SourceValueType#FALSE} depending
 * on its value bit. This is the only kind with no value {@code byte[]}: it reads the {@link FixedBitSet}
 * in place rather than materializing it into words.
 */
final class ElasticsearchBoolColumn extends ElasticsearchColumn {

    /** Value bitset (bit set = {@code true}), or {@code null} when every value is {@code false}. */
    private final FixedBitSet values;

    ElasticsearchBoolColumn(int docCount, FixedBitSet absent, FixedBitSet values) {
        super(docCount, absent);
        this.values = values;
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
        // The value bitset is sized only to the last true document (and is null when there are none),
        // so any doc beyond its length reads false. Mirrors ElasticsearchColumn#isAbsent.
        return values != null && d < values.length() && values.get(d);
    }
}
