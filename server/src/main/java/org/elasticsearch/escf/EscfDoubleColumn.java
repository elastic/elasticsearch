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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.SourceValueType;

/** An ESCF column whose values are all {@code double}s (JSON floats and doubles upcast to 64-bit raw bits). */
final class EscfDoubleColumn extends AbstractFixed64Column {

    EscfDoubleColumn(int docCount, FixedBitSet absent, BytesReference data) {
        super(docCount, absent, data);
    }

    private EscfDoubleColumn(int docCount, FixedBitSet absent, BytesReference data, int base) {
        super(docCount, absent, data, base);
    }

    @Override
    byte kind() {
        return EscfColumnKind.DOUBLE;
    }

    @Override
    byte typeByteForPresent(int d) {
        return SourceValueType.DOUBLE;
    }

    @Override
    double getDoubleValue(int d) {
        return Double.longBitsToDouble(rawLong(d));
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        return new EscfDoubleColumn(count, absent, data, base + from);
    }

    @Override
    EscfColumnData toColumnData() {
        FixedBitSet newAbsent = windowBitSet(absent, base, docCount);
        BytesReference newData = data.slice(base * 8, docCount * 8);
        return EscfColumnData.ofFixed64(kind(), docCount, newAbsent, newData);
    }
}
