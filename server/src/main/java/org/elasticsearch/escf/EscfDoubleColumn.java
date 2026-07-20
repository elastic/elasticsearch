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

    @Override
    byte kind() {
        return EscfColumnKind.DOUBLE;
    }

    @Override
    byte typeByteForPresent(int row) {
        return SourceValueType.DOUBLE;
    }

    @Override
    double getDoubleValue(int row) {
        return Double.longBitsToDouble(rawLong(row));
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        return new EscfDoubleColumn(count, windowBitSet(absent, from, count), data.slice(from * 8, count * 8));
    }

    @Override
    EscfColumnData toColumnData() {
        return EscfColumnData.ofFixed64(kind(), docCount, absent, data);
    }
}
