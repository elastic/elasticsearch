/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.SourceValueType;

/** An ESCF column whose values are all {@code long}s (JSON ints and longs upcast to 64-bit). */
public final class EscfLongColumn extends AbstractFixed64Column {

    EscfLongColumn(int docCount, FixedBitSet validity, BytesReference data) {
        super(docCount, validity, data);
    }

    @Override
    public byte kind() {
        return EscfColumnKind.LONG;
    }

    @Override
    byte typeByteForPresent(int row) {
        return SourceValueType.LONG;
    }

    @Override
    long getLongValue(int row) {
        return rawLong(row);
    }

    public long longValueAt(int row) {
        return rawLong(row);
    }

    /**
     * Returns a new dense {@link LongValuesCursor} positioned before the first row of this column's
     * window. The column must be fully present ({@link #validity} {@code == null}); call this only on
     * dense columns.
     */
    DenseLongValuesCursor longValuesCursor() {
        assert validity == null : "values cursor is only valid for dense (fully-present) columns";
        return new DenseLongValuesCursor(docCount, this);
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        return new EscfLongColumn(count, windowValidity(validity, from, count), data.slice(from * 8, count * 8));
    }

    @Override
    EscfColumnData toColumnData() {
        return EscfColumnData.ofFixed64(kind(), docCount, validity, data);
    }
}
