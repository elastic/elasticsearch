/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Shared base for the variable-length columns (STRING and BINARY), whose values are a contiguous
 * {@code data} payload delimited by a {@code (docCount + 1)}-entry offset vector
 * ({@code [offsets[d], offsets[d + 1])} within {@code data} starting at {@code base}). Subclasses
 * differ only in the value type they expose and the {@link org.elasticsearch.sourcebatch.SourceValueType} byte they
 * report.
 */
abstract class AbstractVarColumn extends ElasticsearchColumn {

    final byte[] data;
    final int base;
    final int[] offsets;

    AbstractVarColumn(int columnIndex, int docCount, FixedBitSet absent, byte[] data, int base, int[] offsets) {
        super(columnIndex, docCount, absent);
        this.data = data;
        this.base = base;
        this.offsets = offsets;
    }

    @Override
    final BytesRef getBinaryValue(int d) {
        int off0 = offsets[d];
        return new BytesRef(data, base + off0, offsets[d + 1] - off0);
    }
}
