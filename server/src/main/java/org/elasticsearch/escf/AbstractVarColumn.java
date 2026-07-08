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
import org.elasticsearch.common.bytes.BytesReference;

/**
 * Shared base for the variable-length columns (STRING and BINARY), whose values are a contiguous
 * {@code data} payload delimited by a {@code (docCount + 1)}-entry offset vector
 * ({@code [offsets[d], offsets[d + 1])} within {@code data}).
 */
abstract class AbstractVarColumn extends EscfColumn {

    final BytesReference data;
    final int[] offsets;

    AbstractVarColumn(int docCount, FixedBitSet absent, BytesReference data, int[] offsets) {
        super(docCount, absent);
        this.data = data;
        this.offsets = offsets;
    }

    @Override
    final BytesRef getBinaryValue(int d) {
        int off = offsets[d];
        return data.slice(off, offsets[d + 1] - off).toBytesRef();
    }
}
