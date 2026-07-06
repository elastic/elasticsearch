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
 * ({@code [offsets[d], offsets[d + 1])} within {@code data}). Subclasses differ only in the value
 * type they expose and the {@link org.elasticsearch.sourcebatch.SourceValueType} byte they report.
 *
 * <p>The payload is held as its native, possibly-paged {@link BytesReference} rather than a
 * materialised {@code byte[]}, so the pooled recycler pages are read in place instead of being copied
 * up front. A single value's contiguous bytes are obtained lazily via {@code slice(off, len).toBytesRef()},
 * which is zero-copy when the value lives inside one page (the common case) and copies only a lone
 * page-straddling value.
 */
abstract class AbstractVarColumn extends ElasticsearchColumn {

    final BytesReference data;
    final int[] offsets;

    AbstractVarColumn(int docCount, FixedBitSet absent, BytesReference data, int[] offsets) {
        super(docCount, absent);
        this.data = data;
        this.offsets = offsets;
    }

    @Override
    final BytesRef getBinaryValue(int d) {
        int off0 = offsets[d];
        return data.slice(off0, offsets[d + 1] - off0).toBytesRef();
    }
}
