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
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * Shared base for the variable-length columns (STRING and BINARY), whose values are a contiguous
 * {@code data} payload delimited by an {@link IntsRef} offset window. Document {@code d}'s value
 * occupies bytes {@code [offsets.ints[offsets.offset + d], offsets.ints[offsets.offset + d + 1])}
 * within {@code data} (which is kept full/shared across slices). Slicing adjusts
 * {@code offsets.offset}; the backing arrays are shared.
 */
abstract class AbstractVarColumn extends EscfColumn {

    /** Full (shared) data payload. Addressed via the absolute byte offsets in {@code offsets}. */
    final BytesReference data;

    /**
     * Windowed offset vector: {@code offsets.ints[offsets.offset + d]} is the absolute byte start
     * of document {@code d}'s value. The window covers {@code docCount + 1} entries so that the
     * end of the last document can be computed as {@code offsets.ints[offsets.offset + docCount]}.
     */
    final IntsRef offsets;

    AbstractVarColumn(int docCount, FixedBitSet absent, BytesReference data, IntsRef offsets) {
        super(docCount, absent);
        this.data = data;
        this.offsets = offsets;
    }

    @Override
    final BytesRef getBinaryValue(int d) {
        int off = offsets.ints[offsets.offset + d];
        return data.slice(off, offsets.ints[offsets.offset + d + 1] - off).toBytesRef();
    }
}
