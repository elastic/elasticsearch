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

/**
 * Shared base for the fixed-width 64-bit columns (LONG and DOUBLE), whose values are contiguous
 * little-endian 8-byte slots ({@code data.getLongLE((base + d) * 8)}).
 */
abstract class AbstractFixed64Column extends EscfColumn {

    /** The value payload; 8 bytes per document slot, accessed at absolute index {@code base + d}. */
    final BytesReference data;

    AbstractFixed64Column(int docCount, FixedBitSet absent, BytesReference data) {
        super(docCount, absent);
        this.data = data;
    }

    AbstractFixed64Column(int docCount, FixedBitSet absent, BytesReference data, int base) {
        super(docCount, absent, base);
        this.data = data;
    }

    /** The raw little-endian 8-byte slot for document {@code d} (window-relative). */
    final long rawLong(int d) {
        return data.getLongLE((base + d) * 8);
    }
}
