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
import org.elasticsearch.common.util.ByteUtils;

/**
 * Shared base for the fixed-width 64-bit columns (LONG and DOUBLE), whose values are contiguous
 * little-endian 8-byte slots ({@code data[base + d * 8]}). Subclasses differ only in how they
 * interpret the raw long and which {@link org.elasticsearch.sourcebatch.SourceValueType} byte they report.
 */
abstract class AbstractFixed64Column extends ElasticsearchColumn {

    private final byte[] data;
    private final int base;

    AbstractFixed64Column(int columnIndex, int docCount, FixedBitSet absent, byte[] data, int base) {
        super(columnIndex, docCount, absent);
        this.data = data;
        this.base = base;
    }

    /** The raw little-endian 8-byte slot for document {@code d}. */
    final long rawLong(int d) {
        return ByteUtils.readLongLE(data, base + d * 8);
    }
}
