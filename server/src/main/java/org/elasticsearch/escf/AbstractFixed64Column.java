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
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Shared base for the fixed-width 64-bit columns (LONG and DOUBLE), whose values are contiguous
 * little-endian 8-byte slots ({@code data.getLongLE(d * 8)}).
 */
abstract class AbstractFixed64Column extends EscfColumn {

    protected final BytesReference data;

    AbstractFixed64Column(int docCount, FixedBitSet validity, BytesReference data) {
        super(docCount, validity);
        assert assertDataValid(docCount, data);
        this.data = data;
    }

    /** The raw little-endian 8-byte slot for document {@code d}. */
    final long rawLong(int row) {
        return data.getLongLE(row * 8);
    }

    private static boolean assertDataValid(int docCount, BytesReference data) {
        assert data.length() == (long) docCount * 8
            : "fixed-64 column data length " + data.length() + " != docCount * 8 = " + ((long) docCount * 8);
        try {
            BytesRefIterator iter = data.iterator();
            BytesRef chunk;
            while ((chunk = iter.next()) != null) {
                assert chunk.length % 8 == 0 : "chunk length " + chunk.length + " is not a multiple of 8";
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }
}
