/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/** The kind that records the exact smallest and largest value of the slots each compressed chunk owns. */
final class ChunkBoundsSkip implements PlainSkipKind {

    static final byte ID = 2;

    static final ChunkBoundsSkip INSTANCE = new ChunkBoundsSkip();

    private ChunkBoundsSkip() {}

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public Writer newWriter(Directory directory, IOContext context, String dataName, long numValues) throws IOException {
        return new PlainSkipBounds.Writer(directory, context, dataName, numValues);
    }

    @Override
    public Reader newReader(StringColumnMetadata.Plain.Skipper skipper, long numValues, IndexInput skipIndex) throws IOException {
        return new PlainSkipBounds.Reader(skipper, numValues, skipIndex);
    }
}
