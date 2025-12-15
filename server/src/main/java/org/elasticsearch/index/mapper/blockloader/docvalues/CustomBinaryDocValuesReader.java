/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

/**
 * Helper class to read custom binary doc values.
 */
public final class CustomBinaryDocValuesReader {

    private final BytesRef scratch = new BytesRef();
    private final ByteArrayStreamInput in = new ByteArrayStreamInput();

    public CustomBinaryDocValuesReader() {}

    public void read(BytesRef bytes, BlockLoader.BytesRefBuilder builder) throws IOException {
        assert bytes.length > 0;
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        int count = in.readVInt();
        scratch.bytes = bytes.bytes;

        if (count == 1) {
            scratch.length = in.readVInt();
            scratch.offset = in.getPosition();
            builder.appendBytesRef(scratch);
            return;
        }
        builder.beginPositionEntry();
        for (int v = 0; v < count; v++) {
            scratch.length = in.readVInt();
            scratch.offset = in.getPosition();
            in.setPosition(scratch.offset + scratch.length);
            builder.appendBytesRef(scratch);
        }
        builder.endPositionEntry();
    }
}
