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
import java.util.function.Predicate;

/**
 * Helper class to read custom binary doc values.
 */
public final class MultiValueSeparateCountBinaryDocValuesReader {

    private final BytesRef scratch = new BytesRef();
    private final ByteArrayStreamInput in = new ByteArrayStreamInput();

    public MultiValueSeparateCountBinaryDocValuesReader() {}

    public void read(BytesRef bytes, long count, BlockLoader.BytesRefBuilder builder) throws IOException {
        if (count == 1) {
            builder.appendBytesRef(bytes);
            return;
        }

        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        builder.beginPositionEntry();
        for (int v = 0; v < count; v++) {
            initializeScratch();
            builder.appendBytesRef(scratch);
        }
        builder.endPositionEntry();
    }

    public boolean match(BytesRef bytes, long count, Predicate<BytesRef> predicate) throws IOException {
        if (count == 1) {
            return predicate.test(bytes);
        }

        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        for (int v = 0; v < count; v++) {
            initializeScratch();
            if (predicate.test(scratch)) {
                return true;
            }
        }
        return false;
    }

    public void readMin(BytesRef bytes, int count, BlockLoader.BytesRefBuilder builder) throws IOException {
        if (count == 1) {
            builder.appendBytesRef(bytes);
            return;
        }

        BytesRef min = null;
        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        for (int v = 0; v < count; v++) {
            initializeScratch();
            if (min == null || scratch.compareTo(min) < 0) {
                min = new BytesRef(scratch.bytes, scratch.offset, scratch.length);
            }
        }
        builder.appendBytesRef(min);
    }

    private void initializeScratch() throws IOException {
        scratch.length = in.readVInt();
        scratch.offset = in.getPosition();
        in.setPosition(scratch.offset + scratch.length);
    }
}
