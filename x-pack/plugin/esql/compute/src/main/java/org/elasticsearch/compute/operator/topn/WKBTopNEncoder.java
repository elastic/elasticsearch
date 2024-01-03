/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

/**
 * Encodes WKB bytes[]. Spatial types are unsortable by definition.
 */
final class WKBTopNEncoder extends UnsortableTopNEncoder {
    @Override
    public int encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        final int offset = bytesRefBuilder.length();
        encodeVInt(value.length, bytesRefBuilder);
        bytesRefBuilder.append(value);
        return bytesRefBuilder.length() - offset;
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        final int len = decodeVInt(bytes);
        scratch.bytes = bytes.bytes;
        scratch.offset = bytes.offset;
        scratch.length = len;
        bytes.offset += len;
        bytes.length -= len;
        return scratch;
    }

    @Override
    public String toString() {
        return "WKBTopNEncoder";
    }

    @Override
    public TopNEncoder toSortable() {
        throw new UnsupportedOperationException("Cannot sort spatial types");
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }
}
