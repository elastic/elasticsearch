/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

class VersionTopNEncoder extends SortableTopNEncoder {
    @Override
    public int encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        // TODO versions can contain nul so we need to delegate to the utf-8 encoder for the utf-8 parts of a version
        for (int i = value.offset; i < value.length; i++) {
            if (value.bytes[i] == UTF8TopNEncoder.TERMINATOR) {
                throw new IllegalArgumentException("Can't sort versions containing nul");
            }
        }
        bytesRefBuilder.append(value);
        bytesRefBuilder.append(UTF8TopNEncoder.TERMINATOR);
        return value.length + 1;
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        int i = bytes.offset;
        while (bytes.bytes[i] != UTF8TopNEncoder.TERMINATOR) {
            i++;
        }
        scratch.bytes = bytes.bytes;
        scratch.offset = bytes.offset;
        scratch.length = i - bytes.offset;
        bytes.offset += scratch.length + 1;
        bytes.length -= scratch.length + 1;
        return scratch;
    }

    @Override
    public String toString() {
        return "VersionTopNEncoder";
    }

    @Override
    public TopNEncoder toSortable() {
        return this;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }
}
