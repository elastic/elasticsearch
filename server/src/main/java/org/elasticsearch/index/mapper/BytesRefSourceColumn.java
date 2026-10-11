/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A {@link SourceColumn} layer whose values are UTF-8 strings, captured in original source order and written with
 * {@link XContentBuilder#utf8Value}. Used by keyword-family mappers for the {@code columnar_stored} direct source path: it holds one
 * document's values contiguously in a {@link BytesRefArray} and a compressed-sparse-row offset array, so a document's slice is
 * {@code [starts[doc], starts[doc + 1])}.
 */
final class BytesRefSourceColumn implements SourceColumn {

    private final int[] starts;
    private final BytesRefArray values;
    private final BytesRefBuilder spare = new BytesRefBuilder();

    /**
     * @param starts compressed-sparse-row offsets of length {@code docCount + 1}; document {@code d} owns values
     *               {@code [starts[d], starts[d + 1])}
     * @param values all documents' values in document order, each document's values in original source order
     */
    BytesRefSourceColumn(int[] starts, BytesRefArray values) {
        this.starts = starts;
        this.values = values;
    }

    @Override
    public int valueCount(int doc) {
        return starts[doc + 1] - starts[doc];
    }

    @Override
    public void writeValues(int doc, XContentBuilder builder) throws IOException {
        for (int i = starts[doc]; i < starts[doc + 1]; i++) {
            final BytesRef value = values.get(spare, i);
            builder.utf8Value(value.bytes, value.offset, value.length);
        }
    }
}
