/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;

import static org.elasticsearch.index.mapper.blockloader.Warnings.registerSingleValueWarning;

public abstract class MultiValuedBinaryWithSeparateCountsLengthReader extends BlockDocValuesReader {
    private final Warnings warnings;
    private final NumericDocValues counts;
    private final BinaryDocValues values;

    MultiValuedBinaryWithSeparateCountsLengthReader(Warnings warnings, NumericDocValues counts, BinaryDocValues values) {
        this.warnings = warnings;
        this.counts = counts;
        this.values = values;
    }

    abstract int length(BytesRef bytesRef);

    public abstract String toString();

    @Override
    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
        throws IOException {
        int count = docs.count() - offset;
        if (count == 1) {
            return blockForSingleDoc(factory, docs.get(offset));
        }

        try (BlockLoader.IntBuilder builder = factory.ints(count)) {
            for (int i = offset; i < docs.count(); i++) {
                int doc = docs.get(i);
                appendLength(doc, builder);
            }
            return builder.build();
        }
    }

    @Override
    public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
        appendLength(docId, (BlockLoader.IntBuilder) builder);
    }

    @Override
    public int docId() {
        return counts.docID();
    }

    private void appendLength(int docId, BlockLoader.IntBuilder builder) throws IOException {
        if (counts.advanceExact(docId) == false) {
            builder.appendNull();
        } else {
            int valueCount = Math.toIntExact(counts.longValue());
            if (valueCount == 1) {
                boolean advanced = values.advanceExact(docId);
                assert advanced;

                BytesRef bytes = values.binaryValue();
                builder.appendInt(length(bytes));
            } else {
                registerSingleValueWarning(warnings);
                builder.appendNull();
            }
        }
    }

    private BlockLoader.Block blockForSingleDoc(BlockLoader.BlockFactory factory, int docId) throws IOException {
        if (counts.advanceExact(docId) == false) {
            return factory.constantNulls(1);
        } else {
            int valueCount = Math.toIntExact(counts.longValue());
            if (valueCount == 1) {
                boolean advanced = values.advanceExact(docId);
                assert advanced;

                BytesRef bytes = values.binaryValue();
                int length = length(bytes);
                return factory.constantInt(length, 1);
            } else {
                registerSingleValueWarning(warnings);
                return factory.constantNulls(1);
            }
        }
    }

}
