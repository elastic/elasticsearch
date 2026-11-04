/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.List;

/**
 * Binary doc values over a {@code BYTE_ARRAY} column of external Parquet files, with the bytes of a value as they are in the file. For
 * a string column that is the UTF-8 of the string, which is what a single-valued {@code keyword} field of a columnar index reads as
 * its binary doc values. A row without a value ({@code OPTIONAL} column) is not a document of the iterator.
 */
final class ParquetBinaryDocValues extends BinaryDocValues {

    private final ColumnChunkReader reader;
    private final BytesPageDecoder decoder;
    private final int maxDoc;

    private int doc = -1;

    ParquetBinaryDocValues(List<ColumnChunkReader.Block> blocks, boolean required) {
        this.decoder = new BytesPageDecoder();
        this.reader = new ColumnChunkReader(blocks, required, decoder);
        this.maxDoc = reader.maxDoc();
    }

    @Override
    public BytesRef binaryValue() {
        return decoder.value(doc - reader.pageStartDoc());
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = DocIdSetIterator.NO_MORE_DOCS;
            return false;
        }
        reader.load(target);
        doc = target;
        return reader.present(target);
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        // an iterator that is exhausted stays exhausted, and doc + 1 would overflow
        return doc == DocIdSetIterator.NO_MORE_DOCS ? doc : advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        while (target < maxDoc) {
            reader.load(target);
            int pageEnd = reader.pageStartDoc() + reader.pageRows();
            for (; target < pageEnd; target++) {
                if (reader.present(target)) {
                    doc = target;
                    return doc;
                }
            }
        }
        doc = DocIdSetIterator.NO_MORE_DOCS;
        return doc;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
