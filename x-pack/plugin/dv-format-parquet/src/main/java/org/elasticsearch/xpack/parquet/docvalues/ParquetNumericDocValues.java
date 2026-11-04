/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * Numeric doc values over an {@code INT32} or {@code INT64} column of external Parquet files. A document is a row. For an
 * {@code OPTIONAL} column a row without a value is not a document of the iterator, so it is skipped by {@link #nextDoc} and
 * {@link #advance} and reported by {@link #advanceExact}.
 */
final class ParquetNumericDocValues extends NumericDocValues {

    private final ColumnChunkReader reader;
    private final LongPageDecoder decoder;
    private final int maxDoc;

    private int doc = -1;

    ParquetNumericDocValues(
        java.util.List<ColumnChunkReader.Block> blocks,
        boolean required,
        int physicalType,
        int convertedType,
        Conversion conversion
    ) {
        this.decoder = new LongPageDecoder(physicalType, convertedType, conversion);
        this.reader = new ColumnChunkReader(blocks, required, decoder);
        this.maxDoc = reader.maxDoc();
    }

    @Override
    public long longValue() {
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
