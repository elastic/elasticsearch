/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;

/**
 * Numeric doc values over a {@link NumericWindow}: the Lucene iterator is a view over blocks that the ES|QL reader decoded. It also
 * reads a whole page of documents into a block for ES|QL ({@link #tryRead}), which skips the call for each document.
 */
final class ParquetNumericDocValues extends NumericDocValues implements BlockLoader.OptionalColumnAtATimeReader {

    private final NumericWindow window;
    private int doc = -1;
    /** The values of the documents of one {@link #tryRead}, which a block builder takes as an array. */
    private long[] buffer = new long[0];

    ParquetNumericDocValues(NumericWindow window) {
        this.window = window;
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        if (target >= window.maxDoc()) {
            return doc = DocIdSetIterator.NO_MORE_DOCS;
        }
        return doc = window.nextPresent(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        window.ensure(target);
        doc = target;
        return window.has(target);
    }

    @Override
    public long cost() {
        return window.maxDoc();
    }

    @Override
    public long longValue() {
        return window.value(doc);
    }

    /**
     * The values of the documents {@code docs[offset..]} as a block: the documents are in order, and they are read a page at a time. A
     * block of documents that all have a value is built from the array in one call.
     */
    @Override
    public BlockLoader.Block tryRead(
        BlockLoader.BlockFactory factory,
        BlockLoader.Docs docs,
        int offset,
        boolean nullsFiltered,
        BlockDocValuesReader.ToDouble toDouble,
        boolean toInt,
        boolean binaryMultiValuedFormat
    ) throws IOException {
        int count = docs.count() - offset;
        if (count <= 0 || binaryMultiValuedFormat) {
            return null;
        }
        if (buffer.length < count) {
            buffer = new long[Math.max(count, buffer.length * 2)];
        }
        boolean[] missing = null;
        for (int i = 0; i < count; i++) {
            int target = docs.get(offset + i);
            window.ensure(target);
            if (window.has(target)) {
                buffer[i] = window.value(target);
            } else {
                if (missing == null) {
                    missing = new boolean[count];
                }
                missing[i] = true;
            }
        }
        if (missing == null) {
            if (toDouble != null) {
                try (BlockLoader.SingletonDoubleBuilder builder = factory.singletonDoubles(count)) {
                    return builder.appendLongs(toDouble, buffer, 0, count).build();
                }
            }
            if (toInt) {
                try (BlockLoader.SingletonIntBuilder builder = factory.singletonInts(count)) {
                    return builder.appendLongs(buffer, 0, count).build();
                }
            }
            try (BlockLoader.SingletonLongBuilder builder = factory.singletonLongs(count)) {
                return builder.appendLongs(buffer, 0, count).build();
            }
        }
        if (toDouble != null) {
            try (BlockLoader.DoubleBuilder builder = factory.doubles(count)) {
                for (int i = 0; i < count; i++) {
                    if (missing[i]) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(toDouble.convert(buffer[i]));
                    }
                }
                return builder.build();
            }
        }
        if (toInt) {
            try (BlockLoader.IntBuilder builder = factory.ints(count)) {
                for (int i = 0; i < count; i++) {
                    if (missing[i]) {
                        builder.appendNull();
                    } else {
                        builder.appendInt(Math.toIntExact(buffer[i]));
                    }
                }
                return builder.build();
            }
        }
        try (BlockLoader.LongBuilder builder = factory.longs(count)) {
            for (int i = 0; i < count; i++) {
                if (missing[i]) {
                    builder.appendNull();
                } else {
                    builder.appendLong(buffer[i]);
                }
            }
            return builder.build();
        }
    }
}
