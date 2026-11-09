/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;

/**
 * Numeric doc values over an {@code INT32} or {@code INT64} column of external Parquet files. A document is a row. For an
 * {@code OPTIONAL} column a row without a value is not a document of the iterator, so it is skipped by {@link #nextDoc} and
 * {@link #advance} and reported by {@link #advanceExact}.
 *
 * <p>It also serves ES|QL a block of values at a time ({@link #tryRead}), which finds the page of the first document once and copies
 * the values of all documents of that page, instead of finding the page again for every document.
 */
final class ParquetNumericDocValues extends NumericDocValues implements BlockLoader.OptionalColumnAtATimeReader {

    private final ColumnChunkReader reader;
    private final LongPageDecoder decoder;
    private final boolean required;
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
        this.required = required;
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

    /** Receives the values of a run of documents of one page. */
    @FunctionalInterface
    private interface RunSink {
        void values(long[] values, int from, int length);
    }

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
        int end = docs.count();
        int count = end - offset;
        if (count <= 0 || binaryMultiValuedFormat || docs.mayContainDuplicates() || (toInt && toDouble != null)) {
            return null;
        }
        if (required || nullsFiltered) {
            return readEveryDocument(factory, docs, offset, end, nullsFiltered, toDouble, toInt);
        }
        return readWithMissingValues(factory, docs, offset, end, toDouble, toInt);
    }

    /**
     * Every document has a value (the column is required, or the caller says so), so the values go to a builder that takes arrays of
     * values and has no nulls. Returns {@code null}, so that the caller reads one document at a time, for documents that are not in
     * ascending order or outside of the segment.
     */
    private BlockLoader.Block readEveryDocument(
        BlockLoader.BlockFactory factory,
        BlockLoader.Docs docs,
        int offset,
        int end,
        boolean nullsFiltered,
        BlockDocValuesReader.ToDouble toDouble,
        boolean toInt
    ) throws IOException {
        int count = end - offset;
        final BlockLoader.Builder builder;
        final RunSink sink;
        if (toDouble != null) {
            BlockLoader.SingletonDoubleBuilder doubles = factory.singletonDoubles(count);
            builder = doubles;
            sink = (values, from, length) -> doubles.appendLongs(toDouble, values, from, length);
        } else if (toInt) {
            BlockLoader.SingletonIntBuilder ints = factory.singletonInts(count);
            builder = ints;
            sink = ints::appendLongs;
        } else {
            BlockLoader.SingletonLongBuilder longs = factory.singletonLongs(count);
            builder = longs;
            sink = longs::appendLongs;
        }
        try (builder) {
            long[] gathered = null;
            int previous = -1;
            int i = offset;
            while (i < end) {
                int doc = docs.get(i);
                if (doc <= previous || doc >= maxDoc) {
                    return null;
                }
                reader.load(doc);
                int pageStart = reader.pageStartDoc();
                int pageEnd = pageStart + reader.pageRows();
                // the documents of this page: a run of the documents that we were asked for
                int j = i + 1;
                int last = doc;
                while (j < end) {
                    int next = docs.get(j);
                    if (next >= pageEnd) {
                        break;
                    }
                    if (next <= last) {
                        return null;
                    }
                    last = next;
                    j++;
                }
                if (required == false && nullsFiltered) {
                    for (int k = i; k < j; k++) {
                        if (reader.present(docs.get(k)) == false) {
                            return null; // the caller promised a value for every document
                        }
                    }
                }
                long[] page = decoder.pageValues();
                int run = j - i;
                if (last - doc == run - 1) {
                    // consecutive documents: the values are a slice of the page
                    sink.values(page, doc - pageStart, run);
                } else {
                    if (gathered == null || gathered.length < run) {
                        gathered = new long[Math.max(run, 256)];
                    }
                    for (int k = 0; k < run; k++) {
                        gathered[k] = page[docs.get(i + k) - pageStart];
                    }
                    sink.values(gathered, 0, run);
                }
                previous = last;
                i = j;
            }
            this.doc = previous;
            return builder.build();
        }
    }

    /** A column that may miss values and no promise from the caller: a builder with nulls, filled one value at a time from the pages. */
    private BlockLoader.Block readWithMissingValues(
        BlockLoader.BlockFactory factory,
        BlockLoader.Docs docs,
        int offset,
        int end,
        BlockDocValuesReader.ToDouble toDouble,
        boolean toInt
    ) throws IOException {
        int count = end - offset;
        final BlockLoader.Builder builder;
        final java.util.function.LongConsumer value;
        if (toDouble != null) {
            BlockLoader.DoubleBuilder doubles = factory.doublesFromDocValues(count);
            builder = doubles;
            value = v -> doubles.appendDouble(toDouble.convert(v));
        } else if (toInt) {
            BlockLoader.IntBuilder ints = factory.intsFromDocValues(count);
            builder = ints;
            value = v -> ints.appendInt(Math.toIntExact(v));
        } else {
            BlockLoader.LongBuilder longs = factory.longsFromDocValues(count);
            builder = longs;
            value = longs::appendLong;
        }
        try (builder) {
            int previous = -1;
            for (int i = offset; i < end; i++) {
                int doc = docs.get(i);
                if (doc <= previous || doc >= maxDoc) {
                    return null;
                }
                reader.load(doc);
                if (reader.present(doc)) {
                    value.accept(decoder.value(doc - reader.pageStartDoc()));
                } else {
                    builder.appendNull();
                }
                previous = doc;
            }
            this.doc = previous;
            return builder.build();
        }
    }
}
