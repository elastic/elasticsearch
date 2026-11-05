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
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Binary doc values over a {@code BYTE_ARRAY} column of external Parquet files, with the bytes of a value as they are in the file. For
 * a string column that is the UTF-8 of the string, which is what a single-valued {@code keyword} field of a columnar index reads as
 * its binary doc values. A row without a value ({@code OPTIONAL} column) is not a document of the iterator.
 *
 * <p>It also serves ES|QL a block of values at a time ({@link #tryRead}), which finds the page of a document once for all the
 * documents of that page, and copies their bytes into one array.
 */
final class ParquetBinaryDocValues extends BinaryDocValues implements BlockLoader.OptionalColumnAtATimeReader {

    private final ColumnChunkReader reader;
    private final BytesPageDecoder decoder;
    private final boolean required;
    private final int maxDoc;

    private int doc = -1;

    ParquetBinaryDocValues(List<ColumnChunkReader.Block> blocks, boolean required) {
        this.decoder = new BytesPageDecoder();
        this.required = required;
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
        // these are the values of a single valued field, which is not the format that a multi valued binary field has
        if (end - offset <= 0 || binaryMultiValuedFormat || toDouble != null || toInt || docs.mayContainDuplicates()) {
            return null;
        }
        if (required || nullsFiltered) {
            return readEveryDocument(factory, docs, offset, end, nullsFiltered);
        }
        return readWithMissingValues(factory, docs, offset, end);
    }

    /**
     * Every document has a value. The block takes one array with all the bytes and the offsets of the values in it, so the values of
     * the documents are copied into one growing array, a page at a time. Returns {@code null}, so that the caller reads one document at
     * a time, for documents that are not in ascending order or outside of the segment.
     */
    private BlockLoader.Block readEveryDocument(
        BlockLoader.BlockFactory factory,
        BlockLoader.Docs docs,
        int offset,
        int end,
        boolean nullsFiltered
    ) throws IOException {
        int count = end - offset;
        int[] offsets = new int[count + 1];
        byte[] bytes = new byte[Math.max(1024, Math.min(count, 1 << 16) * 32)];
        int used = 0;
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
            byte[] page = decoder.pageBytes();
            int last = doc - 1;
            for (; i < end; i++) {
                int d = docs.get(i);
                if (d >= pageEnd) {
                    break;
                }
                if (d <= last) {
                    return null;
                }
                if (required == false && nullsFiltered && reader.present(d) == false) {
                    return null; // the caller promised a value for every document
                }
                int row = d - pageStart;
                int length = decoder.valueLength(row);
                long needed = (long) used + length;
                if (needed > Integer.MAX_VALUE - 8) {
                    return null;
                }
                if (needed > bytes.length) {
                    bytes = Arrays.copyOf(bytes, (int) Math.max(needed, Math.min((long) bytes.length * 2, Integer.MAX_VALUE - 8)));
                }
                System.arraycopy(page, decoder.valueStart(row), bytes, used, length);
                offsets[i - offset] = used;
                used += length;
                last = d;
            }
            previous = last;
        }
        offsets[count] = used;
        try (BlockLoader.SingletonBytesRefBuilder builder = factory.singletonBytesRefs(count)) {
            // the builder keeps the arrays, so they must not be longer than the bytes
            builder.appendBytesRefs(used == bytes.length ? bytes : Arrays.copyOf(bytes, used), offsets);
            this.doc = previous;
            return builder.build();
        }
    }

    /** A column that may miss values and no promise from the caller: a builder with nulls, filled one value at a time from the pages. */
    private BlockLoader.Block readWithMissingValues(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, int end)
        throws IOException {
        try (BlockLoader.BytesRefBuilder builder = factory.bytesRefsFromDocValues(end - offset)) {
            int previous = -1;
            for (int i = offset; i < end; i++) {
                int doc = docs.get(i);
                if (doc <= previous || doc >= maxDoc) {
                    return null;
                }
                reader.load(doc);
                if (reader.present(doc)) {
                    builder.appendBytesRef(decoder.value(doc - reader.pageStartDoc()));
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
