/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;
import java.util.Arrays;

/**
 * Binary doc values over a {@link BytesWindow}: the Lucene iterator is a view over blocks that the ES|QL reader decoded. It also reads a
 * page of documents into a block for ES|QL ({@link #tryRead}): as ordinals into a dictionary of the distinct values when the pages are
 * dictionary pages, so that a consumer that groups or filters works on a distinct value once, and as plain values otherwise.
 */
final class ParquetBinaryDocValues extends BinaryDocValues implements BlockLoader.OptionalColumnAtATimeReader {

    private final BytesWindow window;
    private int doc = -1;
    /** The distinct values of a block that {@link #tryRead} reads as ordinals; kept to reuse its memory. */
    private BytesRefHash distinct;
    private int[] ordinalsBuffer = new int[0];
    /** For the entries of the dictionary of the page that is being read, the entry of the block's dictionary, or {@code -1}. */
    private int[] pageMap = new int[0];

    ParquetBinaryDocValues(BytesWindow window) {
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
    public BytesRef binaryValue() {
        return window.value(doc);
    }

    /** The window that this iterator reads through, for the tests. */
    BytesWindow window() {
        return window;
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
        int count = docs.count() - offset;
        // a column of single values only: the multi-valued framing, and a numeric view of a string, are not served here
        if (count <= 0 || binaryMultiValuedFormat || toDouble != null || toInt) {
            return null;
        }
        BlockLoader.Block ordinals = tryReadOrdinals(factory, docs, offset, count);
        if (ordinals != null) {
            return ordinals;
        }
        try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(count)) {
            for (int i = 0; i < count; i++) {
                int target = docs.get(offset + i);
                window.ensure(target);
                if (window.has(target)) {
                    builder.appendBytesRef(window.value(target));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    /**
     * The documents as ordinals into the distinct values that they hold, or {@code null} when one of them is in a page that has no
     * dictionary. A block whose documents hold one value is a constant, and a block of documents that hold nothing is a block of nulls.
     */
    private BlockLoader.Block tryReadOrdinals(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, int count)
        throws IOException {
        if (distinct == null) {
            distinct = new BytesRefHash();
        }
        // clear() leaves the hash uninitialized, and reinit() makes it usable again with the memory that it kept
        distinct.clear();
        distinct.reinit();
        if (ordinalsBuffer.length < count) {
            ordinalsBuffer = new int[Math.max(count, ordinalsBuffer.length * 2)];
        }
        int[] valueCounts = null;
        int values = 0;
        BytesWindow.BytesPage page = null;
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < count; i++) {
            int target = docs.get(offset + i);
            window.ensure(target);
            BytesWindow.BytesPage current = window.bytesPage();
            if (current.isDictionary() == false) {
                return null;
            }
            if (current.has(target) == false) {
                if (valueCounts == null) {
                    valueCounts = new int[count];
                    Arrays.fill(valueCounts, 0, i, 1);
                }
                continue;
            }
            if (valueCounts != null) {
                valueCounts[i] = 1;
            }
            if (current != page) {
                page = current;
                if (pageMap.length < page.dictionarySize()) {
                    pageMap = new int[Math.max(page.dictionarySize(), pageMap.length * 2)];
                }
                Arrays.fill(pageMap, 0, page.dictionarySize(), -1);
            }
            int entry = page.ordinal(target);
            int id = pageMap[entry];
            if (id < 0) {
                page.entry(entry, scratch);
                id = distinct.add(scratch);
                if (id < 0) {
                    id = -id - 1;
                }
                pageMap[entry] = id;
            }
            ordinalsBuffer[values++] = id;
        }
        if (values == 0) {
            return factory.constantNulls(count);
        }
        int size = distinct.size();
        if (size == 1 && valueCounts == null) {
            return factory.constantBytes(BytesRef.deepCopyOf(distinct.get(0, scratch)), count);
        }
        BytesRef[] dictionary = new BytesRef[size];
        for (int id = 0; id < size; id++) {
            dictionary[id] = distinct.get(id, new BytesRef());
        }
        return factory.buildOrdinalBytesRefDirect(ordinalsBuffer, values, valueCounts, count, dictionary, size);
    }
}
