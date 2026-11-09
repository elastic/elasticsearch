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
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

/**
 * The {@code _id} of a document in a Parquet-backed index is its row number in the index, which no file holds. Every document has one,
 * so the iterator is dense, and it builds the value from the position of the document.
 *
 * <p>The {@code _id} has no postings in such an index (see {@code index.mapping.id_field.indexed}), so a query on it is a scan of these
 * doc values. The scan is never needed: the id is a row number, so the document is at a known position of one segment, or there is none.
 * That is what {@link #tryTermEqualIterator} answers, and the scanning term query asks for it before it scans.
 */
final class SyntheticIdDocValues extends BinaryDocValues implements BlockLoader.OptionalColumnAtATimeReader {

    private final int maxDoc;
    private final long indexRowBase;
    private int doc = -1;

    SyntheticIdDocValues(int maxDoc, long indexRowBase) {
        this.maxDoc = maxDoc;
        this.indexRowBase = indexRowBase;
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() {
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) {
        return doc = target >= maxDoc ? DocIdSetIterator.NO_MORE_DOCS : target;
    }

    @Override
    public boolean advanceExact(int target) {
        doc = target;
        return target < maxDoc;
    }

    @Override
    public long cost() {
        return maxDoc;
    }

    @Override
    public BytesRef binaryValue() {
        return Uid.encodeId(Long.toString(indexRowBase + doc));
    }

    /** Not supported: the values are cheap to build one at a time, and a block read falls back to that. */
    @Override
    public BlockLoader.Block tryRead(
        BlockLoader.BlockFactory factory,
        BlockLoader.Docs docs,
        int offset,
        boolean nullsFiltered,
        BlockDocValuesReader.ToDouble toDouble,
        boolean toInt,
        boolean binaryMultiValuedFormat
    ) {
        return null;
    }

    /** The document whose {@code _id} is the term, found without a scan, or no document. */
    @Override
    public DocIdSetIterator tryTermEqualIterator(BytesRef term) {
        final String id;
        final long row;
        try {
            id = Uid.decodeId(term.bytes, term.offset, term.length);
            row = Long.parseLong(id);
        } catch (RuntimeException e) {
            // not the encoding of an id, or not a number: no row has this id
            return DocIdSetIterator.empty();
        }
        // "05" and "+5" are not the id of row 5
        if (Long.toString(row).equals(id) == false || row < indexRowBase || row - indexRowBase >= maxDoc) {
            return DocIdSetIterator.empty();
        }
        int doc = (int) (row - indexRowBase);
        return DocIdSetIterator.range(doc, doc + 1);
    }
}
