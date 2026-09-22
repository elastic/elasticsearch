/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A fork of {@link org.apache.lucene.codecs.DocValuesConsumer#getMergedBinaryDocValues} that keeps
 * the current source segment's reader reachable through {@link #currentValues()}. Lucene's version
 * hides the per-segment sub behind a private {@code BinaryDocValuesSub}, which prevents the merge
 * loop from asking a source reader for a block it could copy verbatim.
 *
 * <p>This class mirrors the pattern in
 * {@code org.elasticsearch.columnar.ColumNARDocValuesConsumer}, which does the same for its own
 * merge path.
 */
final class MergedBinaryDocValues extends BinaryDocValues {

    /**
     * One source segment's cursor in merged doc order. Exposes {@link #values} so the merge loop
     * can reach the per-segment {@link AbstractTSDBDocValuesProducer.TSDBBinaryDocValues} and call
     * {@link AbstractTSDBDocValuesProducer.TSDBBinaryDocValues#rawSingleValueBlock}.
     */
    private static final class BinaryMergeSub extends DocIDMerger.Sub {
        private final BinaryDocValues values;

        BinaryMergeSub(MergeState.DocMap docMap, BinaryDocValues values) {
            super(docMap);
            this.values = values;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
    }

    /**
     * Creates a {@link MergedBinaryDocValues} for the given field across all source segments in
     * {@code mergeState}, faithfully reproducing the sub-selection logic of
     * {@link org.apache.lucene.codecs.DocValuesConsumer#getMergedBinaryDocValues}.
     */
    static MergedBinaryDocValues create(FieldInfo mergeFieldInfo, MergeState mergeState) throws IOException {
        List<BinaryMergeSub> subs = new ArrayList<>();
        long cost = 0;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            DocValuesProducer producer = mergeState.docValuesProducers[i];
            if (producer == null) {
                continue;
            }
            FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
            if (readerFieldInfo == null || readerFieldInfo.getDocValuesType() != DocValuesType.BINARY) {
                continue;
            }
            BinaryDocValues values = producer.getBinary(readerFieldInfo);
            if (values == null) {
                continue;
            }
            cost += values.cost();
            subs.add(new BinaryMergeSub(mergeState.docMaps[i], values));
        }
        return new MergedBinaryDocValues(subs, mergeState.needsIndexSort, cost);
    }

    private final DocIDMerger<BinaryMergeSub> merger;
    private final long cost;
    private BinaryMergeSub current;
    private int docID = -1;

    private MergedBinaryDocValues(List<BinaryMergeSub> subs, boolean needsIndexSort, long cost) throws IOException {
        this.merger = DocIDMerger.of(subs, needsIndexSort);
        this.cost = cost;
    }

    @Override
    public int docID() {
        return docID;
    }

    @Override
    public int nextDoc() throws IOException {
        current = merger.next();
        docID = current == null ? DocIdSetIterator.NO_MORE_DOCS : current.mappedDocID;
        return docID;
    }

    @Override
    public int advance(int target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        return cost;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        return current.values.binaryValue();
    }

    /**
     * Returns the current source segment's reader if it is a
     * {@link AbstractTSDBDocValuesProducer.TSDBBinaryDocValues} (and therefore supports
     * {@link AbstractTSDBDocValuesProducer.TSDBBinaryDocValues#rawSingleValueBlock}), or
     * {@code null} if the reader belongs to a non-TSDB codec or is not a compressed binary reader.
     *
     * <p>Must be called after {@link #nextDoc()} and before the next {@link #nextDoc()} call.
     */
    AbstractTSDBDocValuesProducer.TSDBBinaryDocValues currentValues() {
        if (current == null) {
            return null;
        }
        return current.values instanceof AbstractTSDBDocValuesProducer.TSDBBinaryDocValues tsdb ? tsdb : null;
    }
}
