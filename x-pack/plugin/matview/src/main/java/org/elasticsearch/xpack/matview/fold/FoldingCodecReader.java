/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.FilterSortedNumericDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.elasticsearch.index.codec.FilterDocValuesProducer;

import java.io.IOException;

/**
 * Merge-time view of an input segment: hides the documents folded into another representative and serves the
 * reduced values for the reduced fields of the documents that stay. Everything else, including the key fields,
 * which are also the index sort fields and are read for every document by the sorter, passes through unchanged.
 */
final class FoldingCodecReader extends FilterCodecReader {
    private final FoldSpec spec;
    private final FoldStrategy.ReaderFold fold;

    FoldingCodecReader(CodecReader in, FoldSpec spec, FoldStrategy.ReaderFold fold) {
        super(in);
        this.spec = spec;
        this.fold = fold;
    }

    @Override
    public Bits getLiveDocs() {
        Bits foldedLive = fold.foldedLive();
        return foldedLive != null ? foldedLive : in.getLiveDocs();
    }

    @Override
    public int numDocs() {
        return fold.numDocs();
    }

    @Override
    public DocValuesProducer getDocValuesReader() {
        return new FoldingDocValuesProducer(in.getDocValuesReader());
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return null;
    }

    private boolean hidden(int doc) {
        Bits foldedLive = fold.foldedLive();
        return foldedLive != null && foldedLive.get(doc) == false;
    }

    /**
     * Serves reduced fields. Every call returns a fresh iterator because the doc values writer pulls a field
     * several times per merge, and only {@code nextDoc} style access is supported, like Lucene's own merged
     * iterators.
     */
    private final class FoldingDocValuesProducer extends FilterDocValuesProducer {
        FoldingDocValuesProducer(DocValuesProducer in) {
            super(in);
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            int idx = spec.indexOf(field.name);
            if (idx < 0) {
                return super.getNumeric(field);
            }
            return new FoldedNumeric(super.getNumeric(field), idx);
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            int idx = spec.indexOf(field.name);
            if (idx < 0) {
                return super.getSortedNumeric(field);
            }
            SortedNumericDocValues raw = super.getSortedNumeric(field);
            NumericDocValues single = DocValues.unwrapSingleton(raw);
            if (single != null) {
                // keep the singleton shape so the writer takes its single-valued fast path
                return DocValues.singleton(new FoldedNumeric(single, idx));
            }
            return new FoldedSortedNumeric(raw, idx);
        }

        private final class FoldedNumeric extends FilterNumericDocValues {
            private final int field;

            FoldedNumeric(NumericDocValues in, int field) {
                super(in);
                this.field = field;
            }

            @Override
            public int nextDoc() throws IOException {
                int doc;
                do {
                    doc = in.nextDoc();
                } while (doc != DocIdSetIterator.NO_MORE_DOCS && hidden(doc));
                return doc;
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
            public long longValue() throws IOException {
                return fold.reducedValue(docID(), field, in.longValue());
            }
        }

        private final class FoldedSortedNumeric extends FilterSortedNumericDocValues {
            private final int field;

            FoldedSortedNumeric(SortedNumericDocValues in, int field) {
                super(in);
                this.field = field;
            }

            @Override
            public int nextDoc() throws IOException {
                int doc;
                do {
                    doc = in.nextDoc();
                } while (doc != DocIdSetIterator.NO_MORE_DOCS && hidden(doc));
                if (doc != DocIdSetIterator.NO_MORE_DOCS && in.docValueCount() != 1) {
                    throw new IllegalStateException(
                        "reduced field ["
                            + spec.fields().get(field).name()
                            + "] must be single valued, doc ["
                            + doc
                            + "] has ["
                            + in.docValueCount()
                            + "] values"
                    );
                }
                return doc;
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
            public long nextValue() throws IOException {
                return fold.reducedValue(docID(), field, in.nextValue());
            }
        }
    }
}
