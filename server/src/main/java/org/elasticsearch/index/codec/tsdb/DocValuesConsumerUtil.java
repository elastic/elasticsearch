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
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Contains mainly forked code from {@link org.apache.lucene.codecs.DocValuesConsumer}.
 */
class DocValuesConsumerUtil {

    static final MergeStats UNSUPPORTED = new MergeStats(false, -1, -1);

    abstract static class TsdbDocValuesProducer extends EmptyDocValuesProducer {

        final MergeStats mergeStats;

        TsdbDocValuesProducer(MergeStats mergeStats) {
            this.mergeStats = mergeStats;
        }

    }

    record MergeStats(boolean supported, long sumNumValues, int sumNumDocsWithField) {}

    record FieldEntry(long docsWithFieldOffset, long numValues, int numDocsWithField) {}

    static MergeStats compatibleWithOptimizedMerge(
        boolean optimizedMergeEnabled,
        FieldInfo mergeFieldInfo,
        MergeState mergeState,
        BiFunction<ES87TSDBDocValuesProducer, String, FieldEntry> function
    ) {
        if (optimizedMergeEnabled == false
            || mergeState.needsIndexSort == false
            || mergeFieldInfo.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
            return UNSUPPORTED;
        }

        long sumNumValues = 0;
        int sumNumDocsWithField = 0;

        for (DocValuesProducer docValuesProducer : mergeState.docValuesProducers) {
            if (docValuesProducer instanceof ES87TSDBDocValuesProducer tsdbProducer) {
                if (tsdbProducer.version != ES87TSDBDocValuesFormat.VERSION_CURRENT) {
                    return UNSUPPORTED;
                }

                var entry = function.apply(tsdbProducer, mergeFieldInfo.name);
                assert entry != null;
                // TODO: support also fields with offsets
                if (entry.docsWithFieldOffset != -1) {
                    return UNSUPPORTED;
                }
                sumNumValues += entry.numValues;
                sumNumDocsWithField += entry.numDocsWithField;
            } else {
                return UNSUPPORTED;
            }
        }

        if (Math.toIntExact(sumNumValues) != sumNumDocsWithField) {
            return UNSUPPORTED;
        }
        // Documents marked as deleted should be rare. Maybe in the case of noop operation?
        for (int i = 0; i < mergeState.liveDocs.length; i++) {
            if (mergeState.liveDocs[i] != null) {
                return UNSUPPORTED;
            }
        }

        return new MergeStats(true, sumNumValues, sumNumDocsWithField);
    }

    static SortedNumericDocValues mergeSortedNumericValues(List<SortedNumericDocValuesSub> subs, boolean indexIsSorted) throws IOException {
        long cost = 0;
        for (SortedNumericDocValuesSub sub : subs) {
            cost += sub.values.cost();
        }
        final long finalCost = cost;

        final DocIDMerger<SortedNumericDocValuesSub> docIDMerger = DocIDMerger.of(subs, indexIsSorted);

        return new SortedNumericDocValues() {
            private int docID = -1;
            private SortedNumericDocValuesSub current;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = docIDMerger.next();
                if (current == null) {
                    docID = NO_MORE_DOCS;
                } else {
                    docID = current.mappedDocID;
                }
                return docID;
            }

            @Override
            public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }

            @Override
            public long nextValue() throws IOException {
                return current.values.nextValue();
            }

            @Override
            public int docValueCount() {
                return current.values.docValueCount();
            }

        };
    }

    static class SortedNumericDocValuesSub extends DocIDMerger.Sub {

        final SortedNumericDocValues values;
        int docID = -1;

        SortedNumericDocValuesSub(MergeState.DocMap docMap, SortedNumericDocValues values) {
            super(docMap);
            this.values = values;
            assert values.docID() == -1;
        }

        @Override
        public int nextDoc() throws IOException {
            return docID = values.nextDoc();
        }
    }

    static NumericDocValues mergeNumericValues(List<NumericDocValuesSub> subs, boolean indexIsSorted) throws IOException {
        long cost = 0;
        for (NumericDocValuesSub sub : subs) {
            cost += sub.values.cost();
        }
        final long finalCost = cost;

        final DocIDMerger<NumericDocValuesSub> docIDMerger = DocIDMerger.of(subs, indexIsSorted);

        return new NumericDocValues() {
            private int docID = -1;
            private NumericDocValuesSub current;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = docIDMerger.next();
                if (current == null) {
                    docID = NO_MORE_DOCS;
                } else {
                    docID = current.mappedDocID;
                }
                return docID;
            }

            @Override
            public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }

            @Override
            public long longValue() throws IOException {
                return current.values.longValue();
            }

        };
    }

    static class NumericDocValuesSub extends DocIDMerger.Sub {

        final NumericDocValues values;
        int docID = -1;

        NumericDocValuesSub(MergeState.DocMap docMap, NumericDocValues values) {
            super(docMap);
            this.values = values;
            assert values.docID() == -1;
        }

        @Override
        public int nextDoc() throws IOException {
            return docID = values.nextDoc();
        }
    }

    static SortedDocValues mergeSortedValues(List<SortedDocValuesSub> subs, boolean indexIsSorted, OrdinalMap map) throws IOException {
        long cost = 0;
        for (SortedDocValuesSub sub : subs) {
            cost += sub.values.cost();
        }
        final long finalCost = cost;

        final DocIDMerger<SortedDocValuesSub> docIDMerger = DocIDMerger.of(subs, indexIsSorted);

        return new SortedDocValues() {
            private int docID = -1;
            private SortedDocValuesSub current;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = docIDMerger.next();
                if (current == null) {
                    docID = NO_MORE_DOCS;
                } else {
                    docID = current.mappedDocID;
                }
                return docID;
            }

            @Override
            public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }

            @Override
            public int ordValue() throws IOException {
                int subOrd = current.values.ordValue();
                assert subOrd != -1;
                return (int) current.map.get(subOrd);
            }

            @Override
            public BytesRef lookupOrd(int ord) throws IOException {
                int segmentNumber = map.getFirstSegmentNumber(ord);
                int segmentOrd = (int) map.getFirstSegmentOrd(ord);
                return subs.get(segmentNumber).values.lookupOrd(segmentOrd);
            }

            @Override
            public int getValueCount() {
                return (int) map.getValueCount();
            }

            @Override
            public TermsEnum termsEnum() throws IOException {
                TermsEnum[] termsEnurmSubs = new TermsEnum[subs.size()];
                for (int sub = 0; sub < termsEnurmSubs.length; ++sub) {
                    termsEnurmSubs[sub] = subs.get(sub).values.termsEnum();
                }
                return new MergedTermsEnum(map, termsEnurmSubs);
            }
        };
    }

    static class SortedDocValuesSub extends DocIDMerger.Sub {

        LongValues map;
        final SortedDocValues values;

        SortedDocValuesSub(MergeState.DocMap docMap, SortedDocValues values) {
            super(docMap);
            this.values = values;
            assert values.docID() == -1;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
    }

    static SortedSetDocValues mergeSortedSetValues(List<SortedSetDocValuesSub> subs, boolean indexIsSorted, OrdinalMap map)
        throws IOException {
        long cost = 0;
        for (SortedSetDocValuesSub sub : subs) {
            cost += sub.values.cost();
        }
        final long finalCost = cost;

        final DocIDMerger<SortedSetDocValuesSub> docIDMerger = DocIDMerger.of(subs, indexIsSorted);

        return new SortedSetDocValues() {
            private int docID = -1;
            private SortedSetDocValuesSub current;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = docIDMerger.next();
                if (current == null) {
                    docID = NO_MORE_DOCS;
                } else {
                    docID = current.mappedDocID;
                }
                return docID;
            }

            @Override
            public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }

            @Override
            public long nextOrd() throws IOException {
                long subOrd = current.values.nextOrd();
                return current.map.get(subOrd);
            }

            @Override
            public int docValueCount() {
                return current.values.docValueCount();
            }

            @Override
            public BytesRef lookupOrd(long ord) throws IOException {
                int segmentNumber = map.getFirstSegmentNumber(ord);
                int segmentOrd = (int) map.getFirstSegmentOrd(ord);
                return subs.get(segmentNumber).values.lookupOrd(segmentOrd);
            }

            @Override
            public long getValueCount() {
                return map.getValueCount();
            }

            @Override
            public TermsEnum termsEnum() throws IOException {
                TermsEnum[] termsEnurmSubs = new TermsEnum[subs.size()];
                for (int sub = 0; sub < termsEnurmSubs.length; ++sub) {
                    termsEnurmSubs[sub] = subs.get(sub).values.termsEnum();
                }
                return new MergedTermsEnum(map, termsEnurmSubs);
            }
        };
    }

    static class SortedSetDocValuesSub extends DocIDMerger.Sub {

        LongValues map;
        final SortedSetDocValues values;

        SortedSetDocValuesSub(MergeState.DocMap docMap, SortedSetDocValues values) {
            super(docMap);
            this.values = values;
            assert values.docID() == -1;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
    }

    static class MergedTermsEnum extends BaseTermsEnum {

        private final TermsEnum[] subs;
        private final OrdinalMap ordinalMap;
        private final long valueCount;
        private long ord = -1;
        private BytesRef term;

        MergedTermsEnum(OrdinalMap ordinalMap, TermsEnum[] subs) {
            this.ordinalMap = ordinalMap;
            this.subs = subs;
            this.valueCount = ordinalMap.getValueCount();
        }

        @Override
        public BytesRef term() throws IOException {
            return term;
        }

        @Override
        public long ord() throws IOException {
            return ord;
        }

        @Override
        public BytesRef next() throws IOException {
            if (++ord >= valueCount) {
                return null;
            }
            final int subNum = ordinalMap.getFirstSegmentNumber(ord);
            final TermsEnum sub = subs[subNum];
            final long subOrd = ordinalMap.getFirstSegmentOrd(ord);
            do {
                term = sub.next();
            } while (sub.ord() < subOrd);
            assert sub.ord() == subOrd;
            return term;
        }

        @Override
        public AttributeSource attributes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekExact(long ord) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docFreq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long totalTermFreq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public TermState termState() throws IOException {
            throw new UnsupportedOperationException();
        }
    }

}
