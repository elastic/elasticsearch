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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
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
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mostly contains forked code from {@link org.apache.lucene.codecs.DocValuesConsumer}.
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

    static MergeStats compatibleWithOptimizedMerge(boolean optimizedMergeEnabled, MergeState mergeState, FieldInfo fieldInfo)
        throws IOException {
        if (optimizedMergeEnabled == false || mergeState.needsIndexSort == false) {
            return UNSUPPORTED;
        }

        // Documents marked as deleted should be rare. Maybe in the case of noop operation?
        for (int i = 0; i < mergeState.liveDocs.length; i++) {
            if (mergeState.liveDocs[i] != null) {
                return UNSUPPORTED;
            }
        }

        long sumNumValues = 0;
        int sumNumDocsWithField = 0;

        // TODO bring back codec version check? (per field doc values producer sits between ES87TSDBDocValuesConsumer)
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            switch (fieldInfo.getDocValuesType()) {
                case NUMERIC -> {
                    var numeric = docValuesProducer.getNumeric(fieldInfo);
                    if (numeric instanceof ES87TSDBDocValuesProducer.BaseNumericDocValues baseNumeric) {
                        var entry = baseNumeric.entry;
                        sumNumValues += entry.numValues;
                        int numDocsWithField = getNumDocsWithField(entry, mergeState.maxDocs[i]);
                        sumNumDocsWithField += numDocsWithField;
                    } else if (numeric != null) {
                        return UNSUPPORTED;
                    }
                }
                case SORTED_NUMERIC -> {
                    var sortedNumeric = docValuesProducer.getSortedNumeric(fieldInfo);
                    if (sortedNumeric instanceof ES87TSDBDocValuesProducer.BaseSortedNumericDocValues baseSortedNumericDocValues) {
                        var entry = baseSortedNumericDocValues.entry;
                        sumNumValues += entry.numValues;
                        sumNumDocsWithField += entry.numDocsWithField;
                    } else {
                        var singleton = DocValues.unwrapSingleton(sortedNumeric);
                        if (singleton instanceof ES87TSDBDocValuesProducer.BaseNumericDocValues baseNumeric) {
                            var entry = baseNumeric.entry;
                            sumNumValues += entry.numValues;
                            // In this case the numDocsWithField doesn't get recorded in meta:
                            int numDocsWithField = getNumDocsWithField(entry, mergeState.maxDocs[i]);
                            sumNumDocsWithField += numDocsWithField;
                        } else if (sortedNumeric != null) {
                            return UNSUPPORTED;
                        }
                    }
                }
                case SORTED -> {
                    var sorted = docValuesProducer.getSorted(fieldInfo);
                    if (sorted instanceof ES87TSDBDocValuesProducer.BaseSortedDocValues baseSortedDocValues) {
                        var entry = baseSortedDocValues.entry;
                        sumNumValues += entry.ordsEntry.numValues;
                        // In this case the numDocsWithField doesn't get recorded in meta:v
                        int numDocsWithField = getNumDocsWithField(entry.ordsEntry, mergeState.maxDocs[i]);
                        sumNumDocsWithField += numDocsWithField;
                    } else if (sorted != null) {
                        return UNSUPPORTED;
                    }
                }
                case SORTED_SET -> {
                    var sortedSet = docValuesProducer.getSortedSet(fieldInfo);
                    if (sortedSet instanceof ES87TSDBDocValuesProducer.BaseSortedSetDocValues baseSortedSet) {
                        var entry = baseSortedSet.entry;
                        sumNumValues += entry.ordsEntry.numValues;
                        sumNumDocsWithField += entry.ordsEntry.numDocsWithField;
                    } else {
                        var singleton = DocValues.unwrapSingleton(sortedSet);
                        if (singleton instanceof ES87TSDBDocValuesProducer.BaseSortedDocValues baseSorted) {
                            var entry = baseSorted.entry;
                            sumNumValues += entry.ordsEntry.numValues;
                            // In this case the numDocsWithField doesn't get recorded in meta:
                            int numDocsWithField = getNumDocsWithField(entry.ordsEntry, mergeState.maxDocs[i]);
                            sumNumDocsWithField += numDocsWithField;
                        } else if (sortedSet != null) {
                            return UNSUPPORTED;
                        }
                    }
                }
                default -> throw new IllegalStateException("unexpected doc values producer type: " + fieldInfo.getDocValuesType());
            }
        }

        return new MergeStats(true, sumNumValues, sumNumDocsWithField);
    }

    private static int getNumDocsWithField(ES87TSDBDocValuesProducer.NumericEntry entry, int maxDoc) {
        // In this case the numDocsWithField doesn't get recorded in meta:
        if (entry.docsWithFieldOffset == -2) {
            return 0;
        } else if (entry.docsWithFieldOffset == -1) {
            return maxDoc;
        } else {
            // numDocsWithField doesn't matter in this case:
            return 1;
        }
    }

    static DocValuesProducer mergeNumericProducer(MergeStats mergeStats, FieldInfo mergeFieldInfo, MergeState mergeState) {
        return new TsdbDocValuesProducer(mergeStats) {

            @Override
            public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                List<NumericDocValuesSub> subs = new ArrayList<>();
                assert mergeState.docMaps.length == mergeState.docValuesProducers.length;
                for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
                    NumericDocValues values = null;
                    DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                    if (docValuesProducer != null) {
                        FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                        if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
                            values = docValuesProducer.getNumeric(readerFieldInfo);
                        }
                    }
                    if (values != null) {
                        subs.add(new NumericDocValuesSub(mergeState.docMaps[i], values));
                    }
                }

                return mergeNumericValues(subs, mergeState.needsIndexSort);
            }
        };
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

    static DocValuesProducer mergeSortedNumericProducer(MergeStats mergeStats, FieldInfo mergeFieldInfo, MergeState mergeState) {
        return new TsdbDocValuesProducer(mergeStats) {

            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
                List<DocValuesConsumerUtil.SortedNumericDocValuesSub> subs = new ArrayList<>();
                assert mergeState.docMaps.length == mergeState.docValuesProducers.length;
                for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
                    SortedNumericDocValues values = null;
                    DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                    if (docValuesProducer != null) {
                        FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                        if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
                            values = docValuesProducer.getSortedNumeric(readerFieldInfo);
                        }
                    }
                    if (values != null) {
                        subs.add(new SortedNumericDocValuesSub(mergeState.docMaps[i], values));
                    }
                }
                return mergeSortedNumericValues(subs, mergeState.needsIndexSort);
            }
        };
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

    static DocValuesProducer mergeSortedProducer(MergeStats mergeStats, FieldInfo mergeFieldInfo, MergeState mergeState) {
        return new TsdbDocValuesProducer(mergeStats) {

            @Override
            public SortedDocValues getSorted(FieldInfo field) throws IOException {
                List<SortedDocValuesSub> subs = new ArrayList<>();
                assert mergeState.docMaps.length == mergeState.docValuesProducers.length;

                TermsEnum[] liveTerms = new TermsEnum[mergeState.docValuesProducers.length];
                long[] weights = new long[liveTerms.length];
                for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
                    SortedDocValues values = null;
                    DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                    if (docValuesProducer != null) {
                        FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                        if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED) {
                            values = docValuesProducer.getSorted(readerFieldInfo);
                        }
                    }
                    if (values == null) {
                        values = DocValues.emptySorted();
                    }

                    liveTerms[i] = values.termsEnum();
                    weights[i] = values.getValueCount();
                    subs.add(new SortedDocValuesSub(mergeState.docMaps[i], values));
                }

                final OrdinalMap map = OrdinalMap.build(null, liveTerms, weights, PackedInts.COMPACT);
                for (int i = 0; i < subs.size(); i++) {
                    subs.get(i).map = map.getGlobalOrds(i);
                }
                return mergeSortedValues(subs, mergeState.needsIndexSort, map);
            }
        };
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

    static DocValuesProducer mergeSortedSetProducer(MergeStats mergeStats, FieldInfo mergeFieldInfo, MergeState mergeState) {
        return new TsdbDocValuesProducer(mergeStats) {

            @Override
            public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
                List<SortedSetDocValuesSub> subs = new ArrayList<>();
                assert mergeState.docMaps.length == mergeState.docValuesProducers.length;

                TermsEnum[] liveTerms = new TermsEnum[mergeState.docValuesProducers.length];
                long[] weights = new long[liveTerms.length];
                for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
                    SortedSetDocValues values = null;
                    DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                    if (docValuesProducer != null) {
                        FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                        if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
                            values = docValuesProducer.getSortedSet(readerFieldInfo);
                        }
                    }
                    if (values == null) {
                        values = DocValues.emptySortedSet();
                    }
                    liveTerms[i] = values.termsEnum();
                    weights[i] = values.getValueCount();
                    subs.add(new SortedSetDocValuesSub(mergeState.docMaps[i], values));
                }

                final OrdinalMap map = OrdinalMap.build(null, liveTerms, weights, PackedInts.COMPACT);
                for (int i = 0; i < subs.size(); i++) {
                    subs.get(i).map = map.getGlobalOrds(i);
                }
                return mergeSortedSetValues(subs, mergeState.needsIndexSort, map);
            }
        };
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
