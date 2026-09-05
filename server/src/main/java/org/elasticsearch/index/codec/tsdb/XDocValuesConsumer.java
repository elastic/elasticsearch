/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.index.codec.tsdb.DocValuesConsumerUtil.MergeStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base subclass that allows pushing down {@link TsdbDocValuesProducer} instance so that subclasses can perform optimized merge.
 *
 * <p>The {@link #mergeSortedField} and {@link #mergeSortedSetField} overloads that accept a
 * {@link MergeStats} use a {@link MergeOrdinalMap} instead of Lucene's full
 * {@code OrdinalMap}. This eliminates the {@code globalOrdDeltas} and {@code firstSegments}
 * packed arrays (≈81% of the ordinal-map peak allocation) by re-running a PQ merge over fresh
 * sub-{@link TermsEnum}s on each term-dictionary pass instead of materialising a random-access
 * structure. See {@link MergeOrdinalMap} for the full rationale.
 *
 * <p>These overloads are only reached from the {@code MergeStats.supported()} path (index-sorted,
 * deletion-free), so the {@code BitsFilteredTermsEnum} / live-doc handling in Lucene's
 * {@code createOrdinalMapForSortedSetDV} is intentionally absent here.
 */
public abstract class XDocValuesConsumer extends DocValuesConsumer {

    /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
    protected XDocValuesConsumer() {}

    // -------------------------------------------------------------------------
    // Helper sub-classes (private copies of Lucene's package-private inner classes)
    // -------------------------------------------------------------------------

    /** Tracks state of one SORTED sub-reader during a merge doc pass. */
    private static final class SortedDocValuesSub extends DocIDMerger.Sub {
        final SortedDocValues values;
        final LongValues map;

        SortedDocValuesSub(MergeState.DocMap docMap, SortedDocValues values, LongValues map) {
            super(docMap);
            this.values = values;
            this.map = map;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
    }

    /** Tracks state of one SORTED_SET sub-reader during a merge doc pass. */
    private static final class SortedSetDocValuesSub extends DocIDMerger.Sub {
        final SortedSetDocValues values;
        final LongValues map;

        SortedSetDocValuesSub(MergeState.DocMap docMap, SortedSetDocValues values, LongValues map) {
            super(docMap);
            this.values = values;
            this.map = map;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
    }

    // -------------------------------------------------------------------------
    // Helper: open fresh TermsEnums for mergedTermsEnum / lookupOrd cursor
    // -------------------------------------------------------------------------

    private static TermsEnum[] freshSortedSubs(SortedDocValues[] dvs) throws IOException {
        TermsEnum[] subs = new TermsEnum[dvs.length];
        for (int i = 0; i < dvs.length; i++) {
            subs[i] = dvs[i].termsEnum();
        }
        return subs;
    }

    private static TermsEnum[] freshSortedSetSubs(List<SortedSetDocValues> list) throws IOException {
        TermsEnum[] subs = new TermsEnum[list.size()];
        for (int i = 0; i < list.size(); i++) {
            subs[i] = list.get(i).termsEnum();
        }
        return subs;
    }

    // -------------------------------------------------------------------------
    // Numeric / binary / sorted-numeric (unchanged from the original)
    // -------------------------------------------------------------------------

    /**
     * Merges the numeric docvalues from <code>MergeState</code>.
     *
     * <p>The default implementation calls {@link #addNumericField}, passing a DocValuesProducer that
     * merges and filters deleted documents on the fly.
     */
    public void mergeNumericField(MergeStats mergeStats, final FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
        addNumericField(mergeFieldInfo, new TsdbDocValuesProducer(mergeStats) {
            @Override
            public NumericDocValues getNumeric(FieldInfo fieldInfo) throws IOException {
                if (fieldInfo != mergeFieldInfo) {
                    throw new IllegalArgumentException("wrong fieldInfo");
                }
                return getMergedNumericDocValues(mergeState, mergeFieldInfo);
            }
        });
    }

    /**
     * Merges the binary docvalues from <code>MergeState</code>.
     *
     * <p>The default implementation calls {@link #addBinaryField}, passing a DocValuesProducer that
     * merges and filters deleted documents on the fly.
     */
    public void mergeBinaryField(MergeStats mergeStats, FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
        addBinaryField(mergeFieldInfo, new TsdbDocValuesProducer(mergeStats) {
            @Override
            public BinaryDocValues getBinary(FieldInfo fieldInfo) throws IOException {
                if (fieldInfo != mergeFieldInfo) {
                    throw new IllegalArgumentException("wrong fieldInfo");
                }
                return getMergedBinaryDocValues(mergeFieldInfo, mergeState);
            }
        });
    }

    /**
     * Merges the sorted docvalues from <code>toMerge</code>.
     *
     * <p>The default implementation calls {@link #addSortedNumericField}, passing iterables that
     * filter deleted documents.
     */
    public void mergeSortedNumericField(MergeStats mergeStats, FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
        addSortedNumericField(mergeFieldInfo, new TsdbDocValuesProducer(mergeStats) {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo fieldInfo) throws IOException {
                if (fieldInfo != mergeFieldInfo) {
                    throw new IllegalArgumentException("wrong FieldInfo");
                }
                return getMergedSortedNumericDocValues(mergeFieldInfo, mergeState);
            }
        });
    }

    // -------------------------------------------------------------------------
    // mergeSortedField — ES-local, uses MergeOrdinalMap
    // -------------------------------------------------------------------------

    /**
     * Merges the sorted docvalues from <code>toMerge</code>.
     *
     * <p>Uses {@link MergeOrdinalMap} instead of Lucene's {@code OrdinalMap} to avoid building
     * the {@code globalOrdDeltas} and {@code firstSegments} packed arrays (~81% of peak memory).
     * The merged {@link TermsEnum} is produced by re-running a PQ merge over fresh sub-enumerators
     * on each call, which is I/O-neutral since the same iterators would be opened anyway.
     *
     * <p>Only called from the {@code MergeStats.supported()} path (deletion-free, index-sorted).
     */
    public void mergeSortedField(MergeStats mergeStats, FieldInfo fieldInfo, final MergeState mergeState) throws IOException {
        // Collect SortedDocValues for each segment (no liveDocs filter: supported() guarantees
        // deletion-free, so no BitsFilteredTermsEnum is needed)
        final SortedDocValues[] sortedDvs = new SortedDocValues[mergeState.docValuesProducers.length];
        final long[] weights = new long[sortedDvs.length];
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            SortedDocValues dv = null;
            DocValuesProducer dvProducer = mergeState.docValuesProducers[i];
            if (dvProducer != null) {
                FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
                if (readerField != null && readerField.getDocValuesType() == DocValuesType.SORTED) {
                    dv = dvProducer.getSorted(readerField);
                }
            }
            sortedDvs[i] = (dv != null) ? dv : DocValues.emptySorted();
            weights[i] = sortedDvs[i].getValueCount();
        }

        // Build MergeOrdinalMap — opens one TermsEnum per sub (I/O-neutral)
        final TermsEnum[] mapSubs = freshSortedSubs(sortedDvs);
        final MergeOrdinalMap map = new MergeOrdinalMap(mapSubs, weights);

        addSortedField(fieldInfo, new TsdbDocValuesProducer(mergeStats) {
            @Override
            public SortedDocValues getSorted(FieldInfo fieldInfoIn) throws IOException {
                if (fieldInfoIn != fieldInfo) {
                    throw new IllegalArgumentException("wrong FieldInfo");
                }
                return buildMergedSortedDocValues(fieldInfo, mergeState, map, sortedDvs);
            }
        });
    }

    /**
     * Builds the merged {@link SortedDocValues} for one field.
     *
     * <p>Opens fresh sub doc-value instances from {@code mergeState.docValuesProducers} for the
     * doc-pass (so callers may invoke {@link DocValuesProducer#getSorted} multiple times to obtain
     * independent iterators). The {@code sortedDvsForTerms} array is used only for
     * {@link TermsEnum} and {@link SortedDocValues#lookupOrd} access, which is independent of
     * doc-iteration state.
     */
    private SortedDocValues buildMergedSortedDocValues(
        FieldInfo fieldInfo,
        MergeState mergeState,
        MergeOrdinalMap map,
        SortedDocValues[] sortedDvsForTerms
    ) throws IOException {
        // Fresh doc-pass sub instances (independent of sortedDvsForTerms)
        final List<SortedDocValuesSub> subs = new ArrayList<>();
        long cost = 0;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            SortedDocValues dv = null;
            DocValuesProducer dvProducer = mergeState.docValuesProducers[i];
            if (dvProducer != null) {
                FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
                if (readerField != null && readerField.getDocValuesType() == DocValuesType.SORTED) {
                    dv = dvProducer.getSorted(readerField);
                }
            }
            if (dv == null) {
                dv = DocValues.emptySorted();
            }
            cost += dv.cost();
            subs.add(new SortedDocValuesSub(mergeState.docMaps[i], dv, map.getGlobalOrds(i)));
        }
        final long finalCost = cost;
        final DocIDMerger<SortedDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

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
                docID = (current == null) ? NO_MORE_DOCS : current.mappedDocID;
                return docID;
            }

            @Override
            public int ordValue() throws IOException {
                return (int) current.map.get(current.values.ordValue());
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
                return finalCost;
            }

            @Override
            public int getValueCount() {
                return (int) map.getValueCount();
            }

            // Forward-only cursor for lookupOrd. Restarts from ord 0 on backward access
            // (expected once per merge in the single-valued SortedSetDV path).
            private TermsEnum lookupCursor = map.mergedTermsEnum(freshSortedSubs(sortedDvsForTerms));
            private long cursorOrd = 0;

            @Override
            public BytesRef lookupOrd(int ord) throws IOException {
                if (cursorOrd == map.getValueCount()) {
                    cursorOrd = 0;
                    lookupCursor = map.mergedTermsEnum(freshSortedSubs(sortedDvsForTerms));
                }
                assert cursorOrd == ord;

                cursorOrd++;
                return lookupCursor.next();
            }

            /**
             * Returns a fresh PQ-based terms enumeration, bypassing the {@code lookupOrd} cursor.
             * Called twice per sorted-DV merge: once by {@code addTermsDict} and once by
             * {@code writeTermsIndex}; each call gets an independent iterator.
             */
            @Override
            public TermsEnum termsEnum() throws IOException {
                return map.mergedTermsEnum(freshSortedSubs(sortedDvsForTerms));
            }
        };
    }

    // -------------------------------------------------------------------------
    // mergeSortedSetField — ES-local, uses MergeOrdinalMap
    // -------------------------------------------------------------------------

    /**
     * Merges the sortedset docvalues from <code>toMerge</code>.
     *
     * <p>Uses {@link MergeOrdinalMap} instead of Lucene's {@code OrdinalMap} to avoid building
     * the {@code globalOrdDeltas} and {@code firstSegments} packed arrays (~81% of peak memory).
     * The merged {@link TermsEnum} is produced by re-running a PQ merge over fresh sub-enumerators
     * on each call, which is I/O-neutral since the same iterators would be opened anyway.
     *
     * <p>Only called from the {@code MergeStats.supported()} path (deletion-free, index-sorted).
     */
    public void mergeSortedSetField(MergeStats mergeStats, FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
        // selectLeavesToMerge is protected-static in DocValuesConsumer and handles absent fields
        final List<SortedSetDocValues> toMerge = selectLeavesToMerge(mergeFieldInfo, mergeState);

        // Build MergeOrdinalMap — no liveDocs filter: supported() guarantees deletion-free
        final TermsEnum[] mapSubs = freshSortedSetSubs(toMerge);
        final long[] weights = new long[toMerge.size()];
        for (int i = 0; i < toMerge.size(); i++) {
            weights[i] = toMerge.get(i).getValueCount();
        }
        final MergeOrdinalMap map = new MergeOrdinalMap(mapSubs, weights);

        addSortedSetField(mergeFieldInfo, new TsdbDocValuesProducer(mergeStats) {
            @Override
            public SortedSetDocValues getSortedSet(FieldInfo fieldInfo) throws IOException {
                if (fieldInfo != mergeFieldInfo) {
                    throw new IllegalArgumentException("wrong FieldInfo");
                }
                return buildMergedSortedSetDocValues(mergeFieldInfo, mergeState, map, toMerge);
            }
        });
    }

    /**
     * Builds the merged {@link SortedSetDocValues} for one field.
     *
     * <p>Opens fresh sub doc-value instances from {@code mergeState.docValuesProducers} for the
     * doc-pass (so callers may invoke {@link DocValuesProducer#getSortedSet} multiple times to
     * obtain independent iterators). The {@code toMerge} list is used only for {@link TermsEnum}
     * and {@link SortedSetDocValues#lookupOrd} access, which is independent of doc-iteration state.
     *
     * <p>The {@code lookupOrd} implementation uses a forward-only cursor over a PQ-merged
     * {@link TermsEnum}. The cursor restarts on backward access, which occurs exactly once per
     * merge in the single-valued field path (between {@code addTermsDict} and
     * {@code writeTermsIndex}): the restart is logged at DEBUG and costs one additional forward
     * scan through the term dictionary (O(G), the same as one terms-dict pass).
     */
    private SortedSetDocValues buildMergedSortedSetDocValues(
        FieldInfo mergeFieldInfo,
        MergeState mergeState,
        MergeOrdinalMap map,
        List<SortedSetDocValues> toMerge
    ) throws IOException {
        // Fresh doc-pass sub instances
        final List<SortedSetDocValuesSub> subs = new ArrayList<>();
        long cost = 0;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            SortedSetDocValues values = null;
            DocValuesProducer dvProducer = mergeState.docValuesProducers[i];
            if (dvProducer != null) {
                FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                if (readerField != null && readerField.getDocValuesType() == DocValuesType.SORTED_SET) {
                    values = dvProducer.getSortedSet(readerField);
                }
            }
            if (values == null) {
                values = DocValues.emptySortedSet();
            }
            cost += values.cost();
            subs.add(new SortedSetDocValuesSub(mergeState.docMaps[i], values, map.getGlobalOrds(i)));
        }
        final long finalCost = cost;
        final DocIDMerger<SortedSetDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

        return new SortedSetDocValues() {
            private int docID = -1;
            private SortedSetDocValuesSub currentSub;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                currentSub = docIDMerger.next();
                docID = (currentSub == null) ? NO_MORE_DOCS : currentSub.mappedDocID;
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
            public long nextOrd() throws IOException {
                return currentSub.map.get(currentSub.values.nextOrd());
            }

            @Override
            public int docValueCount() {
                return currentSub.values.docValueCount();
            }

            @Override
            public long cost() {
                return finalCost;
            }

            @Override
            public long getValueCount() {
                return map.getValueCount();
            }

            // Forward-only cursor for lookupOrd. Restarts from ord 0 on backward access
            // (expected once per single-valued merge: between addTermsDict and writeTermsIndex).
            private TermsEnum lookupCursor = map.mergedTermsEnum(freshSortedSetSubs(toMerge));
            private long cursorOrd = 0;

            @Override
            public BytesRef lookupOrd(long ord) throws IOException {
                if (cursorOrd == map.getValueCount()) {
                    cursorOrd = 0;
                    lookupCursor = map.mergedTermsEnum(freshSortedSetSubs(toMerge));
                }
                assert cursorOrd == ord;

                cursorOrd++;
                return lookupCursor.next();
            }

            /**
             * Returns a fresh PQ-based terms enumeration independent of the {@code lookupOrd}
             * cursor. Called up to twice per sorted-set-DV merge: once by {@code addTermsDict}
             * and once by {@code writeTermsIndex}; each call gets an independent iterator.
             */
            @Override
            public TermsEnum termsEnum() throws IOException {
                return map.mergedTermsEnum(freshSortedSetSubs(toMerge));
            }
        };
    }

}
