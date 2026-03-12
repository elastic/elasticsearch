/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.codec.tsdb.es819.DocValuesConsumerUtil.MergeStats;

import java.io.IOException;
import java.util.List;

/**
 * Base subclass that allows pushing down {@link TsdbDocValuesProducer} instance so that subclasses can perform optimized merge.
 */
public abstract class XDocValuesConsumer extends DocValuesConsumer {

    /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
    protected XDocValuesConsumer() {}

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

    /**
     * Merges the sorted docvalues from <code>toMerge</code>.
     *
     * <p>The default implementation calls {@link #addSortedField}, passing an Iterable that merges
     * ordinals and values and filters deleted documents .
     */
    public void mergeSortedField(MergeStats mergeStats, FieldInfo fieldInfo, final MergeState mergeState) throws IOException {
        // step 1: iterate thru each sub and mark terms still in use
        // step 2: create ordinal map (this conceptually does the "merging")
        final OrdinalMap map = createOrdinalMapForSortedDV(fieldInfo, mergeState);
        // step 3: add field
        addSortedField(fieldInfo, new TsdbDocValuesProducer(mergeStats) {
            @Override
            public SortedDocValues getSorted(FieldInfo fieldInfoIn) throws IOException {
                if (fieldInfoIn != fieldInfo) {
                    throw new IllegalArgumentException("wrong FieldInfo");
                }
                return getMergedSortedSetDocValues(fieldInfo, mergeState, map);
            }
        });
    }

    /**
     * Merges the sortedset docvalues from <code>toMerge</code>.
     *
     * <p>The default implementation calls {@link #addSortedSetField}, passing an Iterable that merges
     * ordinals and values and filters deleted documents .
     */
    public void mergeSortedSetField(MergeStats mergeStats, FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
        // step 1: iterate thru each sub and mark terms still in use
        // step 2: create ordinal map (this conceptually does the "merging")
        List<SortedSetDocValues> toMerge = selectLeavesToMerge(mergeFieldInfo, mergeState);
        OrdinalMap map = createOrdinalMapForSortedSetDV(toMerge, mergeState);
        // step 3: add field
        addSortedSetField(mergeFieldInfo, new TsdbDocValuesProducer(mergeStats) {
            @Override
            public SortedSetDocValues getSortedSet(FieldInfo fieldInfo) throws IOException {
                if (fieldInfo != mergeFieldInfo) {
                    throw new IllegalArgumentException("wrong FieldInfo");
                }
                return getMergedSortedSetDocValues(mergeFieldInfo, mergeState, map, toMerge);
            }
        });
    }

}
