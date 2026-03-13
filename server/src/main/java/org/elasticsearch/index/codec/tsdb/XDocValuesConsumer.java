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
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.codec.tsdb.DocValuesConsumerUtil.MergeStats;

import java.io.IOException;
import java.util.List;

/**
 * Abstract base class that provides merge methods accepting pre-computed {@link DocValuesSource}
 * instances, allowing subclasses to perform optimized merge.
 */
public abstract class XDocValuesConsumer extends DocValuesConsumer {

    /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
    protected XDocValuesConsumer() {}

    /**
     * Merges the numeric doc values from {@link MergeState}.
     *
     * <p>The default implementation calls {@link #addNumericField}, passing a {@link DocValuesSource}
     * that merges and filters deleted documents on the fly.
     *
     * @param mergeStats     pre-computed merge stats for the field
     * @param mergeFieldInfo the field being merged
     * @param mergeState     the merge state
     * @throws IOException if an I/O error occurs
     */
    public void mergeNumericField(final MergeStats mergeStats, final FieldInfo mergeFieldInfo, final MergeState mergeState)
        throws IOException {
        addNumericField(mergeFieldInfo, new DocValuesSource(mergeStats) {
            @Override
            public NumericDocValues getNumeric(final FieldInfo fieldInfo) throws IOException {
                if (fieldInfo != mergeFieldInfo) {
                    throw new IllegalArgumentException("wrong fieldInfo");
                }
                return getMergedNumericDocValues(mergeState, mergeFieldInfo);
            }
        });
    }

    /**
     * Merges the binary doc values from {@link MergeState}.
     *
     * <p>The default implementation calls {@link #addBinaryField}, passing a {@link DocValuesSource}
     * that merges and filters deleted documents on the fly.
     *
     * @param mergeStats     pre-computed merge stats for the field
     * @param mergeFieldInfo the field being merged
     * @param mergeState     the merge state
     * @throws IOException if an I/O error occurs
     */
    public void mergeBinaryField(final MergeStats mergeStats, final FieldInfo mergeFieldInfo, final MergeState mergeState)
        throws IOException {
        addBinaryField(mergeFieldInfo, new DocValuesSource(mergeStats) {
            @Override
            public BinaryDocValues getBinary(final FieldInfo fieldInfo) throws IOException {
                if (fieldInfo != mergeFieldInfo) {
                    throw new IllegalArgumentException("wrong fieldInfo");
                }
                return getMergedBinaryDocValues(mergeFieldInfo, mergeState);
            }
        });
    }

    /**
     * Merges the sorted numeric doc values from {@link MergeState}.
     *
     * <p>The default implementation calls {@link #addSortedNumericField}, passing a {@link DocValuesSource}
     * that merges and filters deleted documents on the fly.
     *
     * @param mergeStats     pre-computed merge stats for the field
     * @param mergeFieldInfo the field being merged
     * @param mergeState     the merge state
     * @throws IOException if an I/O error occurs
     */
    public void mergeSortedNumericField(final MergeStats mergeStats, final FieldInfo mergeFieldInfo, final MergeState mergeState)
        throws IOException {
        addSortedNumericField(mergeFieldInfo, new DocValuesSource(mergeStats) {
            @Override
            public SortedNumericDocValues getSortedNumeric(final FieldInfo fieldInfo) throws IOException {
                if (fieldInfo != mergeFieldInfo) {
                    throw new IllegalArgumentException("wrong FieldInfo");
                }
                return getMergedSortedNumericDocValues(mergeFieldInfo, mergeState);
            }
        });
    }

    /**
     * Merges the sorted doc values from {@link MergeState}.
     *
     * <p>The default implementation calls {@link #addSortedField}, passing a {@link DocValuesSource}
     * that merges ordinals and values and filters deleted documents on the fly.
     *
     * @param mergeStats pre-computed merge stats for the field
     * @param fieldInfo  the field being merged
     * @param mergeState the merge state
     * @throws IOException if an I/O error occurs
     */
    public void mergeSortedField(final MergeStats mergeStats, final FieldInfo fieldInfo, final MergeState mergeState) throws IOException {
        final OrdinalMap map = createOrdinalMapForSortedDV(fieldInfo, mergeState);
        addSortedField(fieldInfo, new DocValuesSource(mergeStats) {
            @Override
            public SortedDocValues getSorted(final FieldInfo fieldInfoIn) throws IOException {
                if (fieldInfoIn != fieldInfo) {
                    throw new IllegalArgumentException("wrong FieldInfo");
                }
                return getMergedSortedSetDocValues(fieldInfo, mergeState, map);
            }
        });
    }

    /**
     * Merges the sorted set doc values from {@link MergeState}.
     *
     * <p>The default implementation calls {@link #addSortedSetField}, passing a {@link DocValuesSource}
     * that merges ordinals and values and filters deleted documents on the fly.
     *
     * @param mergeStats     pre-computed merge stats for the field
     * @param mergeFieldInfo the field being merged
     * @param mergeState     the merge state
     * @throws IOException if an I/O error occurs
     */
    public void mergeSortedSetField(final MergeStats mergeStats, final FieldInfo mergeFieldInfo, final MergeState mergeState)
        throws IOException {
        List<SortedSetDocValues> toMerge = selectLeavesToMerge(mergeFieldInfo, mergeState);
        OrdinalMap map = createOrdinalMapForSortedSetDV(toMerge, mergeState);
        addSortedSetField(mergeFieldInfo, new DocValuesSource(mergeStats) {
            @Override
            public SortedSetDocValues getSortedSet(final FieldInfo fieldInfo) throws IOException {
                if (fieldInfo != mergeFieldInfo) {
                    throw new IllegalArgumentException("wrong FieldInfo");
                }
                return getMergedSortedSetDocValues(mergeFieldInfo, mergeState, map, toMerge);
            }
        });
    }

}
