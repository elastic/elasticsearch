/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;

import java.io.IOException;

/**
 * Contains logic to determine whether optimized merge can occur.
 */
class DocValuesConsumerUtil {

    static final MergeStats UNSUPPORTED = new MergeStats(false, -1, -1);

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

        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            switch (fieldInfo.getDocValuesType()) {
                case NUMERIC -> {
                    var numeric = docValuesProducer.getNumeric(fieldInfo);
                    // (checking instance type as serves as a version check)
                    if (numeric instanceof ES819TSDBDocValuesProducer.BaseNumericDocValues baseNumeric) {
                        var entry = baseNumeric.entry;
                        sumNumValues += entry.numValues;
                        sumNumDocsWithField += entry.numDocsWithField;
                    } else if (numeric != null) {
                        return UNSUPPORTED;
                    }
                }
                case SORTED_NUMERIC -> {
                    var sortedNumeric = docValuesProducer.getSortedNumeric(fieldInfo);
                    if (sortedNumeric instanceof ES819TSDBDocValuesProducer.BaseSortedNumericDocValues baseSortedNumericDocValues) {
                        var entry = baseSortedNumericDocValues.entry;
                        sumNumValues += entry.numValues;
                        sumNumDocsWithField += entry.numDocsWithField;
                    } else {
                        var singleton = DocValues.unwrapSingleton(sortedNumeric);
                        if (singleton instanceof ES819TSDBDocValuesProducer.BaseNumericDocValues baseNumeric) {
                            var entry = baseNumeric.entry;
                            sumNumValues += entry.numValues;
                            sumNumDocsWithField += entry.numDocsWithField;
                        } else if (sortedNumeric != null) {
                            return UNSUPPORTED;
                        }
                    }
                }
                case SORTED -> {
                    var sorted = docValuesProducer.getSorted(fieldInfo);
                    if (sorted instanceof ES819TSDBDocValuesProducer.BaseSortedDocValues baseSortedDocValues) {
                        var entry = baseSortedDocValues.entry;
                        sumNumValues += entry.ordsEntry.numValues;
                        sumNumDocsWithField += entry.ordsEntry.numDocsWithField;
                    } else if (sorted != null) {
                        return UNSUPPORTED;
                    }
                }
                case SORTED_SET -> {
                    var sortedSet = docValuesProducer.getSortedSet(fieldInfo);
                    if (sortedSet instanceof ES819TSDBDocValuesProducer.BaseSortedSetDocValues baseSortedSet) {
                        var entry = baseSortedSet.entry;
                        sumNumValues += entry.ordsEntry.numValues;
                        sumNumDocsWithField += entry.ordsEntry.numDocsWithField;
                    } else {
                        var singleton = DocValues.unwrapSingleton(sortedSet);
                        if (singleton instanceof ES819TSDBDocValuesProducer.BaseSortedDocValues baseSorted) {
                            var entry = baseSorted.entry;
                            sumNumValues += entry.ordsEntry.numValues;
                            sumNumDocsWithField += entry.ordsEntry.numDocsWithField;
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

}
