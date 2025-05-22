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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.elasticsearch.index.codec.FilterDocValuesProducer;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;

/**
 * Contains logic to determine whether optimized merge can occur.
 */
class DocValuesConsumerUtil {

    static final MergeStats UNSUPPORTED = new MergeStats(false, -1, -1, -1, -1);

    record MergeStats(boolean supported, long sumNumValues, int sumNumDocsWithField, int minLength, int maxLength) {}

    static MergeStats compatibleWithOptimizedMerge(boolean optimizedMergeEnabled, MergeState mergeState, FieldInfo fieldInfo) {
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
        int minLength = Integer.MAX_VALUE;
        int maxLength = 0;

        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer instanceof FilterDocValuesProducer filterDocValuesProducer) {
                docValuesProducer = filterDocValuesProducer.getIn();
            }

            if (docValuesProducer instanceof XPerFieldDocValuesFormat.FieldsReader perFieldReader) {
                var wrapped = perFieldReader.getDocValuesProducer(fieldInfo);
                if (wrapped == null) {
                    continue;
                }

                if (wrapped instanceof ES819TSDBDocValuesProducer tsdbDocValuesProducer) {
                    switch (fieldInfo.getDocValuesType()) {
                        case NUMERIC -> {
                            var entry = tsdbDocValuesProducer.numerics.get(fieldInfo.number);
                            if (entry != null) {
                                sumNumValues += entry.numValues;
                                sumNumDocsWithField += entry.numDocsWithField;
                            }
                        }
                        case SORTED_NUMERIC -> {
                            var entry = tsdbDocValuesProducer.sortedNumerics.get(fieldInfo.number);
                            if (entry != null) {
                                sumNumValues += entry.numValues;
                                sumNumDocsWithField += entry.numDocsWithField;
                            }
                        }
                        case SORTED -> {
                            var entry = tsdbDocValuesProducer.sorted.get(fieldInfo.number);
                            if (entry != null) {
                                sumNumValues += entry.ordsEntry.numValues;
                                sumNumDocsWithField += entry.ordsEntry.numDocsWithField;
                            }
                        }
                        case SORTED_SET -> {
                            var entry = tsdbDocValuesProducer.sortedSets.get(fieldInfo.number);
                            if (entry != null) {
                                if (entry.singleValueEntry != null) {
                                    sumNumValues += entry.singleValueEntry.ordsEntry.numValues;
                                    sumNumDocsWithField += entry.singleValueEntry.ordsEntry.numDocsWithField;
                                } else {
                                    sumNumValues += entry.ordsEntry.numValues;
                                    sumNumDocsWithField += entry.ordsEntry.numDocsWithField;
                                }
                            }
                        }
                        case BINARY -> {
                            var entry = tsdbDocValuesProducer.binaries.get(fieldInfo.number);
                            if (entry != null) {
                                sumNumDocsWithField += entry.numDocsWithField;
                                minLength = Math.min(minLength, entry.minLength);
                                maxLength = Math.max(maxLength, entry.maxLength);
                            }
                        }
                        default -> throw new IllegalStateException("unexpected doc values producer type: " + fieldInfo.getDocValuesType());
                    }
                } else {
                    return UNSUPPORTED;
                }
            } else {
                return UNSUPPORTED;
            }
        }

        return new MergeStats(true, sumNumValues, sumNumDocsWithField, minLength, maxLength);
    }

}
