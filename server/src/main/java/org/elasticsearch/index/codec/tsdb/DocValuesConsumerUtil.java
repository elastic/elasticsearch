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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.elasticsearch.index.codec.FilterDocValuesProducer;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.elasticsearch.index.engine.PruningMergePolicy;

/**
 * Contains logic to determine whether optimized merge can occur.
 */
public class DocValuesConsumerUtil {

    /** Sentinel indicating that optimized merge is not supported for the given field. */
    public static final MergeStats UNSUPPORTED = new MergeStats(Mode.UNSUPPORTED, -1, -1, -1, -1);

    /** How the documents of the segments being merged relate to the documents of the merged segment. */
    public enum Mode {
        /** Nothing about this merge can be shortcut. */
        UNSUPPORTED,
        /** The index sort interleaves the segments, which is the long-standing optimized merge case. */
        SORTED,
        /** The segments concatenate in order, so each one's documents stay contiguous in the merged segment. */
        CONCATENATING
    }

    /**
     * Pre-computed statistics for a field across all segments being merged.
     *
     * @param mode               how the segments relate to the merged segment
     * @param sumNumValues       total number of values across all segments
     * @param sumNumDocsWithField total number of documents with at least one value
     * @param minLength          minimum binary value length (binary fields only)
     * @param maxLength          maximum binary value length (binary fields only)
     */
    public record MergeStats(Mode mode, long sumNumValues, int sumNumDocsWithField, int minLength, int maxLength) {

        /**
         * Whether the optimized merge applies. Deliberately false for {@link Mode#CONCATENATING}, which only the
         * binary merge knows what to do with, so that every other field type behaves exactly as it always has.
         */
        public boolean supported() {
            return mode == Mode.SORTED;
        }

        /**
         * Whether the binary merge can use the pre-computed counts and, where the layouts allow it, copy whole
         * compressed blocks across. Binary gains from concatenating merges as well: with the segments in order every
         * source block's documents are trivially contiguous in the target, so every block is a splice candidate.
         */
        public boolean supportedForBinary() {
            return mode != Mode.UNSUPPORTED;
        }
    }

    /**
     * Determines whether an optimized merge can be performed for the given field by inspecting
     * segment metadata. An optimized merge is possible when all segments use TSDB doc values and
     * there are no deleted documents.
     *
     * <p>The resulting {@link Mode} records whether the index sort interleaves the segments or they simply
     * concatenate. Only binary doc values act on the latter, so {@link MergeStats#supported()} stays false for it
     * and every other field type keeps its previous behaviour.
     *
     * @param optimizedMergeEnabled whether optimized merge is enabled
     * @param mergeState            the merge state containing segment metadata
     * @param mergedFieldInfo       the field to check
     * @return pre-computed stats if optimized merge is possible, or {@link #UNSUPPORTED} otherwise
     */
    public static MergeStats compatibleWithOptimizedMerge(boolean optimizedMergeEnabled, MergeState mergeState, FieldInfo mergedFieldInfo) {
        if (optimizedMergeEnabled == false) {
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
            final FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergedFieldInfo.name);
            if (fieldInfo == null) {
                continue;
            }
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];

            if (docValuesProducer instanceof PruningMergePolicy.PruningDocValuesProducer pdv) {
                if (pdv.shouldPruneNumericDocValues(mergedFieldInfo.name)) {
                    return UNSUPPORTED;
                }
            }

            var perFieldReader = perFieldReader(docValuesProducer);
            if (perFieldReader != null) {
                var wrapped = perFieldReader.getDocValuesProducer(fieldInfo);
                if (wrapped == null) {
                    continue;
                }

                if (wrapped instanceof AbstractTSDBDocValuesProducer tsdbDocValuesProducer) {
                    switch (fieldInfo.getDocValuesType()) {
                        case NUMERIC -> {
                            var entry = tsdbDocValuesProducer.numerics.get(fieldInfo.number);
                            if (entry != null) {
                                sumNumValues += entry.numValues;
                                sumNumDocsWithField += entry.numDocsWithField;
                            } else {
                                assert false : "unexpectedly got no entry for field [" + fieldInfo.number + "\\" + fieldInfo.name + "]";
                                return UNSUPPORTED;
                            }
                        }
                        case SORTED_NUMERIC -> {
                            var entry = tsdbDocValuesProducer.sortedNumerics.get(fieldInfo.number);
                            if (entry != null) {
                                sumNumValues += entry.numValues;
                                sumNumDocsWithField += entry.numDocsWithField;
                            } else {
                                assert false : "unexpectedly got no entry for field [" + fieldInfo.number + "\\" + fieldInfo.name + "]";
                                return UNSUPPORTED;
                            }
                        }
                        case SORTED -> {
                            var entry = tsdbDocValuesProducer.sorted.get(fieldInfo.number);
                            if (entry != null) {
                                sumNumValues += entry.ordsEntry.numValues;
                                sumNumDocsWithField += entry.ordsEntry.numDocsWithField;
                            } else {
                                assert false : "unexpectedly got no entry for field [" + fieldInfo.number + "\\" + fieldInfo.name + "]";
                                return UNSUPPORTED;
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
                            } else {
                                assert false : "unexpectedly got no entry for field [" + fieldInfo.number + "\\" + fieldInfo.name + "]";
                                return UNSUPPORTED;
                            }
                        }
                        case BINARY -> {
                            var entry = tsdbDocValuesProducer.binaries.get(fieldInfo.number);
                            if (entry != null) {
                                sumNumDocsWithField += entry.numDocsWithField;
                                minLength = Math.min(minLength, entry.minLength);
                                maxLength = Math.max(maxLength, entry.maxLength);
                            } else {
                                assert false : "unexpectedly got no entry for field [" + fieldInfo.number + "\\" + fieldInfo.name + "]";
                                return UNSUPPORTED;
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

        final Mode mode = mergeState.needsIndexSort ? Mode.SORTED : Mode.CONCATENATING;
        return new MergeStats(mode, sumNumValues, sumNumDocsWithField, minLength, maxLength);
    }

    /**
     * Looks past the wrapper a merge puts around a segment's doc values producer and returns the per-field
     * reader underneath, or {@code null} when this producer is not one we can look inside — in which case the
     * segment is not backed by the TSDB format and no optimized path applies.
     */
    static XPerFieldDocValuesFormat.FieldsReader perFieldReader(DocValuesProducer docValuesProducer) {
        if (docValuesProducer instanceof FilterDocValuesProducer filterDocValuesProducer) {
            docValuesProducer = filterDocValuesProducer.getIn();
        }
        return docValuesProducer instanceof XPerFieldDocValuesFormat.FieldsReader perFieldReader ? perFieldReader : null;
    }

    /**
     * Whether binary values reach a merge unchanged from this producer, i.e. no wrapper in the chain rewrites
     * them on the way out.
     *
     * <p>Only matters to callers that read a segment's bytes directly rather than through {@code getBinary},
     * since those bypass any such wrapper. {@link PruningMergePolicy.PruningDocValuesProducer} rewrites numeric
     * doc values only, so binary passes through it untouched; any other wrapper we do not know about is treated
     * as opaque so that the caller falls back to reading values through the producer.
     */
    static boolean binaryValuesPassThroughUnchanged(DocValuesProducer docValuesProducer) {
        if (docValuesProducer instanceof FilterDocValuesProducer filterDocValuesProducer) {
            return filterDocValuesProducer instanceof PruningMergePolicy.PruningDocValuesProducer;
        }
        return true;
    }
}
