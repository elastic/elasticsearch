/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.startree;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.startree.StarTreeConfig;
import org.elasticsearch.index.mapper.startree.StarTreeGroupingField;
import org.elasticsearch.index.mapper.startree.StarTreeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class that builds star-trees during segment merges.
 */
public final class StarTreeMergeBuilder {

    private static final Logger logger = LogManager.getLogger(StarTreeMergeBuilder.class);

    private StarTreeMergeBuilder() {}

    /**
     * Build all configured star-trees for the merged segment.
     *
     * @param state the segment write state
     * @param starTreeConfig the star-tree configuration
     * @param mergeState the merge state containing source segment data
     * @param isDoubleFieldLookup function to check if a field is a double/float type
     */
    public static void buildStarTrees(
        SegmentWriteState state,
        StarTreeConfig starTreeConfig,
        MergeState mergeState,
        java.util.function.Function<String, Boolean> isDoubleFieldLookup
    ) {
        logger.debug("buildStarTrees called for segment [{}], starTreeConfig has {} star-trees",
            state.segmentInfo.name, starTreeConfig.getStarTrees().size());
        for (Map.Entry<String, StarTreeConfig.StarTreeFieldConfig> entry : starTreeConfig.getStarTrees().entrySet()) {
            String starTreeName = entry.getKey();
            StarTreeConfig.StarTreeFieldConfig config = entry.getValue();

            try {
                buildStarTree(starTreeName, config, state, mergeState, isDoubleFieldLookup);
                logger.debug("Built star-tree [{}] for segment [{}]", starTreeName, state.segmentInfo.name);
            } catch (Exception e) {
                logger.warn("Failed to build star-tree [{}] for segment [{}]: {}", starTreeName, state.segmentInfo.name, e.getMessage(), e);
                // Don't fail the merge, just skip star-tree building
            }
        }
    }

    private static void buildStarTree(
        String starTreeName,
        StarTreeConfig.StarTreeFieldConfig config,
        SegmentWriteState state,
        MergeState mergeState,
        java.util.function.Function<String, Boolean> isDoubleFieldLookup
    ) throws IOException {
        List<StarTreeGroupingField> groupingFields = config.getGroupingFields();
        List<StarTreeValue> values = config.getValues();

        // Check if we have all required fields in the merge
        if (hasRequiredFields(groupingFields, values, mergeState) == false) {
            logger.debug("Skipping star-tree [{}] build - missing required fields", starTreeName);
            return;
        }
        logger.debug("Building star-tree [{}], groupingFields={}, values={}",
            starTreeName, groupingFields, values);

        // Collect doc values from all segments being merged
        // For grouping fields, we support both SORTED (keyword) and SORTED_NUMERIC (date/numeric)
        Map<String, SortedDocValues> sortedGroupingFields = new HashMap<>();
        Map<String, SortedNumericDocValues> numericGroupingFields = new HashMap<>();
        Map<String, SortedNumericDocValues> valueDocValues = new HashMap<>();

        int foundGroupingFields = 0;

        // Get merged doc values from the merge state producers
        for (StarTreeGroupingField gf : groupingFields) {
            String fieldName = gf.getField();
            DocValuesType dvType = getDocValuesType(fieldName, mergeState);
            logger.debug("Grouping field [{}] has doc values type [{}]", fieldName, dvType);

            if (dvType == DocValuesType.SORTED || dvType == DocValuesType.SORTED_SET) {
                // Keyword field - handle both SORTED and SORTED_SET
                SortedDocValues sorted = getMergedSortedDocValues(fieldName, mergeState);
                if (sorted != null) {
                    sortedGroupingFields.put(fieldName, sorted);
                    foundGroupingFields++;
                    logger.debug("Found sorted doc values for grouping field [{}]", fieldName);
                } else {
                    logger.debug("No sorted doc values for grouping field [{}]", fieldName);
                }
            } else if (dvType == DocValuesType.SORTED_NUMERIC || dvType == DocValuesType.NUMERIC) {
                // Date or numeric field
                SortedNumericDocValues numeric = getMergedSortedNumericDocValues(fieldName, mergeState);
                if (numeric != null) {
                    numericGroupingFields.put(fieldName, numeric);
                    foundGroupingFields++;
                    logger.debug("Found numeric doc values for grouping field [{}]", fieldName);
                } else {
                    logger.debug("No numeric doc values for grouping field [{}]", fieldName);
                }
            } else {
                logger.debug("Unsupported doc values type [{}] for grouping field [{}]", dvType, fieldName);
            }
        }

        for (StarTreeValue value : values) {
            SortedNumericDocValues numeric = getMergedSortedNumericDocValues(value.getField(), mergeState);
            if (numeric != null) {
                valueDocValues.put(value.getField(), numeric);
            }
        }

        // Build and write the star-tree
        if (foundGroupingFields == groupingFields.size() && valueDocValues.size() == values.size()) {
            logger.debug("Star-tree [{}]: all fields found, writing star-tree", starTreeName);
            try (StarTreeWriter writer = new StarTreeWriter(state, config, isDoubleFieldLookup)) {
                writer.build(sortedGroupingFields, numericGroupingFields, valueDocValues);
            }
            logger.debug("Star-tree [{}]: finished writing star-tree", starTreeName);
        } else {
            logger.debug("Missing fields for star-tree [{}]: got {} of {} grouping fields, {} of {} values",
                starTreeName,
                foundGroupingFields, groupingFields.size(),
                valueDocValues.size(), values.size());
        }
    }

    private static DocValuesType getDocValuesType(String fieldName, MergeState mergeState) {
        for (FieldInfo fi : mergeState.mergeFieldInfos) {
            if (fi.name.equals(fieldName)) {
                return fi.getDocValuesType();
            }
        }
        return DocValuesType.NONE;
    }

    private static boolean hasRequiredFields(
        List<StarTreeGroupingField> groupingFields,
        List<StarTreeValue> values,
        MergeState mergeState
    ) {
        // Log all available fields for debugging
        logger.debug("Available fields in merge state:");
        for (FieldInfo fi : mergeState.mergeFieldInfos) {
            logger.debug("  Field: [{}], docValuesType=[{}]", fi.name, fi.getDocValuesType());
        }

        for (StarTreeGroupingField gf : groupingFields) {
            boolean found = hasFieldInMerge(gf.getField(), mergeState);
            logger.debug("Checking grouping field [{}]: found={}", gf.getField(), found);
            if (found == false) {
                return false;
            }
        }
        for (StarTreeValue value : values) {
            boolean found = hasFieldInMerge(value.getField(), mergeState);
            logger.debug("Checking value field [{}]: found={}", value.getField(), found);
            if (found == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasFieldInMerge(String fieldName, MergeState mergeState) {
        for (FieldInfo fi : mergeState.mergeFieldInfos) {
            if (fi.name.equals(fieldName) && fi.getDocValuesType() != DocValuesType.NONE) {
                return true;
            }
        }
        return false;
    }

    private static SortedDocValues getMergedSortedDocValues(String fieldName, MergeState mergeState) throws IOException {
        FieldInfo fieldInfo = null;
        for (FieldInfo fi : mergeState.mergeFieldInfos) {
            if (fi.name.equals(fieldName)) {
                fieldInfo = fi;
                break;
            }
        }

        if (fieldInfo == null) {
            return null;
        }

        DocValuesType dvType = fieldInfo.getDocValuesType();

        // Handle SORTED type directly
        if (dvType == DocValuesType.SORTED) {
            for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
                if (mergeState.docValuesProducers[i] != null) {
                    SortedDocValues sorted = mergeState.docValuesProducers[i].getSorted(fieldInfo);
                    if (sorted != null) {
                        return sorted;
                    }
                }
            }
        }

        // Handle SORTED_SET type - during merge, we need to handle multi-segment doc values
        // For simplicity, we'll skip SORTED_SET fields for now and only support SORTED
        // which is used by single-valued keyword fields
        if (dvType == DocValuesType.SORTED_SET) {
            // SORTED_SET requires special handling during merge
            // For now, we'll create a special wrapper that handles multi-segment access
            // This is a simplified implementation that may not work for all cases
            logger.debug("Field [{}] uses SORTED_SET doc values - creating multi-segment wrapper", fieldName);
            return createMergedSortedDocValues(fieldInfo, mergeState);
        }

        return null;
    }

    /**
     * Create merged SortedDocValues that properly handles multi-segment merging.
     * This reads values from all source segments and builds a unified view.
     */
    private static SortedDocValues createMergedSortedDocValues(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        // Collect all term values from all segments
        // Use a map keyed by segment index to avoid index mismatch issues
        java.util.TreeSet<BytesRef> allTerms = new java.util.TreeSet<>();
        java.util.Map<Integer, SortedSetDocValues> segmentDvMap = new java.util.HashMap<>();

        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            if (mergeState.docValuesProducers[i] != null) {
                SortedSetDocValues dv = mergeState.docValuesProducers[i].getSortedSet(fieldInfo);
                if (dv != null) {
                    segmentDvMap.put(i, dv);
                    // Collect all terms
                    for (int ord = 0; ord < dv.getValueCount(); ord++) {
                        allTerms.add(BytesRef.deepCopyOf(dv.lookupOrd(ord)));
                    }
                }
            }
        }

        if (segmentDvMap.isEmpty()) {
            return null;
        }

        // Build term -> ordinal mapping
        final java.util.List<BytesRef> termList = new java.util.ArrayList<>(allTerms);
        final java.util.Map<BytesRef, Integer> termToOrd = new java.util.HashMap<>();
        for (int i = 0; i < termList.size(); i++) {
            termToOrd.put(termList.get(i), i);
        }

        // Build document ordinal mapping
        int totalDocs = mergeState.segmentInfo.maxDoc();
        final int[] docOrdinals = new int[totalDocs];
        java.util.Arrays.fill(docOrdinals, -1);

        for (int segmentIdx = 0; segmentIdx < mergeState.docValuesProducers.length; segmentIdx++) {
            if (mergeState.docValuesProducers[segmentIdx] == null) {
                continue;
            }

            // Get doc values for this specific segment (using correct segment index)
            SortedSetDocValues segmentDv = segmentDvMap.get(segmentIdx);
            if (segmentDv == null) {
                continue;
            }

            // Get the doc map for this segment
            org.apache.lucene.index.MergeState.DocMap docMap = mergeState.docMaps[segmentIdx];
            int maxDocInSegment = mergeState.maxDocs[segmentIdx];

            for (int oldDoc = 0; oldDoc < maxDocInSegment; oldDoc++) {
                int newDoc = docMap.get(oldDoc);
                if (newDoc >= 0 && newDoc < totalDocs) {
                    if (segmentDv.advanceExact(oldDoc)) {
                        long ord = segmentDv.nextOrd();
                        if (ord >= 0) {
                            BytesRef term = segmentDv.lookupOrd(ord);
                            Integer newOrd = termToOrd.get(term);
                            if (newOrd != null) {
                                docOrdinals[newDoc] = newOrd;
                            }
                        }
                    }
                }
            }
        }

        // Create the wrapper
        return new SortedDocValues() {
            private int currentDoc = -1;

            @Override
            public int ordValue() {
                return docOrdinals[currentDoc];
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                return termList.get(ord);
            }

            @Override
            public int getValueCount() {
                return termList.size();
            }

            @Override
            public boolean advanceExact(int target) {
                currentDoc = target;
                return docOrdinals[target] >= 0;
            }

            @Override
            public int docID() {
                return currentDoc;
            }

            @Override
            public int nextDoc() {
                throw new UnsupportedOperationException("Use advanceExact instead");
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException("Use advanceExact instead");
            }

            @Override
            public long cost() {
                return totalDocs;
            }
        };
    }

    private static SortedNumericDocValues getMergedSortedNumericDocValues(String fieldName, MergeState mergeState) throws IOException {
        FieldInfo fieldInfo = null;
        for (FieldInfo fi : mergeState.mergeFieldInfos) {
            if (fi.name.equals(fieldName)) {
                fieldInfo = fi;
                break;
            }
        }

        if (fieldInfo == null) {
            return null;
        }

        // Handle both NUMERIC and SORTED_NUMERIC types
        if (fieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC
            || fieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
            return createMergedSortedNumericDocValues(fieldInfo, mergeState);
        }

        return null;
    }

    /**
     * Create merged SortedNumericDocValues that properly handles multi-segment merging.
     * This builds a complete mapping of merged doc IDs to their values.
     */
    private static SortedNumericDocValues createMergedSortedNumericDocValues(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        int totalDocs = mergeState.segmentInfo.maxDoc();

        // Build arrays to hold values for each doc in the merged segment
        final long[] docValues = new long[totalDocs];
        final boolean[] hasValue = new boolean[totalDocs];
        java.util.Arrays.fill(docValues, 0);
        java.util.Arrays.fill(hasValue, false);

        // Iterate through each source segment
        for (int segmentIdx = 0; segmentIdx < mergeState.docValuesProducers.length; segmentIdx++) {
            if (mergeState.docValuesProducers[segmentIdx] == null) {
                continue;
            }

            SortedNumericDocValues segmentDv = mergeState.docValuesProducers[segmentIdx].getSortedNumeric(fieldInfo);
            if (segmentDv == null) {
                continue;
            }

            // Get the doc map for this segment
            org.apache.lucene.index.MergeState.DocMap docMap = mergeState.docMaps[segmentIdx];
            int maxDocInSegment = mergeState.maxDocs[segmentIdx];

            // Iterate through all docs in this source segment
            for (int oldDoc = 0; oldDoc < maxDocInSegment; oldDoc++) {
                int newDoc = docMap.get(oldDoc);
                if (newDoc >= 0 && newDoc < totalDocs) {
                    if (segmentDv.advanceExact(oldDoc)) {
                        // Get the value for this document
                        if (segmentDv.docValueCount() > 0) {
                            docValues[newDoc] = segmentDv.nextValue();
                            hasValue[newDoc] = true;
                        }
                    }
                }
            }
        }

        // Create wrapper that provides access to the merged values
        return new SortedNumericDocValues() {
            private int currentDoc = -1;
            private int valueIndex = 0;

            @Override
            public long nextValue() {
                valueIndex++;
                return docValues[currentDoc];
            }

            @Override
            public int docValueCount() {
                return hasValue[currentDoc] ? 1 : 0;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                currentDoc = target;
                valueIndex = 0;
                return hasValue[target];
            }

            @Override
            public int docID() {
                return currentDoc;
            }

            @Override
            public int nextDoc() {
                throw new UnsupportedOperationException("Use advanceExact instead");
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException("Use advanceExact instead");
            }

            @Override
            public long cost() {
                return totalDocs;
            }
        };
    }
}
