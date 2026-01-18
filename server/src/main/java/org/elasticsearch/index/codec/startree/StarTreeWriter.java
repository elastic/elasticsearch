/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.startree;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.startree.StarTreeAggregationType;
import org.elasticsearch.index.mapper.startree.StarTreeConfig;
import org.elasticsearch.index.mapper.startree.StarTreeGroupingField;
import org.elasticsearch.index.mapper.startree.StarTreeValue;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Writes star-tree data to segment files.
 * <p>
 * The star-tree is built during segment merge by:
 * <ol>
 *     <li>Extracting dimension ordinals and metric values from doc values</li>
 *     <li>Sorting documents by dimension values</li>
 *     <li>Recursively building the tree with star nodes for high-cardinality dimensions</li>
 *     <li>Pre-aggregating metrics at each node</li>
 * </ol>
 */
public final class StarTreeWriter implements Closeable {

    private final SegmentWriteState state;
    private final StarTreeConfig.StarTreeFieldConfig config;
    private final java.util.function.Function<String, Boolean> isDoubleFieldLookup;
    private final IndexOutput metaOut;
    private final IndexOutput valuesOut;
    private final IndexOutput dimsOut;
    private final IndexOutput indexOut;

    private int nodeCount = 0;
    private long rootNodeOffset = -1;

    private static final org.elasticsearch.logging.Logger logger =
        org.elasticsearch.logging.LogManager.getLogger(StarTreeWriter.class);

    /**
     * Creates a StarTreeWriter with default field type handling (all fields treated as long).
     * Use this constructor when field type information is not available.
     */
    public StarTreeWriter(SegmentWriteState state, StarTreeConfig.StarTreeFieldConfig config) throws IOException {
        this(state, config, fieldName -> false);  // Default to treating all fields as long
    }

    /**
     * Creates a StarTreeWriter with explicit field type lookup.
     *
     * @param state the segment write state
     * @param config the star-tree field configuration
     * @param isDoubleFieldLookup function to check if a field is a double/float type (returns true for double, false for long)
     */
    public StarTreeWriter(
        SegmentWriteState state,
        StarTreeConfig.StarTreeFieldConfig config,
        java.util.function.Function<String, Boolean> isDoubleFieldLookup
    ) throws IOException {
        this.state = state;
        this.config = config;
        this.isDoubleFieldLookup = isDoubleFieldLookup;

        boolean success = false;
        IndexOutput metaOut = null;
        IndexOutput valuesOut = null;
        IndexOutput dimsOut = null;
        IndexOutput indexOut = null;

        try {
            String segmentName = state.segmentInfo.name;
            String segmentSuffix = config.getName();
            String metaFileName = segmentName + "_" + segmentSuffix + "." + StarTreeConstants.META_EXTENSION;

            logger.info("StarTreeWriter: creating files for segment [{}], starTree=[{}]", segmentName, segmentSuffix);
            logger.info("StarTreeWriter: directory={}, metaFileName=[{}]", state.directory, metaFileName);

            metaOut = state.directory.createOutput(
                metaFileName,
                state.context
            );
            CodecUtil.writeIndexHeader(
                metaOut,
                StarTreeConstants.META_CODEC,
                StarTreeConstants.VERSION_CURRENT,
                state.segmentInfo.getId(),
                segmentSuffix
            );

            valuesOut = state.directory.createOutput(
                segmentName + "_" + segmentSuffix + "." + StarTreeConstants.VALUES_EXTENSION,
                state.context
            );
            CodecUtil.writeIndexHeader(
                valuesOut,
                StarTreeConstants.VALUES_CODEC,
                StarTreeConstants.VERSION_CURRENT,
                state.segmentInfo.getId(),
                segmentSuffix
            );

            dimsOut = state.directory.createOutput(
                segmentName + "_" + segmentSuffix + "." + StarTreeConstants.DIMS_EXTENSION,
                state.context
            );
            CodecUtil.writeIndexHeader(
                dimsOut,
                StarTreeConstants.DIMS_CODEC,
                StarTreeConstants.VERSION_CURRENT,
                state.segmentInfo.getId(),
                segmentSuffix
            );

            indexOut = state.directory.createOutput(
                segmentName + "_" + segmentSuffix + "." + StarTreeConstants.INDEX_EXTENSION,
                state.context
            );
            CodecUtil.writeIndexHeader(
                indexOut,
                StarTreeConstants.INDEX_CODEC,
                StarTreeConstants.VERSION_CURRENT,
                state.segmentInfo.getId(),
                segmentSuffix
            );

            this.metaOut = metaOut;
            this.valuesOut = valuesOut;
            this.dimsOut = dimsOut;
            this.indexOut = indexOut;
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(metaOut, valuesOut, dimsOut, indexOut);
            }
        }
    }

    /**
     * Build and write the star-tree.
     *
     * @param sortedGroupingFields map of sorted grouping field name to doc values (keyword fields)
     * @param numericGroupingFields map of numeric grouping field name to doc values (date/numeric fields)
     * @param valueDocValues map of value field name to doc values
     */
    public void build(
        Map<String, SortedDocValues> sortedGroupingFields,
        Map<String, SortedNumericDocValues> numericGroupingFields,
        Map<String, SortedNumericDocValues> valueDocValues
    ) throws IOException {
        List<StarTreeGroupingField> groupingFields = config.getGroupingFields();
        List<StarTreeValue> values = config.getValues();

        // Write metadata header
        writeMetaHeader(groupingFields, values);

        // Extract and sort document records
        List<DocumentRecord> records = extractDocumentRecords(
            sortedGroupingFields,
            numericGroupingFields,
            valueDocValues,
            groupingFields,
            values
        );

        // Build and write tree
        if (records.isEmpty() == false) {
            TreeNode root = buildTree(records, 0, groupingFields, values);
            rootNodeOffset = writeTreeRecursive(root, values.size());
        }

        // Write root offset to index file
        indexOut.writeLong(rootNodeOffset);

        // Write metadata footer
        writeMetaFooter();
    }

    private void writeMetaHeader(List<StarTreeGroupingField> groupingFields, List<StarTreeValue> values) throws IOException {
        // Write grouping fields
        metaOut.writeVInt(groupingFields.size());
        for (StarTreeGroupingField gf : groupingFields) {
            metaOut.writeString(gf.getField());
            metaOut.writeByte((byte) gf.getType().ordinal());
            metaOut.writeLong(gf.getIntervalMillis());
        }

        // Write values
        metaOut.writeVInt(values.size());
        for (StarTreeValue value : values) {
            metaOut.writeString(value.getField());
            Set<StarTreeAggregationType> aggregations = value.getAggregations();
            metaOut.writeVInt(aggregations.size());
            for (StarTreeAggregationType type : aggregations) {
                metaOut.writeByte((byte) type.ordinal());
            }
        }

        // Write config
        metaOut.writeVInt(config.getMaxLeafDocs());
        metaOut.writeVInt(config.getStarNodeThreshold());
    }

    private void writeMetaFooter() throws IOException {
        metaOut.writeVInt(nodeCount);
    }

    /**
     * Extract document records from doc values.
     */
    private List<DocumentRecord> extractDocumentRecords(
        Map<String, SortedDocValues> sortedGroupingFields,
        Map<String, SortedNumericDocValues> numericGroupingFields,
        Map<String, SortedNumericDocValues> valueDocValues,
        List<StarTreeGroupingField> groupingFields,
        List<StarTreeValue> values
    ) throws IOException {
        int maxDoc = state.segmentInfo.maxDoc();
        List<DocumentRecord> records = new ArrayList<>(maxDoc);

        for (int doc = 0; doc < maxDoc; doc++) {
            // Extract grouping field ordinals/values
            long[] gfOrdinals = new long[groupingFields.size()];

            for (int i = 0; i < groupingFields.size(); i++) {
                StarTreeGroupingField gf = groupingFields.get(i);
                String fieldName = gf.getField();

                // Try sorted doc values first (keyword fields)
                SortedDocValues sortedDv = sortedGroupingFields.get(fieldName);
                if (sortedDv != null && sortedDv.advanceExact(doc)) {
                    gfOrdinals[i] = sortedDv.ordValue();
                    continue;
                }

                // Try sorted numeric doc values (date/numeric fields)
                SortedNumericDocValues numericDv = numericGroupingFields.get(fieldName);
                if (numericDv != null && numericDv.advanceExact(doc)) {
                    long value = numericDv.nextValue();
                    // Apply bucketing for date histogram fields
                    if (gf.getIntervalMillis() > 0) {
                        value = (value / gf.getIntervalMillis()) * gf.getIntervalMillis();
                    }
                    gfOrdinals[i] = value;
                    continue;
                }

                // Missing grouping field value - use special ordinal for NULL
                gfOrdinals[i] = StarTreeConstants.NULL_DIMENSION_ORDINAL;
            }

            // Extract value field values
            double[][] valueVals = new double[values.size()][StarTreeAggregationType.values().length];
            for (int i = 0; i < values.size(); i++) {
                StarTreeValue value = values.get(i);
                SortedNumericDocValues dv = valueDocValues.get(value.getField());

                // Initialize with defaults
                valueVals[i][StarTreeAggregationType.MIN.ordinal()] = Double.POSITIVE_INFINITY;
                valueVals[i][StarTreeAggregationType.MAX.ordinal()] = Double.NEGATIVE_INFINITY;
                valueVals[i][StarTreeAggregationType.SUM.ordinal()] = 0.0;
                valueVals[i][StarTreeAggregationType.COUNT.ordinal()] = 0.0;

                if (dv != null && dv.advanceExact(doc)) {
                    // Get the numeric value - for long/int fields, doc values store the raw long value
                    // For double/float fields, doc values store the sortable long representation
                    long rawValue = dv.nextValue();
                    // Use field type information to properly decode the value
                    boolean isDoubleField = isDoubleFieldLookup.apply(value.getField());
                    double numericValue = isDoubleField
                        ? NumericUtils.sortableLongToDouble(rawValue)
                        : (double) rawValue;

                    for (StarTreeAggregationType type : value.getAggregations()) {
                        switch (type) {
                            case SUM -> valueVals[i][type.ordinal()] = numericValue;
                            case COUNT -> valueVals[i][type.ordinal()] = 1;
                            case MIN -> valueVals[i][type.ordinal()] = numericValue;
                            case MAX -> valueVals[i][type.ordinal()] = numericValue;
                        }
                    }
                }
            }

            records.add(new DocumentRecord(gfOrdinals, valueVals));
        }

        // Sort by grouping field ordinals (lexicographic order)
        records.sort((a, b) -> {
            for (int i = 0; i < a.groupingFieldOrdinals.length; i++) {
                int cmp = Long.compare(a.groupingFieldOrdinals[i], b.groupingFieldOrdinals[i]);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        });

        return records;
    }

    /**
     * Recursively build the star-tree in memory.
     */
    private TreeNode buildTree(
        List<DocumentRecord> records,
        int depth,
        List<StarTreeGroupingField> groupingFields,
        List<StarTreeValue> values
    ) {
        // Base case: create leaf node
        if (depth >= groupingFields.size() || records.size() <= config.getMaxLeafDocs()) {
            return createLeafNode(records, depth, values);
        }

        // Group records by current grouping field value
        Map<Long, List<DocumentRecord>> groups = new TreeMap<>();
        for (DocumentRecord record : records) {
            long ordinal = record.groupingFieldOrdinals[depth];
            groups.computeIfAbsent(ordinal, k -> new ArrayList<>()).add(record);
        }

        List<TreeNode> children = new ArrayList<>();

        // Create star node if cardinality exceeds threshold
        if (groups.size() > config.getStarNodeThreshold()) {
            // Star node aggregates ALL records at this level
            TreeNode starChild = buildTree(records, depth + 1, groupingFields, values);
            starChild.groupingFieldOrdinal = StarTreeConstants.STAR_NODE_ORDINAL;
            starChild.isStarNode = true;
            children.add(starChild);
        }

        // Create child node for each grouping field value
        for (Map.Entry<Long, List<DocumentRecord>> entry : groups.entrySet()) {
            TreeNode child = buildTree(entry.getValue(), depth + 1, groupingFields, values);
            child.groupingFieldOrdinal = entry.getKey();
            children.add(child);
        }

        // Create internal node
        TreeNode node = new TreeNode();
        node.groupingFieldOrdinal = depth > 0 ? records.get(0).groupingFieldOrdinals[depth - 1] : 0;
        node.depth = depth;
        node.isStarNode = false;
        node.isLeaf = false;
        node.children = children;
        node.aggregatedValues = aggregateValues(records, values);
        node.docCount = records.size();

        return node;
    }

    private TreeNode createLeafNode(List<DocumentRecord> records, int depth, List<StarTreeValue> values) {
        TreeNode node = new TreeNode();
        node.groupingFieldOrdinal = records.isEmpty() ? StarTreeConstants.STAR_NODE_ORDINAL
            : records.get(0).groupingFieldOrdinals[Math.min(depth, records.get(0).groupingFieldOrdinals.length - 1)];
        node.depth = depth;
        node.isStarNode = false;
        node.isLeaf = true;
        node.children = List.of();
        node.aggregatedValues = aggregateValues(records, values);
        node.docCount = records.size();
        return node;
    }

    private double[][] aggregateValues(List<DocumentRecord> records, List<StarTreeValue> values) {
        double[][] result = new double[values.size()][StarTreeAggregationType.values().length];

        // Initialize MIN to max and MAX to min
        for (int i = 0; i < values.size(); i++) {
            result[i][StarTreeAggregationType.MIN.ordinal()] = Double.POSITIVE_INFINITY;
            result[i][StarTreeAggregationType.MAX.ordinal()] = Double.NEGATIVE_INFINITY;
            result[i][StarTreeAggregationType.SUM.ordinal()] = 0.0;
            result[i][StarTreeAggregationType.COUNT.ordinal()] = 0.0;
        }

        for (DocumentRecord record : records) {
            for (int i = 0; i < values.size(); i++) {
                for (StarTreeAggregationType type : values.get(i).getAggregations()) {
                    int typeOrd = type.ordinal();
                    switch (type) {
                        case SUM, COUNT -> result[i][typeOrd] += record.valueAggregations[i][typeOrd];
                        case MIN -> result[i][typeOrd] = Math.min(result[i][typeOrd], record.valueAggregations[i][typeOrd]);
                        case MAX -> result[i][typeOrd] = Math.max(result[i][typeOrd], record.valueAggregations[i][typeOrd]);
                    }
                }
            }
        }

        return result;
    }

    /**
     * Write tree nodes using post-order traversal (children first, then parent).
     * This way, when we write a parent, all children offsets are known.
     * Returns the offset where this node was written.
     */
    private long writeTreeRecursive(TreeNode node, int numValues) throws IOException {
        nodeCount++;

        // First, recursively write all children and collect their offsets
        long[] childOffsets = new long[node.children.size()];
        for (int i = 0; i < node.children.size(); i++) {
            childOffsets[i] = writeTreeRecursive(node.children.get(i), numValues);
        }

        // Now write this node (all children are already written)
        long nodeOffset = valuesOut.getFilePointer();

        // Write node header
        valuesOut.writeLong(node.groupingFieldOrdinal);
        byte nodeType = node.isStarNode ? StarTreeConstants.NODE_TYPE_STAR
            : node.isLeaf ? StarTreeConstants.NODE_TYPE_LEAF
            : StarTreeConstants.NODE_TYPE_INTERNAL;
        valuesOut.writeByte(nodeType);
        valuesOut.writeVInt(node.depth);
        valuesOut.writeVLong(node.docCount);
        valuesOut.writeVInt(node.children.size());

        // Write aggregated values
        if (node.aggregatedValues != null) {
            for (double[] valueAggregations : node.aggregatedValues) {
                for (double value : valueAggregations) {
                    valuesOut.writeLong(Double.doubleToLongBits(value));
                }
            }
        }

        // Write children offsets (now known since children written first)
        for (long childOffset : childOffsets) {
            valuesOut.writeLong(childOffset);
        }

        return nodeOffset;
    }

    @Override
    public void close() throws IOException {
        String segmentName = state.segmentInfo.name;
        String segmentSuffix = config.getName();

        // Get file names before closing
        String metaFileName = segmentName + "_" + segmentSuffix + "." + StarTreeConstants.META_EXTENSION;
        String valuesFileName = segmentName + "_" + segmentSuffix + "." + StarTreeConstants.VALUES_EXTENSION;
        String dimsFileName = segmentName + "_" + segmentSuffix + "." + StarTreeConstants.DIMS_EXTENSION;
        String indexFileName = segmentName + "_" + segmentSuffix + "." + StarTreeConstants.INDEX_EXTENSION;

        try {
            if (metaOut != null) {
                CodecUtil.writeFooter(metaOut);
            }
            if (valuesOut != null) {
                CodecUtil.writeFooter(valuesOut);
            }
            if (dimsOut != null) {
                CodecUtil.writeFooter(dimsOut);
            }
            if (indexOut != null) {
                CodecUtil.writeFooter(indexOut);
            }
        } finally {
            IOUtils.close(metaOut, valuesOut, dimsOut, indexOut);
        }

        // Sync the star-tree files to disk
        state.directory.sync(java.util.Set.of(metaFileName, valuesFileName, dimsFileName, indexFileName));
        logger.info("StarTreeWriter: synced files [{}], [{}], [{}], [{}]",
            metaFileName, valuesFileName, dimsFileName, indexFileName);
    }

    /**
     * Internal record for document data during tree construction.
     */
    private static class DocumentRecord {
        final long[] groupingFieldOrdinals;
        final double[][] valueAggregations;

        DocumentRecord(long[] groupingFieldOrdinals, double[][] valueAggregations) {
            this.groupingFieldOrdinals = groupingFieldOrdinals;
            this.valueAggregations = valueAggregations;
        }
    }

    /**
     * Internal tree node for building the tree in memory before writing.
     */
    private static class TreeNode {
        long groupingFieldOrdinal;
        int depth;
        boolean isStarNode;
        boolean isLeaf;
        List<TreeNode> children;
        double[][] aggregatedValues;
        long docCount;
    }
}
