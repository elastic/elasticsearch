/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.startree;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.startree.StarTreeAggregationType;

import java.util.Arrays;

/**
 * Represents a node in the star-tree.
 * <p>
 * The star-tree is a hierarchical structure where:
 * <ul>
 *     <li>Each level corresponds to a grouping field</li>
 *     <li>Each node has children grouped by grouping field values</li>
 *     <li>Star nodes (*) aggregate across all values of their grouping field</li>
 *     <li>Leaf nodes contain pre-aggregated value fields</li>
 * </ul>
 */
public final class StarTreeNode {

    /** The grouping field ordinal for this node (-1 for star nodes) */
    private final long groupingFieldOrdinal;

    /** The depth/level of this node (0 = root, grouping field index = depth) */
    private final int depth;

    /** Whether this is a star node that aggregates across all grouping field values */
    private final boolean isStarNode;

    /** Whether this is a leaf node (no children) */
    private final boolean isLeaf;

    /** File offset where this node's data begins */
    private final long fileOffset;

    /** Number of child nodes */
    private final int numChildren;

    /** File offsets of child nodes */
    @Nullable
    private final long[] childrenOffsets;

    /** Pre-aggregated values for this node (indexed by value field then aggregation type) */
    @Nullable
    private final double[][] aggregatedValues;

    /** Number of documents aggregated at this node */
    private final long docCount;

    /**
     * Constructor for building a node during tree construction (in memory).
     */
    public StarTreeNode(
        long groupingFieldOrdinal,
        int depth,
        boolean isStarNode,
        boolean isLeaf,
        int numChildren,
        double[][] aggregatedValues,
        long docCount
    ) {
        this.groupingFieldOrdinal = groupingFieldOrdinal;
        this.depth = depth;
        this.isStarNode = isStarNode;
        this.isLeaf = isLeaf;
        this.fileOffset = -1;
        this.numChildren = numChildren;
        this.childrenOffsets = null;
        this.aggregatedValues = aggregatedValues;
        this.docCount = docCount;
    }

    /**
     * Constructor for reading a node from file.
     */
    public StarTreeNode(
        long groupingFieldOrdinal,
        int depth,
        boolean isStarNode,
        boolean isLeaf,
        long fileOffset,
        int numChildren,
        long[] childrenOffsets,
        double[][] aggregatedValues,
        long docCount
    ) {
        this.groupingFieldOrdinal = groupingFieldOrdinal;
        this.depth = depth;
        this.isStarNode = isStarNode;
        this.isLeaf = isLeaf;
        this.fileOffset = fileOffset;
        this.numChildren = numChildren;
        this.childrenOffsets = childrenOffsets;
        this.aggregatedValues = aggregatedValues;
        this.docCount = docCount;
    }

    public long getGroupingFieldOrdinal() {
        return groupingFieldOrdinal;
    }

    public int getDepth() {
        return depth;
    }

    public boolean isStarNode() {
        return isStarNode;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public int getNumChildren() {
        return numChildren;
    }

    /**
     * Get the file offsets of child nodes.
     * Returns null for in-memory nodes that haven't been written to file yet.
     */
    @Nullable
    public long[] getChildrenOffsets() {
        return childrenOffsets;
    }

    /**
     * Get the aggregated value for a specific value field and aggregation type.
     *
     * @param valueIndex the index of the value field
     * @param aggregationType the aggregation type
     * @return the pre-aggregated value
     */
    public double getAggregatedValue(int valueIndex, StarTreeAggregationType aggregationType) {
        if (aggregatedValues == null) {
            throw new IllegalStateException("Node does not have aggregated values");
        }
        return aggregatedValues[valueIndex][aggregationType.ordinal()];
    }

    /**
     * Get all aggregated values for a value field.
     *
     * @param valueIndex the index of the value field
     * @return array of values indexed by StarTreeAggregationType ordinal
     */
    public double[] getValueAggregations(int valueIndex) {
        if (aggregatedValues == null) {
            throw new IllegalStateException("Node does not have aggregated values");
        }
        return aggregatedValues[valueIndex];
    }

    /**
     * Get the total document count aggregated at this node.
     */
    public long getDocCount() {
        return docCount;
    }

    /**
     * Returns a copy of the aggregated values.
     */
    @Nullable
    public double[][] getAggregatedValues() {
        if (aggregatedValues == null) {
            return null;
        }
        double[][] copy = new double[aggregatedValues.length][];
        for (int i = 0; i < aggregatedValues.length; i++) {
            copy[i] = Arrays.copyOf(aggregatedValues[i], aggregatedValues[i].length);
        }
        return copy;
    }

    /**
     * Creates a new node with file position information.
     */
    public StarTreeNode withFilePosition(long fileOffset, long[] childrenOffsets) {
        return new StarTreeNode(
            groupingFieldOrdinal,
            depth,
            isStarNode,
            isLeaf,
            fileOffset,
            numChildren,
            childrenOffsets,
            aggregatedValues,
            docCount
        );
    }

    /**
     * Builder for constructing StarTreeNode instances during tree building.
     */
    public static class Builder {
        private long groupingFieldOrdinal = StarTreeConstants.STAR_NODE_ORDINAL;
        private int depth = 0;
        private boolean isStarNode = false;
        private boolean isLeaf = true;
        private int numChildren = 0;
        private double[][] aggregatedValues;
        private long docCount = 0;

        public Builder() {}

        public Builder groupingFieldOrdinal(long ordinal) {
            this.groupingFieldOrdinal = ordinal;
            return this;
        }

        public Builder depth(int depth) {
            this.depth = depth;
            return this;
        }

        public Builder isStarNode(boolean isStarNode) {
            this.isStarNode = isStarNode;
            return this;
        }

        public Builder isLeaf(boolean isLeaf) {
            this.isLeaf = isLeaf;
            return this;
        }

        public Builder numChildren(int numChildren) {
            this.numChildren = numChildren;
            return this;
        }

        public Builder aggregatedValues(double[][] values) {
            this.aggregatedValues = values;
            return this;
        }

        public Builder docCount(long docCount) {
            this.docCount = docCount;
            return this;
        }

        /**
         * Aggregate values from another node into this builder.
         */
        public Builder aggregateFrom(StarTreeNode other) {
            if (other.aggregatedValues != null) {
                if (this.aggregatedValues == null) {
                    this.aggregatedValues = other.getAggregatedValues();
                } else {
                    for (int i = 0; i < aggregatedValues.length; i++) {
                        for (int j = 0; j < aggregatedValues[i].length; j++) {
                            StarTreeAggregationType type = StarTreeAggregationType.values()[j];
                            switch (type) {
                                case SUM, COUNT -> aggregatedValues[i][j] += other.aggregatedValues[i][j];
                                case MIN -> aggregatedValues[i][j] = Math.min(aggregatedValues[i][j], other.aggregatedValues[i][j]);
                                case MAX -> aggregatedValues[i][j] = Math.max(aggregatedValues[i][j], other.aggregatedValues[i][j]);
                            }
                        }
                    }
                }
                this.docCount += other.docCount;
            }
            return this;
        }

        public StarTreeNode build() {
            return new StarTreeNode(groupingFieldOrdinal, depth, isStarNode, isLeaf, numChildren, aggregatedValues, docCount);
        }
    }

    @Override
    public String toString() {
        return "StarTreeNode{"
            + "ordinal="
            + (isStarNode ? "*" : groupingFieldOrdinal)
            + ", depth="
            + depth
            + ", leaf="
            + isLeaf
            + ", children="
            + numChildren
            + ", docs="
            + docCount
            + "}";
    }
}
