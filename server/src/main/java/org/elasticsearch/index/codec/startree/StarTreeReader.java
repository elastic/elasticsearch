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
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.startree.StarTreeAggregationType;
import org.elasticsearch.index.mapper.startree.StarTreeGroupingField;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads star-tree data from segment files.
 */
public final class StarTreeReader implements Closeable {

    private final SegmentReadState state;
    private final String starTreeName;
    private final IndexInput valuesIn;
    private final IndexInput indexIn;

    // Parsed metadata
    private final List<GroupingFieldInfo> groupingFields;
    private final List<ValueInfo> values;
    private final int maxLeafDocs;
    private final int starNodeThreshold;
    private final int nodeCount;
    private final long rootNodeOffset;

    // Computed sizes
    private final int numAggregationTypes;

    // Caches for performance
    private static final int NODE_CACHE_SIZE = 1000;
    private final Map<Long, StarTreeNode> nodeCache;
    private final Map<String, Integer> groupingFieldIndexCache;
    private final Map<String, Integer> valueIndexCache;

    public StarTreeReader(SegmentReadState state, String starTreeName) throws IOException {
        this.state = state;
        this.starTreeName = starTreeName;

        boolean success = false;
        IndexInput valuesIn = null;
        IndexInput indexIn = null;

        try {
            String segmentName = state.segmentInfo.name;
            String segmentSuffix = starTreeName;

            // Open and read metadata file
            String metaName = segmentName + "_" + segmentSuffix + "." + StarTreeConstants.META_EXTENSION;
            ChecksumIndexInput checksumMetaIn = state.directory.openChecksumInput(metaName);
            CodecUtil.checkIndexHeader(
                checksumMetaIn,
                StarTreeConstants.META_CODEC,
                StarTreeConstants.VERSION_START,
                StarTreeConstants.VERSION_CURRENT,
                state.segmentInfo.getId(),
                segmentSuffix
            );

            // Parse grouping fields
            this.groupingFields = new ArrayList<>();
            int numGroupingFields = checksumMetaIn.readVInt();
            for (int i = 0; i < numGroupingFields; i++) {
                String field = checksumMetaIn.readString();
                StarTreeGroupingField.GroupingType type = StarTreeGroupingField.GroupingType.values()[checksumMetaIn.readByte()];
                long intervalMillis = checksumMetaIn.readLong();
                groupingFields.add(new GroupingFieldInfo(field, type, intervalMillis));
            }

            // Parse values
            this.values = new ArrayList<>();
            int numValues = checksumMetaIn.readVInt();
            for (int i = 0; i < numValues; i++) {
                String field = checksumMetaIn.readString();
                int numAggregations = checksumMetaIn.readVInt();
                EnumSet<StarTreeAggregationType> aggregations = EnumSet.noneOf(StarTreeAggregationType.class);
                for (int j = 0; j < numAggregations; j++) {
                    aggregations.add(StarTreeAggregationType.values()[checksumMetaIn.readByte()]);
                }
                values.add(new ValueInfo(field, aggregations));
            }
            this.numAggregationTypes = StarTreeAggregationType.values().length;

            this.maxLeafDocs = checksumMetaIn.readVInt();
            this.starNodeThreshold = checksumMetaIn.readVInt();
            this.nodeCount = checksumMetaIn.readVInt();

            CodecUtil.checkFooter(checksumMetaIn);
            checksumMetaIn.close();

            // Open values file
            String valuesName = segmentName + "_" + segmentSuffix + "." + StarTreeConstants.VALUES_EXTENSION;
            valuesIn = state.directory.openInput(valuesName, state.context);
            CodecUtil.checkIndexHeader(
                valuesIn,
                StarTreeConstants.VALUES_CODEC,
                StarTreeConstants.VERSION_START,
                StarTreeConstants.VERSION_CURRENT,
                state.segmentInfo.getId(),
                segmentSuffix
            );

            // Open index file and read root offset
            String indexName = segmentName + "_" + segmentSuffix + "." + StarTreeConstants.INDEX_EXTENSION;
            indexIn = state.directory.openInput(indexName, state.context);
            CodecUtil.checkIndexHeader(
                indexIn,
                StarTreeConstants.INDEX_CODEC,
                StarTreeConstants.VERSION_START,
                StarTreeConstants.VERSION_CURRENT,
                state.segmentInfo.getId(),
                segmentSuffix
            );

            // Root offset is the first entry in the index file (written after tree is built)
            this.rootNodeOffset = indexIn.readLong();

            this.valuesIn = valuesIn;
            this.indexIn = indexIn;

            // Initialize caches
            // LRU cache for nodes using access-order LinkedHashMap
            this.nodeCache = new LinkedHashMap<>(NODE_CACHE_SIZE, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Long, StarTreeNode> eldest) {
                    return size() > NODE_CACHE_SIZE;
                }
            };

            // Build field index caches for O(1) lookup
            this.groupingFieldIndexCache = new HashMap<>();
            for (int i = 0; i < groupingFields.size(); i++) {
                groupingFieldIndexCache.put(groupingFields.get(i).field(), i);
            }
            this.valueIndexCache = new HashMap<>();
            for (int i = 0; i < values.size(); i++) {
                valueIndexCache.put(values.get(i).field(), i);
            }

            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(valuesIn, indexIn);
            }
        }
    }

    /**
     * Get the list of grouping fields.
     */
    public List<GroupingFieldInfo> getGroupingFields() {
        return groupingFields;
    }

    /**
     * Get the list of value fields.
     */
    public List<ValueInfo> getValues() {
        return values;
    }

    /**
     * Get the grouping field index for a field name.
     * Uses O(1) HashMap lookup instead of O(n) linear search.
     */
    public int getGroupingFieldIndex(String fieldName) {
        Integer index = groupingFieldIndexCache.get(fieldName);
        return index != null ? index : -1;
    }

    /**
     * Get the value field index for a field name.
     * Uses O(1) HashMap lookup instead of O(n) linear search.
     */
    public int getValueIndex(String fieldName) {
        Integer index = valueIndexCache.get(fieldName);
        return index != null ? index : -1;
    }

    /**
     * Check if a field is a grouping field in this star-tree.
     */
    public boolean isGroupingField(String fieldName) {
        return getGroupingFieldIndex(fieldName) >= 0;
    }

    /**
     * Check if a field is a value field in this star-tree.
     */
    public boolean isValue(String fieldName) {
        return getValueIndex(fieldName) >= 0;
    }

    /**
     * Read a node at the given offset.
     * Uses an LRU cache to avoid repeated disk reads for frequently accessed nodes.
     */
    public StarTreeNode readNode(long offset) throws IOException {
        // Check cache first
        StarTreeNode cachedNode = nodeCache.get(offset);
        if (cachedNode != null) {
            return cachedNode;
        }

        // Read from disk
        IndexInput in = valuesIn.clone();
        in.seek(offset);

        // Read node header
        long groupingFieldOrdinal = in.readLong();
        byte nodeType = in.readByte();
        boolean isStarNode = nodeType == StarTreeConstants.NODE_TYPE_STAR;
        boolean isLeaf = nodeType == StarTreeConstants.NODE_TYPE_LEAF;
        int depth = in.readVInt();
        long docCount = in.readVLong();
        int numChildren = in.readVInt();

        // Read aggregated values
        double[][] aggregatedValues = new double[values.size()][numAggregationTypes];
        for (int i = 0; i < values.size(); i++) {
            for (int j = 0; j < numAggregationTypes; j++) {
                aggregatedValues[i][j] = Double.longBitsToDouble(in.readLong());
            }
        }

        // Read children offsets
        long[] childrenOffsets = new long[numChildren];
        for (int i = 0; i < numChildren; i++) {
            childrenOffsets[i] = in.readLong();
        }

        StarTreeNode node = new StarTreeNode(
            groupingFieldOrdinal,
            depth,
            isStarNode,
            isLeaf,
            offset,
            numChildren,
            childrenOffsets,
            aggregatedValues,
            docCount
        );

        // Add to cache
        nodeCache.put(offset, node);
        return node;
    }

    /**
     * Create a traverser for querying the star-tree.
     */
    public StarTreeTraverser createTraverser() throws IOException {
        return new StarTreeTraverser(this);
    }

    /**
     * Get the total number of nodes in the tree.
     */
    public int getNodeCount() {
        return nodeCount;
    }

    /**
     * Get the root node offset.
     */
    public long getRootNodeOffset() {
        return rootNodeOffset;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(valuesIn, indexIn);
    }

    /**
     * Information about a grouping field.
     */
    public record GroupingFieldInfo(String field, StarTreeGroupingField.GroupingType type, long intervalMillis) {}

    /**
     * Information about a value field.
     */
    public record ValueInfo(String field, Set<StarTreeAggregationType> aggregations) {}

    /**
     * Traverser for navigating the star-tree during queries.
     */
    public static final class StarTreeTraverser {
        private final StarTreeReader reader;
        private StarTreeNode currentNode;

        StarTreeTraverser(StarTreeReader reader) throws IOException {
            this.reader = reader;
            // Start at root
            long rootOffset = reader.getRootNodeOffset();
            if (rootOffset >= 0) {
                this.currentNode = reader.readNode(rootOffset);
            }
        }

        /**
         * Get the current node.
         */
        public StarTreeNode current() {
            return currentNode;
        }

        /**
         * Check if the traverser has a valid current node.
         */
        public boolean hasNode() {
            return currentNode != null;
        }

        /**
         * Move to the child node matching the given grouping field ordinal.
         * Returns true if a matching child was found.
         */
        public boolean moveToChild(long groupingFieldOrdinal) throws IOException {
            if (currentNode == null || currentNode.isLeaf()) {
                return false;
            }

            long[] childOffsets = currentNode.getChildrenOffsets();
            for (long childOffset : childOffsets) {
                StarTreeNode child = reader.readNode(childOffset);
                if (child.getGroupingFieldOrdinal() == groupingFieldOrdinal) {
                    currentNode = child;
                    return true;
                }
            }

            return false;
        }

        /**
         * Move to the star node child if it exists.
         * Returns true if a star node was found.
         */
        public boolean moveToStarChild() throws IOException {
            return moveToChild(StarTreeConstants.STAR_NODE_ORDINAL);
        }

        /**
         * Get all children of the current node.
         */
        public List<StarTreeNode> getChildren() throws IOException {
            if (currentNode == null || currentNode.isLeaf()) {
                return List.of();
            }

            List<StarTreeNode> children = new ArrayList<>();
            long[] childOffsets = currentNode.getChildrenOffsets();
            for (long childOffset : childOffsets) {
                children.add(reader.readNode(childOffset));
            }
            return children;
        }

        /**
         * Reset to root node.
         */
        public void reset() throws IOException {
            long rootOffset = reader.getRootNodeOffset();
            if (rootOffset >= 0) {
                currentNode = reader.readNode(rootOffset);
            } else {
                currentNode = null;
            }
        }

        /**
         * Get aggregated value for a value field at the current node.
         */
        public double getAggregatedValue(String valueField, StarTreeAggregationType type) {
            if (currentNode == null) {
                throw new IllegalStateException("No current node");
            }
            int valueIndex = reader.getValueIndex(valueField);
            if (valueIndex < 0) {
                throw new IllegalArgumentException("Unknown value field: " + valueField);
            }
            return currentNode.getAggregatedValue(valueIndex, type);
        }

        /**
         * Get document count at the current node.
         */
        public long getDocCount() {
            if (currentNode == null) {
                throw new IllegalStateException("No current node");
            }
            return currentNode.getDocCount();
        }
    }
}
