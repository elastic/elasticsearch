/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.codec.startree.StarTreeBuilder;
import org.elasticsearch.index.codec.startree.StarTreeNode;
import org.elasticsearch.index.codec.startree.StarTreeReader;
import org.elasticsearch.index.mapper.startree.StarTreeAggregationType;
import org.elasticsearch.index.mapper.startree.StarTreeGroupingField;
import org.elasticsearch.xpack.esql.plan.physical.EsStarTreeQueryExec.GroupingFieldFilter;
import org.elasticsearch.xpack.esql.plan.physical.EsStarTreeQueryExec.StarTreeAgg;
import org.elasticsearch.xpack.esql.plan.physical.EsStarTreeQueryExec.StarTreeAggType;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Factory for creating source operators that read from star-tree indices.
 * <p>
 * This factory coordinates reading from star-tree files across multiple shards
 * and produces pre-aggregated results.
 */
public class StarTreeSourceOperatorFactory implements SourceOperator.SourceOperatorFactory {

    private static final Logger logger = LogManager.getLogger(StarTreeSourceOperatorFactory.class);

    private final IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts;
    private final String starTreeName;
    private final List<String> groupByFields;
    private final List<StarTreeAgg> aggregations;
    private final List<GroupingFieldFilter> groupingFieldFilters;
    private final int pageSize;
    private final AggregatorMode mode;

    public StarTreeSourceOperatorFactory(
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        String starTreeName,
        List<String> groupByFields,
        List<StarTreeAgg> aggregations,
        List<GroupingFieldFilter> groupingFieldFilters,
        int pageSize,
        AggregatorMode mode
    ) {
        this.shardContexts = shardContexts;
        this.starTreeName = starTreeName;
        this.groupByFields = groupByFields;
        this.aggregations = aggregations;
        this.groupingFieldFilters = groupingFieldFilters;
        this.pageSize = pageSize > 0 ? pageSize : 1000;
        this.mode = mode;
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        return new StarTreeMultiShardSourceOperator(
            driverContext.blockFactory(),
            shardContexts,
            starTreeName,
            groupByFields,
            aggregations,
            groupingFieldFilters,
            pageSize,
            mode
        );
    }

    @Override
    public String describe() {
        return "StarTreeSource[tree="
            + starTreeName
            + ", groupBy="
            + groupByFields
            + ", aggs="
            + aggregations
            + ", filters="
            + groupingFieldFilters
            + "]";
    }

    /**
     * Source operator that reads pre-aggregated data from star-tree indices across multiple shards.
     */
    private static class StarTreeMultiShardSourceOperator extends SourceOperator {

        private final BlockFactory blockFactory;
        private final IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts;
        private final String starTreeName;
        private final List<String> groupByFields;
        private final List<StarTreeAgg> aggregations;
        private final List<GroupingFieldFilter> groupingFieldFilters;
        private final int pageSize;
        private final AggregatorMode mode;

        private int currentShardIndex = 0;
        private int currentLeafIndex = 0;
        private List<LeafReaderContext> currentLeaves;
        private LeafReader currentLeafReader;
        private StarTreeReader currentReader;
        private StarTreeReader.StarTreeTraverser currentTraverser;
        private boolean finished = false;

        // For GROUP BY traversal - stores nodes with their full ordinal path
        private List<NodeWithOrdinals> pendingNodesWithOrdinals;
        private int currentNodeIndex;
        private boolean segmentNodesCollected = false;

        // For ordinal to keyword value lookup (only for ORDINAL type fields)
        private SortedSetDocValues[] groupingFieldDocValues;
        // Track which grouping fields are DATE_HISTOGRAM vs ORDINAL
        private StarTreeGroupingField.GroupingType[] groupingFieldTypes;
        // For string value → ordinal lookup (used for string equality filters)
        // Maps field name → (string value → ordinal)
        private Map<String, Map<String, Long>> stringToOrdinalMaps;

        // Fallback processing for segments without star-tree
        private List<FallbackLeafContext> fallbackLeaves;
        private int currentFallbackLeafIndex;
        private boolean fallbackProcessingStarted;
        private List<NodeWithOrdinals> pendingFallbackNodes;

        // For aggregate-only queries (no GROUP BY): accumulate partial results from all segments
        // to combine them into a single page at the end
        private AggregateOnlyAccumulator aggregateOnlyAccumulator;

        // Tracking for status reporting
        private int starTreePagesEmitted = 0;
        private int fallbackPagesEmitted = 0;
        private int starTreeSegmentsProcessed = 0;
        private int fallbackSegmentsProcessed = 0;
        private long totalRowsEmitted = 0;

        /**
         * Context for a leaf that needs fallback (non-star-tree) processing.
         */
        private record FallbackLeafContext(LeafReaderContext leafContext, int shardIndex) {}

        /**
         * Helper class to track a node with the full path of ordinals for each grouping field.
         * This is needed because each star-tree node only stores its own dimension's ordinal,
         * not ordinals from parent dimensions.
         * For fallback processing, node is null and fallbackAgg holds the aggregated values.
         */
        private record NodeWithOrdinals(StarTreeNode node, long[] ordinals, @Nullable FallbackAggregation fallbackAgg) {}

        StarTreeMultiShardSourceOperator(
            BlockFactory blockFactory,
            IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
            String starTreeName,
            List<String> groupByFields,
            List<StarTreeAgg> aggregations,
            List<GroupingFieldFilter> groupingFieldFilters,
            int pageSize,
            AggregatorMode mode
        ) {
            this.blockFactory = blockFactory;
            this.shardContexts = shardContexts;
            this.starTreeName = starTreeName;
            this.groupByFields = groupByFields;
            this.aggregations = aggregations;
            this.groupingFieldFilters = groupingFieldFilters;
            this.pageSize = pageSize;
            this.mode = mode;
            this.pendingNodesWithOrdinals = new ArrayList<>();
            this.currentNodeIndex = 0;
            // Fallback processing initialization
            this.fallbackLeaves = new ArrayList<>();
            this.currentFallbackLeafIndex = 0;
            this.fallbackProcessingStarted = false;
            this.pendingFallbackNodes = new ArrayList<>();
            // For aggregate-only queries, initialize accumulator to combine results from all segments
            if (groupByFields.isEmpty()) {
                this.aggregateOnlyAccumulator = new AggregateOnlyAccumulator(aggregations.size());
            }
            // Increment reference count to keep shard contexts alive during operator execution
            shardContexts.iterable().forEach(RefCounted::mustIncRef);
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        private int getOutputCallCount = 0;

        @Override
        public Page getOutput() {
            getOutputCallCount++;
            if (finished) {
                logger.info("Star-tree: getOutput call #{} - already finished, mode={}", getOutputCallCount, mode);
                return null;
            }

            try {
                Page page = getOutputInternal();
                if (page != null) {
                    // Track statistics
                    totalRowsEmitted += page.getPositionCount();

                    StringBuilder blockInfo = new StringBuilder();
                    for (int i = 0; i < page.getBlockCount(); i++) {
                        if (i > 0) blockInfo.append(", ");
                        Block block = page.getBlock(i);
                        blockInfo.append(block.getClass().getSimpleName());
                    }
                    logger.info("Star-tree: getOutput call #{} - mode={}, returning page with {} positions, blocks=[{}]",
                        getOutputCallCount, mode, page.getPositionCount(), blockInfo);
                } else {
                    logger.info("Star-tree: getOutput call #{} - mode={}, returning null page", getOutputCallCount, mode);
                }
                return page;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read star-tree", e);
            }
        }

        @Override
        public Operator.Status status() {
            return new StarTreeSourceOperatorFactory.Status(this);
        }

        private Page getOutputInternal() throws IOException {
            // Process pending nodes from GROUP BY traversal (star-tree)
            if (pendingNodesWithOrdinals.isEmpty() == false && currentNodeIndex < pendingNodesWithOrdinals.size()) {
                return buildPageFromPendingNodes();
            }

            // Process pending nodes from fallback processing
            if (pendingFallbackNodes.isEmpty() == false && currentNodeIndex < pendingFallbackNodes.size()) {
                return buildPageFromFallbackNodes();
            }

            // Move through shards and their segments (star-tree)
            while (currentShardIndex < shardContexts.size()) {
                if (currentReader == null && currentLeaves == null) {
                    initializeCurrentShard();
                }

                if (currentReader != null || (currentLeaves != null && currentLeafIndex < currentLeaves.size())) {
                    Page page = processCurrentShard();
                    if (page != null) {
                        return page;
                    }
                }

                // Move to next shard
                currentLeaves = null;
                currentLeafIndex = 0;
                currentShardIndex++;
            }

            // Process fallback leaves (segments without star-tree)
            if (fallbackLeaves.isEmpty() == false && fallbackProcessingStarted == false) {
                fallbackProcessingStarted = true;
                currentFallbackLeafIndex = 0;
                logger.info("Star-tree: starting fallback processing for {} leaves", fallbackLeaves.size());
            } else if (fallbackLeaves.isEmpty() && fallbackProcessingStarted == false) {
                logger.info("Star-tree: no fallback leaves to process");
            }

            if (fallbackProcessingStarted && currentFallbackLeafIndex < fallbackLeaves.size()) {
                Page page = processFallbackLeaf();
                if (page != null) {
                    return page;
                }
            }

            // All shards and fallback leaves processed
            // For aggregate-only queries, return the combined page from the accumulator
            if (aggregateOnlyAccumulator != null && aggregateOnlyAccumulator.hasData) {
                logger.info("Star-tree: returning combined aggregate-only page from accumulator");
                Page page = buildCombinedAggregateOnlyPage();
                aggregateOnlyAccumulator.hasData = false;  // Mark as consumed
                finished = true;
                return page;
            }

            finished = true;
            return null;
        }

        private void initializeCurrentShard() throws IOException {
            if (currentShardIndex >= shardContexts.size()) {
                currentReader = null;
                return;
            }

            EsPhysicalOperationProviders.ShardContext shardContext = shardContexts.get(currentShardIndex);
            IndexReader indexReader = shardContext.searcher().getIndexReader();

            if (indexReader instanceof DirectoryReader directoryReader) {
                currentLeaves = directoryReader.leaves();
                currentLeafIndex = 0;
                initializeNextLeafWithStarTree();
            } else {
                currentReader = null;
            }
        }

        private void initializeNextLeafWithStarTree() throws IOException {
            currentReader = null;
            currentTraverser = null;
            currentLeafReader = null;
            groupingFieldDocValues = null;
            stringToOrdinalMaps = null;

            logger.debug(
                "Star-tree: initializeNextLeafWithStarTree, currentLeafIndex={}, leaves.size={}",
                currentLeafIndex,
                currentLeaves != null ? currentLeaves.size() : 0
            );

            while (currentLeafIndex < currentLeaves.size()) {
                LeafReaderContext leafContext = currentLeaves.get(currentLeafIndex);
                StarTreeReader reader = tryCreateStarTreeReader(leafContext);
                if (reader != null) {
                    currentReader = reader;
                    currentTraverser = reader.createTraverser();
                    starTreeSegmentsProcessed++;
                    currentLeafReader = leafContext.reader();
                    // Initialize doc values for grouping field ordinal-to-value lookup
                    initializeGroupingFieldDocValues();
                    logger.debug("Star-tree: initialized reader for leaf {}, traverser hasNode={}", currentLeafIndex, currentTraverser.hasNode());
                    return;
                }
                // No star-tree for this leaf - add to fallback list for later processing
                logger.info("Star-tree: no reader for leaf {}, adding to fallback list (total fallback: {})",
                    currentLeafIndex, fallbackLeaves.size() + 1);
                fallbackLeaves.add(new FallbackLeafContext(leafContext, currentShardIndex));
                fallbackSegmentsProcessed++;
                currentLeafIndex++;
            }
            logger.debug("Star-tree: no more leaves with star-tree, {} fallback leaves pending", fallbackLeaves.size());
        }

        private void initializeGroupingFieldDocValues() throws IOException {
            if (currentLeafReader == null) {
                groupingFieldDocValues = null;
                groupingFieldTypes = null;
                stringToOrdinalMaps = null;
                return;
            }

            // Initialize string→ordinal maps even when groupByFields is empty (for filters)
            stringToOrdinalMaps = new HashMap<>();

            // Get grouping field types from the star-tree reader
            List<StarTreeReader.GroupingFieldInfo> starTreeGroupingFields = currentReader != null
                ? currentReader.getGroupingFields()
                : List.of();

            // Build string→ordinal maps for string filter fields (even if not in GROUP BY)
            // This includes both exact string match filters AND IN clause filters
            for (GroupingFieldFilter filter : groupingFieldFilters) {
                if (filter.isStringMatch() || filter.isInClause()) {
                    String fieldName = filter.groupingField();
                    if (stringToOrdinalMaps.containsKey(fieldName) == false) {
                        SortedSetDocValues dv = currentLeafReader.getSortedSetDocValues(fieldName);
                        if (dv != null) {
                            Map<String, Long> valueToOrdinal = new HashMap<>();
                            long valueCount = dv.getValueCount();
                            for (long ord = 0; ord < valueCount; ord++) {
                                BytesRef value = dv.lookupOrd(ord);
                                valueToOrdinal.put(value.utf8ToString(), ord);
                            }
                            stringToOrdinalMaps.put(fieldName, valueToOrdinal);
                            logger.debug("Star-tree: built string→ordinal map for filter field [{}] with {} values", fieldName, valueCount);
                        }
                    }
                }
            }

            // Handle GROUP BY fields
            if (groupByFields.isEmpty()) {
                groupingFieldDocValues = null;
                groupingFieldTypes = null;
                return;
            }

            groupingFieldDocValues = new SortedSetDocValues[groupByFields.size()];
            groupingFieldTypes = new StarTreeGroupingField.GroupingType[groupByFields.size()];

            for (int i = 0; i < groupByFields.size(); i++) {
                String fieldName = groupByFields.get(i);

                // Find the grouping field type from star-tree metadata
                StarTreeGroupingField.GroupingType fieldType = StarTreeGroupingField.GroupingType.ORDINAL;
                for (StarTreeReader.GroupingFieldInfo gf : starTreeGroupingFields) {
                    if (gf.field().equals(fieldName)) {
                        fieldType = gf.type();
                        break;
                    }
                }
                groupingFieldTypes[i] = fieldType;

                // Only initialize doc values for ORDINAL (keyword) fields
                // DATE_HISTOGRAM fields don't need doc values - ordinal IS the bucketed timestamp
                if (fieldType == StarTreeGroupingField.GroupingType.ORDINAL) {
                    SortedSetDocValues dv = currentLeafReader.getSortedSetDocValues(fieldName);
                    groupingFieldDocValues[i] = dv;
                    logger.debug("Star-tree: initialized doc values for ORDINAL grouping field [{}], dv={}", fieldName, dv != null);

                    // Build string → ordinal lookup map for this field (for string equality filters)
                    if (dv != null && hasStringFilterForField(fieldName) && stringToOrdinalMaps.containsKey(fieldName) == false) {
                        Map<String, Long> valueToOrdinal = new HashMap<>();
                        long valueCount = dv.getValueCount();
                        for (long ord = 0; ord < valueCount; ord++) {
                            BytesRef value = dv.lookupOrd(ord);
                            valueToOrdinal.put(value.utf8ToString(), ord);
                        }
                        stringToOrdinalMaps.put(fieldName, valueToOrdinal);
                        logger.debug("Star-tree: built string→ordinal map for field [{}] with {} values", fieldName, valueCount);
                    }
                } else {
                    groupingFieldDocValues[i] = null;
                    logger.debug("Star-tree: DATE_HISTOGRAM grouping field [{}], no doc values needed", fieldName);
                }
            }
        }

        /**
         * Check if there's a string equality filter for the given field.
         */
        private boolean hasStringFilterForField(String fieldName) {
            for (GroupingFieldFilter filter : groupingFieldFilters) {
                if (filter.groupingField().equals(fieldName) && (filter.isStringMatch() || filter.isInClause())) {
                    return true;
                }
            }
            return false;
        }

        private StarTreeReader tryCreateStarTreeReader(LeafReaderContext leafContext) {
            try {
                logger.debug("Star-tree: tryCreateStarTreeReader, reader type={}", leafContext.reader().getClass().getName());
                // Unwrap filter readers to get to the underlying SegmentReader
                SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(leafContext.reader());
                if (segmentReader != null) {
                    Directory rawDirectory = segmentReader.directory();
                    var segmentInfo = segmentReader.getSegmentInfo();
                    String segmentName = segmentInfo.info.name;

                    logger.debug("Star-tree: checking for segment [{}], starTree=[{}]", segmentName, starTreeName);
                    logger.debug("Star-tree: directory={}, looking for file=[{}]", rawDirectory,
                        segmentName + "_" + starTreeName + ".star");

                    // List all files in directory for debugging
                    try {
                        String[] files = rawDirectory.listAll();
                        logger.debug("Star-tree: directory contains {} files", files.length);
                        for (String file : files) {
                            if (file.contains("star") || file.contains(segmentName)) {
                                logger.debug("Star-tree: found file [{}]", file);
                            }
                        }
                    } catch (Exception e) {
                        logger.debug("Star-tree: could not list directory: {}", e.getMessage());
                    }

                    // Get a directory that can read star-tree files (handles compound format)
                    Directory starTreeDirectory = StarTreeBuilder.getStarTreeDirectory(rawDirectory, segmentName, starTreeName);
                    if (starTreeDirectory == null) {
                        logger.debug("No star-tree files found for segment [{}], starTree=[{}]", segmentName, starTreeName);
                        return null;
                    }
                    logger.debug("Star-tree: using directory {} for reading", starTreeDirectory.getClass().getSimpleName());

                    // Create SegmentReadState for reading star-tree
                    SegmentReadState readState = new SegmentReadState(
                        starTreeDirectory,
                        segmentInfo.info,
                        segmentReader.getFieldInfos(),
                        IOContext.DEFAULT,
                        ""  // segment suffix
                    );

                    StarTreeReader reader = new StarTreeReader(readState, starTreeName);
                    logger.debug(
                        "Successfully opened star-tree reader for segment [{}], starTree=[{}], nodeCount=[{}]",
                        segmentName,
                        starTreeName,
                        reader.getNodeCount()
                    );
                    return reader;
                }
            } catch (IOException e) {
                logger.debug("Failed to create star-tree reader: {}", e.getMessage(), e);
            }
            return null;
        }

        private Page processCurrentShard() throws IOException {
            while (currentReader != null) {
                if (currentTraverser == null) {
                    currentTraverser = currentReader.createTraverser();
                }

                if (currentTraverser.hasNode() == false) {
                    // Move to next segment
                    closeCurrentReader();
                    currentLeafIndex++;
                    initializeNextLeafWithStarTree();
                    continue;
                }

                if (groupByFields.isEmpty() && groupingFieldFilters.isEmpty()) {
                    // No GROUP BY and no filters: accumulate root node aggregations for later
                    logger.debug("Star-tree: no GROUP BY, no filters, accumulating root aggregations from star-tree segment");
                    aggregateOnlyAccumulator.addFromStarTreeNode(currentTraverser.current(), aggregations, currentReader);
                    // Move to next segment - don't return page yet, wait until all segments processed
                    closeCurrentReader();
                    currentLeafIndex++;
                    initializeNextLeafWithStarTree();
                    continue;  // Continue to process more segments
                } else if (groupByFields.isEmpty() && groupingFieldFilters.isEmpty() == false) {
                    // No GROUP BY but has filters: traverse and accumulate filtered results
                    logger.info("Star-tree: no GROUP BY but has filters, traversing to find matching nodes");
                    initializeGroupingFieldDocValues();  // Need this for string→ordinal lookup
                    collectFilteredNodesForAggregateOnly();
                    // Move to next segment
                    closeCurrentReader();
                    currentLeafIndex++;
                    initializeNextLeafWithStarTree();
                    continue;
                } else {
                    // GROUP BY: traverse to appropriate nodes and collect
                    // Only collect once per segment
                    if (segmentNodesCollected == false) {
                        logger.debug(
                            "Star-tree: GROUP BY fields={}, root node={}",
                            groupByFields,
                            currentTraverser.current()
                        );
                        collectNodesForGroupBy();
                        segmentNodesCollected = true;
                        logger.debug("Star-tree: collected {} pending nodes for GROUP BY", pendingNodesWithOrdinals.size());
                    }
                    Page page = buildPageFromPendingNodes();
                    if (page != null) {
                        return page;
                    }
                    // All nodes from this segment processed, move to next
                    closeCurrentReader();
                    currentLeafIndex++;
                    segmentNodesCollected = false;
                    initializeNextLeafWithStarTree();
                }
            }
            return null;
        }

        private void collectNodesForGroupBy() throws IOException {
            pendingNodesWithOrdinals.clear();
            currentNodeIndex = 0;

            // Get star-tree dimension order from the reader
            List<StarTreeReader.GroupingFieldInfo> starTreeDimensions = currentReader.getGroupingFields();

            // Build a map of groupByField -> ordinalPath index
            Map<String, Integer> groupByFieldToOrdinalIndex = new HashMap<>();
            for (int i = 0; i < groupByFields.size(); i++) {
                groupByFieldToOrdinalIndex.put(groupByFields.get(i), i);
            }

            logger.debug(
                "Star-tree collectNodesForGroupBy: starTreeDimensions={}, groupByFields={}, groupByFieldToOrdinalIndex={}",
                starTreeDimensions.stream().map(StarTreeReader.GroupingFieldInfo::field).toList(),
                groupByFields,
                groupByFieldToOrdinalIndex
            );

            // Start traversal with empty ordinal path
            long[] ordinalPath = new long[groupByFields.size()];
            traverseAndCollect(currentTraverser.current(), 0, ordinalPath, starTreeDimensions, groupByFieldToOrdinalIndex);
        }

        /**
         * Traverse the star-tree with filters but no GROUP BY.
         * For filtered dimensions, find matching child nodes.
         * For non-filtered dimensions, use star nodes.
         * Accumulate results into aggregateOnlyAccumulator.
         */
        private void collectFilteredNodesForAggregateOnly() throws IOException {
            // Get star-tree dimension order from the reader
            List<StarTreeReader.GroupingFieldInfo> starTreeDimensions = currentReader.getGroupingFields();

            // Build a set of filter field names for quick lookup
            Set<String> filterFieldNames = new HashSet<>();
            for (GroupingFieldFilter filter : groupingFieldFilters) {
                filterFieldNames.add(filter.groupingField());
            }

            logger.debug(
                "Star-tree collectFilteredNodesForAggregateOnly: starTreeDimensions={}, filterFields={}",
                starTreeDimensions.stream().map(StarTreeReader.GroupingFieldInfo::field).toList(),
                filterFieldNames
            );

            // Start traversal from root
            traverseWithFiltersForAggregateOnly(
                currentTraverser.current(),
                0,
                starTreeDimensions,
                filterFieldNames
            );
        }

        /**
         * Recursive traversal for aggregate-only queries with filters.
         */
        private void traverseWithFiltersForAggregateOnly(
            StarTreeNode node,
            int starTreeDepth,
            List<StarTreeReader.GroupingFieldInfo> starTreeDimensions,
            Set<String> filterFieldNames
        ) throws IOException {
            if (node == null) {
                return;
            }

            logger.debug(
                "Star-tree traverseWithFiltersForAggregateOnly: depth={}, isLeaf={}, docCount={}",
                starTreeDepth,
                node.isLeaf(),
                node.getDocCount()
            );

            // Check if we've reached a leaf or traversed all dimensions
            if (node.isLeaf() || starTreeDepth >= starTreeDimensions.size()) {
                // Accumulate this node's aggregations
                logger.debug(
                    "Star-tree: accumulating filtered node, isLeaf={}, docCount={}",
                    node.isLeaf(),
                    node.getDocCount()
                );
                aggregateOnlyAccumulator.addFromStarTreeNode(node, aggregations, currentReader);
                return;
            }

            // Get the current star-tree dimension
            String currentDimension = starTreeDimensions.get(starTreeDepth).field();

            // Check if this dimension has a filter
            GroupingFieldFilter filter = findFilter(currentDimension);
            boolean hasFilter = filter != null && filter.isStar() == false;

            List<StarTreeNode> children = getChildren(node);

            logger.debug(
                "Star-tree: at depth={}, dimension={}, hasFilter={}, children={}",
                starTreeDepth,
                currentDimension,
                hasFilter,
                children.size()
            );

            if (hasFilter) {
                // This dimension has a filter - find matching child(ren)
                for (StarTreeNode child : children) {
                    if (child.isStarNode()) {
                        continue;  // Skip star nodes when filtering
                    }

                    boolean matches = matchesFilter(filter, child.getGroupingFieldOrdinal(), currentDimension);
                    if (matches) {
                        logger.debug(
                            "Star-tree: filter match on dimension {}, child ordinal={}",
                            currentDimension,
                            child.getGroupingFieldOrdinal()
                        );
                        traverseWithFiltersForAggregateOnly(child, starTreeDepth + 1, starTreeDimensions, filterFieldNames);
                    }
                }
            } else {
                // No filter on this dimension - use star node if available
                StarTreeNode starNode = null;
                for (StarTreeNode child : children) {
                    if (child.isStarNode()) {
                        starNode = child;
                        break;
                    }
                }

                if (starNode != null) {
                    logger.debug("Star-tree: no filter on dimension {}, using star node", currentDimension);
                    traverseWithFiltersForAggregateOnly(starNode, starTreeDepth + 1, starTreeDimensions, filterFieldNames);
                } else {
                    // No star node - aggregate all children
                    logger.debug(
                        "Star-tree: no filter and no star node on dimension {}, traversing {} children",
                        currentDimension,
                        children.size()
                    );
                    for (StarTreeNode child : children) {
                        if (child.isStarNode() == false) {
                            traverseWithFiltersForAggregateOnly(child, starTreeDepth + 1, starTreeDimensions, filterFieldNames);
                        }
                    }
                }
            }
        }

        private void traverseAndCollect(
            StarTreeNode node,
            int starTreeDepth,
            long[] ordinalPath,
            List<StarTreeReader.GroupingFieldInfo> starTreeDimensions,
            Map<String, Integer> groupByFieldToOrdinalIndex
        ) throws IOException {
            if (node == null) {
                return;
            }

            logger.debug(
                "Star-tree traverseAndCollect: node.depth={}, starTreeDepth={}, isLeaf={}, ordinalPath={}",
                node.getDepth(),
                starTreeDepth,
                node.isLeaf(),
                java.util.Arrays.toString(ordinalPath)
            );

            // Check if we're at a leaf node or beyond all star-tree dimensions
            if (node.isLeaf() || starTreeDepth >= starTreeDimensions.size()) {
                // Collect this node's aggregations
                logger.debug(
                    "Star-tree: adding terminal node to pending: isLeaf={}, docCount={}, ordinalPath={}",
                    node.isLeaf(),
                    node.getDocCount(),
                    java.util.Arrays.toString(ordinalPath)
                );
                pendingNodesWithOrdinals.add(new NodeWithOrdinals(node, ordinalPath.clone(), null));
                return;
            }

            // Get the current star-tree dimension name
            String currentStarTreeDimension = starTreeDimensions.get(starTreeDepth).field();

            // Check if this dimension is in our GROUP BY fields
            Integer ordinalIndex = groupByFieldToOrdinalIndex.get(currentStarTreeDimension);
            boolean isGroupByDimension = ordinalIndex != null;

            List<StarTreeNode> children = getChildren(node);

            logger.debug(
                "Star-tree: at starTreeDepth={}, dimension={}, isGroupByDimension={}, children={}",
                starTreeDepth,
                currentStarTreeDimension,
                isGroupByDimension,
                children.size()
            );

            // Check if there's a filter on this dimension
            GroupingFieldFilter filter = findFilter(currentStarTreeDimension);
            boolean hasFilter = filter != null && filter.isStar() == false;

            if (isGroupByDimension) {
                // This dimension is in GROUP BY - collect each child separately
                for (StarTreeNode child : children) {
                    if (child.isStarNode()) {
                        continue;  // Skip star nodes for GROUP BY dimensions
                    }

                    // Apply filter if present
                    if (hasFilter && matchesFilter(filter, child.getGroupingFieldOrdinal(), currentStarTreeDimension) == false) {
                        continue;  // Filter doesn't match
                    }

                    // Record ordinal for this GROUP BY dimension
                    ordinalPath[ordinalIndex] = child.getGroupingFieldOrdinal();
                    logger.debug(
                        "Star-tree: GROUP BY dimension {}, child ordinal={}, ordinalIndex={}",
                        currentStarTreeDimension,
                        child.getGroupingFieldOrdinal(),
                        ordinalIndex
                    );
                    traverseAndCollect(child, starTreeDepth + 1, ordinalPath, starTreeDimensions, groupByFieldToOrdinalIndex);
                }
            } else if (hasFilter) {
                // This dimension is NOT in GROUP BY but HAS a filter - find matching children
                logger.debug("Star-tree: non-GROUP BY dimension {} with filter, applying filter", currentStarTreeDimension);
                for (StarTreeNode child : children) {
                    if (child.isStarNode()) {
                        continue;  // Skip star nodes when filtering
                    }

                    if (matchesFilter(filter, child.getGroupingFieldOrdinal(), currentStarTreeDimension)) {
                        logger.debug(
                            "Star-tree: filter match on non-GROUP BY dimension {}, child ordinal={}",
                            currentStarTreeDimension,
                            child.getGroupingFieldOrdinal()
                        );
                        traverseAndCollect(child, starTreeDepth + 1, ordinalPath, starTreeDimensions, groupByFieldToOrdinalIndex);
                    }
                }
            } else {
                // This dimension is NOT in GROUP BY and has NO filter - use star node or aggregate all
                StarTreeNode starNode = null;
                for (StarTreeNode child : children) {
                    if (child.isStarNode()) {
                        starNode = child;
                        break;
                    }
                }

                if (starNode != null) {
                    // Use star node for this dimension (aggregates all values)
                    logger.debug("Star-tree: non-GROUP BY dimension {}, using star node", currentStarTreeDimension);
                    traverseAndCollect(starNode, starTreeDepth + 1, ordinalPath, starTreeDimensions, groupByFieldToOrdinalIndex);
                } else {
                    // No star node - need to traverse all children
                    // Note: If there's no star node, we'll get duplicate results for each child
                    // This can happen if the star node threshold wasn't met during building
                    logger.debug(
                        "Star-tree: non-GROUP BY dimension {}, no star node, traversing {} children",
                        currentStarTreeDimension,
                        children.size()
                    );
                    for (StarTreeNode child : children) {
                        if (child.isStarNode() == false) {
                            traverseAndCollect(child, starTreeDepth + 1, ordinalPath, starTreeDimensions, groupByFieldToOrdinalIndex);
                        }
                    }
                }
            }
        }

        private List<StarTreeNode> getChildren(StarTreeNode node) throws IOException {
            if (node.isLeaf() || currentReader == null) {
                return List.of();
            }

            long[] childOffsets = node.getChildrenOffsets();
            if (childOffsets == null) {
                return List.of();
            }

            List<StarTreeNode> children = new ArrayList<>(childOffsets.length);
            for (long offset : childOffsets) {
                children.add(currentReader.readNode(offset));
            }
            return children;
        }

        private GroupingFieldFilter findFilter(String groupingField) {
            for (GroupingFieldFilter filter : groupingFieldFilters) {
                if (filter.groupingField().equals(groupingField)) {
                    return filter;
                }
            }
            return null;
        }

        /**
         * Check if a node's ordinal matches a filter.
         * Handles exact match (ordinal or string), IN clause (multiple strings), and range filters.
         */
        private boolean matchesFilter(GroupingFieldFilter filter, long nodeOrdinal, String dimensionName) {
            if (filter.isExactMatch()) {
                long expectedOrdinal;
                if (filter.isStringMatch()) {
                    // String filter - look up ordinal
                    Map<String, Long> valueToOrdinal = stringToOrdinalMaps != null
                        ? stringToOrdinalMaps.get(dimensionName)
                        : null;
                    if (valueToOrdinal == null) {
                        return false;  // No lookup map
                    }
                    Long ordLookup = valueToOrdinal.get(filter.stringValue());
                    if (ordLookup == null) {
                        return false;  // String value not found
                    }
                    expectedOrdinal = ordLookup;
                } else {
                    expectedOrdinal = filter.ordinal();
                }
                return nodeOrdinal == expectedOrdinal;
            } else if (filter.isInClause()) {
                // IN clause - check if ordinal matches any of the values
                Map<String, Long> valueToOrdinal = stringToOrdinalMaps != null
                    ? stringToOrdinalMaps.get(dimensionName)
                    : null;
                if (valueToOrdinal == null) {
                    return false;  // No lookup map
                }
                for (String stringValue : filter.stringValues()) {
                    Long ordLookup = valueToOrdinal.get(stringValue);
                    if (ordLookup != null && ordLookup == nodeOrdinal) {
                        return true;  // Match found
                    }
                }
                return false;  // No match in IN list
            } else if (filter.isRange()) {
                return (filter.rangeMin() == null || nodeOrdinal >= filter.rangeMin())
                    && (filter.rangeMax() == null || nodeOrdinal <= filter.rangeMax());
            }
            return false;
        }

        // ========== Fallback Processing for Segments Without Star-Tree ==========

        /**
         * Process a fallback leaf (segment without star-tree) by scanning raw documents.
         */
        private Page processFallbackLeaf() throws IOException {
            while (currentFallbackLeafIndex < fallbackLeaves.size()) {
                FallbackLeafContext ctx = fallbackLeaves.get(currentFallbackLeafIndex);
                LeafReader leafReader = ctx.leafContext().reader();

                logger.debug("Star-tree: processing fallback leaf {}, maxDoc={}", currentFallbackLeafIndex, leafReader.maxDoc());

                // Aggregate documents in this segment
                Map<String, FallbackAggregation> aggregationsByGroup = aggregateFallbackLeaf(leafReader);

                if (aggregationsByGroup.isEmpty()) {
                    logger.debug("Star-tree: fallback leaf {} produced no results", currentFallbackLeafIndex);
                    currentFallbackLeafIndex++;
                    continue;
                }

                // For aggregate-only queries, accumulate into the combined accumulator
                if (groupByFields.isEmpty() && aggregateOnlyAccumulator != null) {
                    for (Map.Entry<String, FallbackAggregation> entry : aggregationsByGroup.entrySet()) {
                        FallbackAggregation agg = entry.getValue();
                        logger.info("Star-tree: accumulating fallback group [{}]: docCount={}, sums={}, counts={}",
                            entry.getKey(), agg.docCount, java.util.Arrays.toString(agg.sums), java.util.Arrays.toString(agg.counts));
                        aggregateOnlyAccumulator.addFromFallbackAggregation(agg, aggregations);
                    }
                    logger.info("Star-tree: fallback leaf {} accumulated into combined result", currentFallbackLeafIndex);
                    currentFallbackLeafIndex++;
                    continue;  // Continue to process more fallback leaves
                }

                // Convert aggregations to pending nodes (for GROUP BY queries)
                pendingFallbackNodes.clear();
                currentNodeIndex = 0;

                for (Map.Entry<String, FallbackAggregation> entry : aggregationsByGroup.entrySet()) {
                    FallbackAggregation agg = entry.getValue();
                    // Create a node with the aggregated values (node is null for fallback)
                    // Ordinals array is not used for fallback nodes (we use groupingValues in fallbackAgg instead)
                    pendingFallbackNodes.add(new NodeWithOrdinals(null, null, agg));
                    logger.info("Star-tree: fallback group [{}]: docCount={}, sums={}, counts={}",
                        entry.getKey(), agg.docCount, java.util.Arrays.toString(agg.sums), java.util.Arrays.toString(agg.counts));
                }

                logger.info("Star-tree: fallback leaf {} produced {} groups", currentFallbackLeafIndex, pendingFallbackNodes.size());
                currentFallbackLeafIndex++;

                if (pendingFallbackNodes.isEmpty() == false) {
                    Page page = buildPageFromFallbackNodes();
                    logger.info("Star-tree: built fallback page with {} positions", page != null ? page.getPositionCount() : 0);
                    return page;
                }
            }
            return null;
        }

        /**
         * Aggregate documents in a fallback leaf.
         */
        private Map<String, FallbackAggregation> aggregateFallbackLeaf(LeafReader leafReader) throws IOException {
            Map<String, FallbackAggregation> aggregationsByGroup = new HashMap<>();

            // Get doc values for grouping fields
            SortedSetDocValues[] groupingDvs = new SortedSetDocValues[groupByFields.size()];
            for (int i = 0; i < groupByFields.size(); i++) {
                groupingDvs[i] = leafReader.getSortedSetDocValues(groupByFields.get(i));
            }

            // Get doc values for metric fields (we need to collect unique metric fields from aggregations)
            Map<String, SortedNumericDocValues> metricDvs = new HashMap<>();
            for (StarTreeAgg agg : aggregations) {
                if (agg.valueField() != null && metricDvs.containsKey(agg.valueField()) == false) {
                    SortedNumericDocValues dv = leafReader.getSortedNumericDocValues(agg.valueField());
                    metricDvs.put(agg.valueField(), dv);
                }
            }

            // Scan all documents
            int maxDoc = leafReader.maxDoc();
            for (int docId = 0; docId < maxDoc; docId++) {
                // Build group key from grouping field values
                StringBuilder groupKeyBuilder = new StringBuilder();
                BytesRef[] groupingValues = new BytesRef[groupByFields.size()];

                for (int i = 0; i < groupByFields.size(); i++) {
                    if (groupingDvs[i] != null && groupingDvs[i].advanceExact(docId)) {
                        long ord = groupingDvs[i].nextOrd();
                        // Look up the actual keyword value
                        BytesRef value = groupingDvs[i].lookupOrd(ord);
                        groupingValues[i] = BytesRef.deepCopyOf(value);
                        groupKeyBuilder.append(value.utf8ToString()).append("|");
                    } else {
                        // Missing grouping value
                        groupingValues[i] = null;
                        groupKeyBuilder.append("_null_|");
                    }
                }

                String groupKey = groupByFields.isEmpty() ? "_all_" : groupKeyBuilder.toString();

                // Get or create aggregation for this group
                FallbackAggregation agg = aggregationsByGroup.get(groupKey);
                if (agg == null) {
                    agg = new FallbackAggregation(groupingValues, aggregations.size());
                    aggregationsByGroup.put(groupKey, agg);
                }

                // Update aggregations
                agg.docCount++;

                for (int aggIdx = 0; aggIdx < aggregations.size(); aggIdx++) {
                    StarTreeAgg starTreeAgg = aggregations.get(aggIdx);
                    if (starTreeAgg.valueField() == null) {
                        // COUNT(*) - already counted above
                        continue;
                    }

                    SortedNumericDocValues metricDv = metricDvs.get(starTreeAgg.valueField());
                    if (metricDv != null && metricDv.advanceExact(docId)) {
                        int valueCount = metricDv.docValueCount();
                        for (int v = 0; v < valueCount; v++) {
                            long rawValue = metricDv.nextValue();
                            // For double fields, rawValue is encoded using NumericUtils.doubleToSortableLong
                            // For long/int fields, rawValue is the actual value
                            double value = starTreeAgg.isLongField() ? (double) rawValue : NumericUtils.sortableLongToDouble(rawValue);

                            // Update aggregation based on type
                            // Note: we track counts for SUM as well to properly set the "seen" flag
                            switch (starTreeAgg.type()) {
                                case SUM -> {
                                    agg.sums[aggIdx] += value;
                                    agg.counts[aggIdx]++;  // Track that we've seen values for this aggregation
                                }
                                case COUNT -> agg.counts[aggIdx]++;
                                case MIN -> {
                                    if (agg.hasMinMax[aggIdx] == false || value < agg.mins[aggIdx]) {
                                        agg.mins[aggIdx] = value;
                                        agg.hasMinMax[aggIdx] = true;
                                    }
                                }
                                case MAX -> {
                                    if (agg.hasMinMax[aggIdx] == false || value > agg.maxs[aggIdx]) {
                                        agg.maxs[aggIdx] = value;
                                        agg.hasMinMax[aggIdx] = true;
                                    }
                                }
                                case AVG -> {
                                    // AVG is computed from SUM/COUNT
                                    agg.sums[aggIdx] += value;
                                    agg.counts[aggIdx]++;
                                }
                            }
                        }
                    }
                }
            }

            return aggregationsByGroup;
        }

        /**
         * Build a page from fallback aggregation results.
         */
        private Page buildPageFromFallbackNodes() {
            int count = Math.min(pageSize, pendingFallbackNodes.size() - currentNodeIndex);
            if (count <= 0) {
                pendingFallbackNodes.clear();
                currentNodeIndex = 0;
                return null;
            }

            // Calculate total number of output blocks (same as buildPageFromPendingNodes)
            int numOutputs = groupByFields.size();
            if (mode == AggregatorMode.INITIAL) {
                for (StarTreeAgg agg : aggregations) {
                    numOutputs += getIntermediateBlockCount(agg);
                }
            } else {
                numOutputs += aggregations.size();
            }

            Block[] blocks = new Block[numOutputs];

            try {
                // Build grouping field blocks
                int numGroupByFields = groupByFields.size();
                for (int g = 0; g < numGroupByFields; g++) {
                    // For fallback, we use the actual keyword values stored in FallbackAggregation
                    try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(count)) {
                        for (int i = 0; i < count; i++) {
                            NodeWithOrdinals nodeWithOrdinals = pendingFallbackNodes.get(currentNodeIndex + i);
                            FallbackAggregation fallbackAgg = nodeWithOrdinals.fallbackAgg();
                            BytesRef value = fallbackAgg.groupingValues[g];
                            if (value != null) {
                                builder.appendBytesRef(value);
                            } else {
                                builder.appendNull();
                            }
                        }
                        blocks[g] = builder.build();
                    }
                }

                // Build aggregation blocks using the same logic as star-tree
                int blockIdx = numGroupByFields;
                for (StarTreeAgg agg : aggregations) {
                    blockIdx = buildFallbackAggregationBlocks(blocks, blockIdx, agg, count);
                }

                currentNodeIndex += count;
                if (currentNodeIndex >= pendingFallbackNodes.size()) {
                    pendingFallbackNodes.clear();
                    currentNodeIndex = 0;
                }

                return new Page(count, blocks);
            } catch (Exception e) {
                // Clean up on failure
                for (Block block : blocks) {
                    if (block != null) {
                        block.close();
                    }
                }
                throw e;
            }
        }

        private int buildFallbackAggregationBlocks(Block[] blocks, int blockIdx, StarTreeAgg agg, int count) {
            if (mode == AggregatorMode.INITIAL) {
                // INITIAL mode: output intermediate format
                switch (agg.type()) {
                    case SUM -> {
                        int aggIdx = aggregations.indexOf(agg);
                        if (agg.isLongField()) {
                            // SUM on long: (LONG sum, BOOLEAN seen)
                            try (LongBlock.Builder sumBuilder = blockFactory.newLongBlockBuilder(count);
                                 BooleanBlock.Builder seenBuilder = blockFactory.newBooleanBlockBuilder(count)) {

                                for (int i = 0; i < count; i++) {
                                    NodeWithOrdinals nodeWithOrdinals = pendingFallbackNodes.get(currentNodeIndex + i);
                                    FallbackAggregation fallbackAgg = nodeWithOrdinals.fallbackAgg();
                                    sumBuilder.appendLong((long) fallbackAgg.sums[aggIdx]);
                                    seenBuilder.appendBoolean(fallbackAgg.counts[aggIdx] > 0);
                                }
                                blocks[blockIdx++] = sumBuilder.build();
                                blocks[blockIdx++] = seenBuilder.build();
                            }
                        } else {
                            // SUM on double: (DOUBLE value, DOUBLE delta, BOOLEAN seen)
                            try (DoubleBlock.Builder sumBuilder = blockFactory.newDoubleBlockBuilder(count);
                                 DoubleBlock.Builder deltaBuilder = blockFactory.newDoubleBlockBuilder(count);
                                 BooleanBlock.Builder seenBuilder = blockFactory.newBooleanBlockBuilder(count)) {

                                for (int i = 0; i < count; i++) {
                                    NodeWithOrdinals nodeWithOrdinals = pendingFallbackNodes.get(currentNodeIndex + i);
                                    FallbackAggregation fallbackAgg = nodeWithOrdinals.fallbackAgg();
                                    sumBuilder.appendDouble(fallbackAgg.sums[aggIdx]);
                                    deltaBuilder.appendDouble(0.0);
                                    seenBuilder.appendBoolean(fallbackAgg.counts[aggIdx] > 0);
                                }
                                blocks[blockIdx++] = sumBuilder.build();
                                blocks[blockIdx++] = deltaBuilder.build();
                                blocks[blockIdx++] = seenBuilder.build();
                            }
                        }
                    }
                    case COUNT -> {
                        // COUNT outputs: [count, seen]
                        try (LongBlock.Builder countBuilder = blockFactory.newLongBlockBuilder(count);
                             BooleanBlock.Builder seenBuilder = blockFactory.newBooleanBlockBuilder(count)) {

                            for (int i = 0; i < count; i++) {
                                NodeWithOrdinals nodeWithOrdinals = pendingFallbackNodes.get(currentNodeIndex + i);
                                FallbackAggregation fallbackAgg = nodeWithOrdinals.fallbackAgg();
                                int aggIdx = aggregations.indexOf(agg);
                                long countVal = agg.valueField() == null ? fallbackAgg.docCount : fallbackAgg.counts[aggIdx];
                                countBuilder.appendLong(countVal);
                                seenBuilder.appendBoolean(true);
                            }
                            blocks[blockIdx++] = countBuilder.build();
                            blocks[blockIdx++] = seenBuilder.build();
                        }
                    }
                    case MIN, MAX -> {
                        // MIN/MAX outputs: (value, BOOLEAN seen)
                        int aggIdx = aggregations.indexOf(agg);
                        if (agg.isLongField()) {
                            try (LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(count);
                                 BooleanBlock.Builder seenBuilder = blockFactory.newBooleanBlockBuilder(count)) {

                                for (int i = 0; i < count; i++) {
                                    NodeWithOrdinals nodeWithOrdinals = pendingFallbackNodes.get(currentNodeIndex + i);
                                    FallbackAggregation fallbackAgg = nodeWithOrdinals.fallbackAgg();
                                    double value = agg.type() == StarTreeAggType.MIN
                                        ? fallbackAgg.mins[aggIdx]
                                        : fallbackAgg.maxs[aggIdx];
                                    boolean seen = fallbackAgg.hasMinMax[aggIdx];
                                    valueBuilder.appendLong(seen ? (long) value : 0L);
                                    seenBuilder.appendBoolean(seen);
                                }
                                blocks[blockIdx++] = valueBuilder.build();
                                blocks[blockIdx++] = seenBuilder.build();
                            }
                        } else {
                            try (DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(count);
                                 BooleanBlock.Builder seenBuilder = blockFactory.newBooleanBlockBuilder(count)) {

                                for (int i = 0; i < count; i++) {
                                    NodeWithOrdinals nodeWithOrdinals = pendingFallbackNodes.get(currentNodeIndex + i);
                                    FallbackAggregation fallbackAgg = nodeWithOrdinals.fallbackAgg();
                                    double value = agg.type() == StarTreeAggType.MIN
                                        ? fallbackAgg.mins[aggIdx]
                                        : fallbackAgg.maxs[aggIdx];
                                    boolean seen = fallbackAgg.hasMinMax[aggIdx];
                                    valueBuilder.appendDouble(seen ? value : Double.NaN);
                                    seenBuilder.appendBoolean(seen);
                                }
                                blocks[blockIdx++] = valueBuilder.build();
                                blocks[blockIdx++] = seenBuilder.build();
                            }
                        }
                    }
                    case AVG -> {
                        // AVG shouldn't reach here in INITIAL mode (it's decomposed into SUM+COUNT)
                        // But handle it just in case - output (value, seen)
                        int aggIdx = aggregations.indexOf(agg);
                        try (DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(count);
                             BooleanBlock.Builder seenBuilder = blockFactory.newBooleanBlockBuilder(count)) {

                            for (int i = 0; i < count; i++) {
                                NodeWithOrdinals nodeWithOrdinals = pendingFallbackNodes.get(currentNodeIndex + i);
                                FallbackAggregation fallbackAgg = nodeWithOrdinals.fallbackAgg();
                                double value = fallbackAgg.counts[aggIdx] > 0
                                    ? fallbackAgg.sums[aggIdx] / fallbackAgg.counts[aggIdx]
                                    : Double.NaN;
                                valueBuilder.appendDouble(value);
                                seenBuilder.appendBoolean(fallbackAgg.counts[aggIdx] > 0);
                            }
                            blocks[blockIdx++] = valueBuilder.build();
                            blocks[blockIdx++] = seenBuilder.build();
                        }
                    }
                }
            } else {
                // SINGLE mode: output final values
                // Use LongBlock for COUNT and SUM/MIN/MAX on integer fields, DoubleBlock otherwise
                boolean useLongBlock = agg.type() == StarTreeAggType.COUNT || agg.isLongField();
                int aggIdx = aggregations.indexOf(agg);

                if (useLongBlock) {
                    try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(count)) {
                        for (int i = 0; i < count; i++) {
                            NodeWithOrdinals nodeWithOrdinals = pendingFallbackNodes.get(currentNodeIndex + i);
                            FallbackAggregation fallbackAgg = nodeWithOrdinals.fallbackAgg();

                            long value = switch (agg.type()) {
                                case SUM -> (long) fallbackAgg.sums[aggIdx];
                                case COUNT -> agg.valueField() == null
                                    ? fallbackAgg.docCount
                                    : fallbackAgg.counts[aggIdx];
                                case MIN -> (long) fallbackAgg.mins[aggIdx];
                                case MAX -> (long) fallbackAgg.maxs[aggIdx];
                                case AVG -> fallbackAgg.counts[aggIdx] > 0
                                    ? (long) (fallbackAgg.sums[aggIdx] / fallbackAgg.counts[aggIdx])
                                    : 0L;
                            };
                            builder.appendLong(value);
                        }
                        blocks[blockIdx++] = builder.build();
                    }
                } else {
                    try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(count)) {
                        for (int i = 0; i < count; i++) {
                            NodeWithOrdinals nodeWithOrdinals = pendingFallbackNodes.get(currentNodeIndex + i);
                            FallbackAggregation fallbackAgg = nodeWithOrdinals.fallbackAgg();

                            double value = switch (agg.type()) {
                                case SUM -> fallbackAgg.sums[aggIdx];
                                case COUNT -> agg.valueField() == null
                                    ? (double) fallbackAgg.docCount
                                    : (double) fallbackAgg.counts[aggIdx];
                                case MIN -> fallbackAgg.mins[aggIdx];
                                case MAX -> fallbackAgg.maxs[aggIdx];
                                case AVG -> fallbackAgg.counts[aggIdx] > 0
                                    ? fallbackAgg.sums[aggIdx] / fallbackAgg.counts[aggIdx]
                                    : Double.NaN;
                            };
                            builder.appendDouble(value);
                        }
                        blocks[blockIdx++] = builder.build();
                    }
                }
            }
            return blockIdx;
        }

        /**
         * Holds aggregation state for a single group during fallback processing.
         */
        private static class FallbackAggregation {
            final BytesRef[] groupingValues;  // Actual keyword values for grouping fields
            long docCount;
            final double[] sums;
            final long[] counts;
            final double[] mins;
            final double[] maxs;
            final boolean[] hasMinMax;

            FallbackAggregation(BytesRef[] groupingValues, int numAggs) {
                this.groupingValues = groupingValues;
                this.docCount = 0;
                this.sums = new double[numAggs];
                this.counts = new long[numAggs];
                this.mins = new double[numAggs];
                this.maxs = new double[numAggs];
                this.hasMinMax = new boolean[numAggs];
                for (int i = 0; i < numAggs; i++) {
                    mins[i] = Double.MAX_VALUE;
                    maxs[i] = Double.MIN_VALUE;
                }
            }
        }

        // ========== End Fallback Processing ==========

        /**
         * Accumulator for aggregate-only queries (no GROUP BY).
         * Combines partial results from all segments (star-tree and fallback) into a single result.
         */
        private static class AggregateOnlyAccumulator {
            final double[] sums;
            final long[] counts;
            final double[] mins;
            final double[] maxs;
            final boolean[] hasMinMax;
            long docCount;
            boolean hasData = false;

            AggregateOnlyAccumulator(int numAggs) {
                this.sums = new double[numAggs];
                this.counts = new long[numAggs];
                this.mins = new double[numAggs];
                this.maxs = new double[numAggs];
                this.hasMinMax = new boolean[numAggs];
                for (int i = 0; i < numAggs; i++) {
                    mins[i] = Double.MAX_VALUE;
                    maxs[i] = -Double.MAX_VALUE;
                }
            }

            void addFromStarTreeNode(StarTreeNode node, List<StarTreeAgg> aggregations, StarTreeReader reader) {
                hasData = true;
                docCount += node.getDocCount();
                for (int i = 0; i < aggregations.size(); i++) {
                    StarTreeAgg agg = aggregations.get(i);
                    int valueIndex = reader.getValueIndex(agg.valueField());
                    if (valueIndex < 0) continue;

                    switch (agg.type()) {
                        case SUM -> sums[i] += node.getAggregatedValue(valueIndex, StarTreeAggregationType.SUM);
                        case COUNT -> counts[i] += (long) node.getAggregatedValue(valueIndex, StarTreeAggregationType.COUNT);
                        case MIN -> {
                            double min = node.getAggregatedValue(valueIndex, StarTreeAggregationType.MIN);
                            if (!hasMinMax[i] || min < mins[i]) {
                                mins[i] = min;
                                hasMinMax[i] = true;
                            }
                        }
                        case MAX -> {
                            double max = node.getAggregatedValue(valueIndex, StarTreeAggregationType.MAX);
                            if (!hasMinMax[i] || max > maxs[i]) {
                                maxs[i] = max;
                                hasMinMax[i] = true;
                            }
                        }
                        case AVG -> {
                            // For AVG, accumulate sum and count separately
                            sums[i] += node.getAggregatedValue(valueIndex, StarTreeAggregationType.SUM);
                            counts[i] += (long) node.getAggregatedValue(valueIndex, StarTreeAggregationType.COUNT);
                        }
                    }
                }
            }

            void addFromFallbackAggregation(FallbackAggregation fallback, List<StarTreeAgg> aggregations) {
                hasData = true;
                docCount += fallback.docCount;
                for (int i = 0; i < aggregations.size(); i++) {
                    StarTreeAgg agg = aggregations.get(i);
                    switch (agg.type()) {
                        case SUM, AVG -> {
                            // Both SUM and AVG need to accumulate sums and counts
                            sums[i] += fallback.sums[i];
                            counts[i] += fallback.counts[i];
                        }
                        case COUNT -> counts[i] += fallback.counts[i];
                        case MIN -> {
                            if (fallback.hasMinMax[i] && (!hasMinMax[i] || fallback.mins[i] < mins[i])) {
                                mins[i] = fallback.mins[i];
                                hasMinMax[i] = true;
                            }
                        }
                        case MAX -> {
                            if (fallback.hasMinMax[i] && (!hasMinMax[i] || fallback.maxs[i] > maxs[i])) {
                                maxs[i] = fallback.maxs[i];
                                hasMinMax[i] = true;
                            }
                        }
                    }
                }
            }
        }

        // ========== Aggregate-Only Accumulation Methods ==========

        /**
         * Build a combined page from the aggregate-only accumulator.
         * This is called once at the end after all segments (star-tree and fallback) are processed.
         */
        private Page buildCombinedAggregateOnlyPage() {
            if (aggregateOnlyAccumulator == null || !aggregateOnlyAccumulator.hasData) {
                return null;
            }

            // Calculate number of blocks (same logic as buildAggregateOnlyPage)
            int numOutputs;
            if (mode == AggregatorMode.INITIAL) {
                numOutputs = 0;
                for (StarTreeAgg agg : aggregations) {
                    numOutputs += getIntermediateBlockCount(agg);
                }
            } else {
                numOutputs = aggregations.size();
            }
            Block[] blocks = new Block[numOutputs];

            try {
                int blockIndex = 0;
                for (int a = 0; a < aggregations.size(); a++) {
                    StarTreeAgg agg = aggregations.get(a);

                    // Get the combined value from accumulator
                    double value = switch (agg.type()) {
                        case SUM -> aggregateOnlyAccumulator.sums[a];
                        case COUNT -> (double) aggregateOnlyAccumulator.counts[a];
                        case MIN -> aggregateOnlyAccumulator.mins[a];
                        case MAX -> aggregateOnlyAccumulator.maxs[a];
                        case AVG -> aggregateOnlyAccumulator.counts[a] > 0
                            ? aggregateOnlyAccumulator.sums[a] / aggregateOnlyAccumulator.counts[a]
                            : Double.NaN;
                    };

                    logger.info("Star-tree: combined aggregate [{}] {} = {}", agg.outputName(), agg.type(), value);

                    // Build value block
                    boolean useLongBlock = agg.type() == StarTreeAggType.COUNT || agg.isLongField();
                    if (useLongBlock) {
                        try (LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(1)) {
                            valueBuilder.appendLong((long) value);
                            blocks[blockIndex++] = valueBuilder.build();
                        }
                    } else {
                        try (DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(1)) {
                            valueBuilder.appendDouble(value);
                            blocks[blockIndex++] = valueBuilder.build();
                        }
                    }

                    // For INITIAL mode, add intermediate state blocks
                    if (mode == AggregatorMode.INITIAL) {
                        // SUM on double fields needs delta block for Kahan compensated summation
                        if (agg.type() == StarTreeAggType.SUM && agg.isLongField() == false) {
                            try (DoubleBlock.Builder deltaBuilder = blockFactory.newDoubleBlockBuilder(1)) {
                                deltaBuilder.appendDouble(0.0);  // Delta is 0 for pre-aggregated values
                                blocks[blockIndex++] = deltaBuilder.build();
                            }
                        }
                        // All aggregations need a "seen" boolean block
                        try (BooleanBlock.Builder seenBuilder = blockFactory.newBooleanBlockBuilder(1)) {
                            seenBuilder.appendBoolean(true);
                            blocks[blockIndex++] = seenBuilder.build();
                        }
                    }
                }

                return new Page(1, blocks);
            } catch (Exception e) {
                for (Block block : blocks) {
                    if (block != null) {
                        block.close();
                    }
                }
                throw e;
            }
        }

        private Page buildPageFromPendingNodes() {
            int count = Math.min(pageSize, pendingNodesWithOrdinals.size() - currentNodeIndex);
            if (count <= 0) {
                pendingNodesWithOrdinals.clear();
                currentNodeIndex = 0;
                return null;
            }

            // Calculate total number of output blocks
            // For INITIAL mode, aggregation intermediate state varies by type:
            // - SUM on double/float: 3 blocks (value, delta, seen)
            // - SUM on long/int: 2 blocks (sum, seen)
            // - COUNT: 2 blocks (count, seen)
            // - MIN/MAX: 2 blocks (value, seen)
            int numOutputs = groupByFields.size();
            if (mode == AggregatorMode.INITIAL) {
                for (StarTreeAgg agg : aggregations) {
                    numOutputs += getIntermediateBlockCount(agg);
                }
            } else {
                numOutputs += aggregations.size();
            }
            Block[] blocks = new Block[numOutputs];

            try {
                // Build grouping field blocks
                // For ORDINAL fields: use BytesRefBlock with actual keyword values
                // For DATE_HISTOGRAM fields: use LongBlock with bucketed timestamp (ordinal IS the timestamp)
                for (int g = 0; g < groupByFields.size(); g++) {
                    StarTreeGroupingField.GroupingType fieldType = groupingFieldTypes != null
                        ? groupingFieldTypes[g]
                        : StarTreeGroupingField.GroupingType.ORDINAL;

                    if (fieldType == StarTreeGroupingField.GroupingType.DATE_HISTOGRAM) {
                        // DATE_HISTOGRAM: ordinal is the bucketed timestamp in milliseconds
                        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(count)) {
                            for (int i = 0; i < count; i++) {
                                NodeWithOrdinals nodeWithOrdinals = pendingNodesWithOrdinals.get(currentNodeIndex + i);
                                long bucketedTimestamp = nodeWithOrdinals.ordinals()[g];
                                builder.appendLong(bucketedTimestamp);
                            }
                            blocks[g] = builder.build();
                        }
                    } else {
                        // ORDINAL: look up keyword value from doc values
                        SortedSetDocValues dv = groupingFieldDocValues != null ? groupingFieldDocValues[g] : null;
                        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(count)) {
                            for (int i = 0; i < count; i++) {
                                NodeWithOrdinals nodeWithOrdinals = pendingNodesWithOrdinals.get(currentNodeIndex + i);
                                long ordinal = nodeWithOrdinals.ordinals()[g];
                                BytesRef value = lookupOrdinalValue(dv, ordinal);
                                if (value != null) {
                                    builder.appendBytesRef(value);
                                } else {
                                    builder.appendNull();
                                }
                            }
                            blocks[g] = builder.build();
                        }
                    }
                }

                // Build aggregation blocks
                int blockIndex = groupByFields.size();
                for (int a = 0; a < aggregations.size(); a++) {
                    StarTreeAgg agg = aggregations.get(a);
                    int valueIndex = currentReader != null ? currentReader.getValueIndex(agg.valueField()) : -1;

                    if (mode == AggregatorMode.INITIAL) {
                        // Output intermediate state format for INITIAL mode
                        blockIndex = buildIntermediateAggBlocks(blocks, blockIndex, agg, valueIndex, count);
                    } else {
                        // SINGLE mode - output final values directly
                        boolean useLongBlock = agg.type() == StarTreeAggType.COUNT || agg.isLongField();
                        if (useLongBlock) {
                            try (LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(count)) {
                                for (int i = 0; i < count; i++) {
                                    StarTreeNode node = pendingNodesWithOrdinals.get(currentNodeIndex + i).node();
                                    double value = getAggregatedValue(node, agg, valueIndex);
                                    valueBuilder.appendLong((long) value);
                                }
                                blocks[blockIndex++] = valueBuilder.build();
                            }
                        } else {
                            try (DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(count)) {
                                for (int i = 0; i < count; i++) {
                                    StarTreeNode node = pendingNodesWithOrdinals.get(currentNodeIndex + i).node();
                                    double value = getAggregatedValue(node, agg, valueIndex);
                                    valueBuilder.appendDouble(value);
                                }
                                blocks[blockIndex++] = valueBuilder.build();
                            }
                        }
                    }
                }

                currentNodeIndex += count;
                if (currentNodeIndex >= pendingNodesWithOrdinals.size()) {
                    pendingNodesWithOrdinals.clear();
                    currentNodeIndex = 0;
                }

                return new Page(count, blocks);
            } catch (Exception e) {
                for (Block block : blocks) {
                    if (block != null) {
                        block.close();
                    }
                }
                throw e;
            }
        }

        private Page buildAggregateOnlyPage(StarTreeNode node) {
            // For INITIAL mode, each aggregation outputs value + seen boolean
            int aggOutputsPerAgg = (mode == AggregatorMode.INITIAL) ? 2 : 1;
            int numOutputs = aggregations.size() * aggOutputsPerAgg;
            Block[] blocks = new Block[numOutputs];

            try {
                int blockIndex = 0;
                for (int a = 0; a < aggregations.size(); a++) {
                    StarTreeAgg agg = aggregations.get(a);
                    int valueIndex = currentReader != null ? currentReader.getValueIndex(agg.valueField()) : -1;

                    double value = getAggregatedValue(node, agg, valueIndex);

                    // Build value block - use LONG for COUNT and SUM/MIN/MAX on integer fields, DOUBLE otherwise
                    boolean useLongBlock = agg.type() == StarTreeAggType.COUNT || agg.isLongField();
                    if (useLongBlock) {
                        try (LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(1)) {
                            valueBuilder.appendLong((long) value);
                            blocks[blockIndex++] = valueBuilder.build();
                        }
                    } else {
                        try (DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(1)) {
                            valueBuilder.appendDouble(value);
                            blocks[blockIndex++] = valueBuilder.build();
                        }
                    }

                    // For INITIAL mode, also build "seen" boolean block
                    if (mode == AggregatorMode.INITIAL) {
                        try (BooleanBlock.Builder seenBuilder = blockFactory.newBooleanBlockBuilder(1)) {
                            seenBuilder.appendBoolean(true);
                            blocks[blockIndex++] = seenBuilder.build();
                        }
                    }
                }

                return new Page(1, blocks);
            } catch (Exception e) {
                for (Block block : blocks) {
                    if (block != null) {
                        block.close();
                    }
                }
                throw e;
            }
        }

        private double getAggregatedValue(StarTreeNode node, StarTreeAgg agg, int valueIndex) {
            if (valueIndex < 0) {
                return Double.NaN;
            }

            StarTreeAggregationType aggregationType = switch (agg.type()) {
                case SUM -> StarTreeAggregationType.SUM;
                case COUNT -> StarTreeAggregationType.COUNT;
                case MIN -> StarTreeAggregationType.MIN;
                case MAX -> StarTreeAggregationType.MAX;
                case AVG -> null; // AVG is computed from SUM/COUNT
            };

            if (aggregationType == null) {
                // AVG case: compute from SUM and COUNT
                double sum = node.getAggregatedValue(valueIndex, StarTreeAggregationType.SUM);
                double count = node.getAggregatedValue(valueIndex, StarTreeAggregationType.COUNT);
                return count > 0 ? sum / count : Double.NaN;
            }

            return node.getAggregatedValue(valueIndex, aggregationType);
        }

        /**
         * Look up the actual keyword value from an ordinal using the doc values.
         *
         * @param dv the sorted set doc values for the field
         * @param ordinal the ordinal to look up
         * @return the BytesRef value, or null if lookup fails
         */
        private BytesRef lookupOrdinalValue(SortedSetDocValues dv, long ordinal) {
            if (dv == null || ordinal < 0) {
                return null;
            }
            try {
                return dv.lookupOrd(ordinal);
            } catch (IOException e) {
                logger.warn("Failed to lookup ordinal [{}]: {}", ordinal, e.getMessage());
                return null;
            }
        }

        /**
         * Get the number of intermediate blocks for an aggregation type in INITIAL mode.
         * - SUM on double/float: 3 blocks (value, delta, seen)
         * - SUM on long/int: 2 blocks (sum, seen)
         * - COUNT: 2 blocks (count, seen)
         * - MIN/MAX: 2 blocks (value, seen)
         */
        private int getIntermediateBlockCount(StarTreeAgg agg) {
            if (agg.type() == StarTreeAggType.SUM && agg.isLongField() == false) {
                // SUM on double fields uses Kahan compensated summation with delta
                return 3;
            }
            // All others: (value, seen)
            return 2;
        }

        /**
         * Build intermediate state blocks for an aggregation in INITIAL mode.
         * Returns the next block index to use.
         */
        private int buildIntermediateAggBlocks(Block[] blocks, int blockIndex, StarTreeAgg agg, int valueIndex, int count) {
            switch (agg.type()) {
                case SUM -> {
                    if (agg.isLongField()) {
                        // SUM on long: (LONG sum, BOOLEAN seen)
                        try (LongBlock.Builder sumBuilder = blockFactory.newLongBlockBuilder(count)) {
                            for (int i = 0; i < count; i++) {
                                StarTreeNode node = pendingNodesWithOrdinals.get(currentNodeIndex + i).node();
                                double value = getAggregatedValue(node, agg, valueIndex);
                                sumBuilder.appendLong((long) value);
                            }
                            blocks[blockIndex++] = sumBuilder.build();
                        }
                    } else {
                        // SUM on double: (DOUBLE value, DOUBLE delta, BOOLEAN seen)
                        try (DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(count)) {
                            for (int i = 0; i < count; i++) {
                                StarTreeNode node = pendingNodesWithOrdinals.get(currentNodeIndex + i).node();
                                double value = getAggregatedValue(node, agg, valueIndex);
                                valueBuilder.appendDouble(value);
                            }
                            blocks[blockIndex++] = valueBuilder.build();
                        }
                        // Delta for Kahan summation - 0.0 for pre-aggregated values
                        try (DoubleBlock.Builder deltaBuilder = blockFactory.newDoubleBlockBuilder(count)) {
                            for (int i = 0; i < count; i++) {
                                deltaBuilder.appendDouble(0.0);
                            }
                            blocks[blockIndex++] = deltaBuilder.build();
                        }
                    }
                }
                case COUNT -> {
                    // COUNT: (LONG count, BOOLEAN seen)
                    try (LongBlock.Builder countBuilder = blockFactory.newLongBlockBuilder(count)) {
                        for (int i = 0; i < count; i++) {
                            StarTreeNode node = pendingNodesWithOrdinals.get(currentNodeIndex + i).node();
                            double value = getAggregatedValue(node, agg, valueIndex);
                            countBuilder.appendLong((long) value);
                        }
                        blocks[blockIndex++] = countBuilder.build();
                    }
                }
                case MIN, MAX -> {
                    // MIN/MAX: (value, BOOLEAN seen)
                    if (agg.isLongField()) {
                        try (LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(count)) {
                            for (int i = 0; i < count; i++) {
                                StarTreeNode node = pendingNodesWithOrdinals.get(currentNodeIndex + i).node();
                                double value = getAggregatedValue(node, agg, valueIndex);
                                valueBuilder.appendLong((long) value);
                            }
                            blocks[blockIndex++] = valueBuilder.build();
                        }
                    } else {
                        try (DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(count)) {
                            for (int i = 0; i < count; i++) {
                                StarTreeNode node = pendingNodesWithOrdinals.get(currentNodeIndex + i).node();
                                double value = getAggregatedValue(node, agg, valueIndex);
                                valueBuilder.appendDouble(value);
                            }
                            blocks[blockIndex++] = valueBuilder.build();
                        }
                    }
                }
                case AVG -> {
                    // AVG shouldn't reach here in INITIAL mode (it's decomposed into SUM+COUNT)
                    // But handle it just in case
                    try (DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(count)) {
                        for (int i = 0; i < count; i++) {
                            StarTreeNode node = pendingNodesWithOrdinals.get(currentNodeIndex + i).node();
                            double value = getAggregatedValue(node, agg, valueIndex);
                            valueBuilder.appendDouble(value);
                        }
                        blocks[blockIndex++] = valueBuilder.build();
                    }
                }
            }

            // All aggregation types have a "seen" boolean block at the end
            try (BooleanBlock.Builder seenBuilder = blockFactory.newBooleanBlockBuilder(count)) {
                for (int i = 0; i < count; i++) {
                    // Star-tree nodes always have data (seen = true)
                    seenBuilder.appendBoolean(true);
                }
                blocks[blockIndex++] = seenBuilder.build();
            }

            return blockIndex;
        }

        private void closeCurrentReader() {
            if (currentReader != null) {
                try {
                    currentReader.close();
                } catch (IOException e) {
                    // Log warning but don't fail - this may indicate resource pressure
                    logger.warn("Failed to close star-tree reader: {}", e.getMessage());
                }
                currentReader = null;
                currentTraverser = null;
            }
            currentLeafReader = null;
            groupingFieldDocValues = null;
            groupingFieldTypes = null;
            stringToOrdinalMaps = null;
        }

        @Override
        public void close() {
            closeCurrentReader();
            // Decrement reference count on shard contexts
            shardContexts.iterable().forEach(RefCounted::decRef);
        }
    }

    /**
     * Status of the StarTreeSourceOperator for the profile API.
     * Shows whether the star-tree is being used and statistics about processing.
     */
    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "star_tree_source",
            Status::new
        );

        private final String starTreeName;
        private final int starTreeSegments;
        private final int fallbackSegments;
        private final long rowsEmitted;
        private final List<String> groupByFields;
        private final List<String> aggregations;

        Status(StarTreeMultiShardSourceOperator operator) {
            this.starTreeName = operator.starTreeName;
            this.starTreeSegments = operator.starTreeSegmentsProcessed;
            this.fallbackSegments = operator.fallbackSegmentsProcessed;
            this.rowsEmitted = operator.totalRowsEmitted;
            this.groupByFields = List.copyOf(operator.groupByFields);
            this.aggregations = operator.aggregations.stream()
                .map(agg -> agg.type() + "(" + (agg.valueField() != null ? agg.valueField() : "*") + ")")
                .toList();
        }

        Status(StreamInput in) throws IOException {
            this.starTreeName = in.readString();
            this.starTreeSegments = in.readVInt();
            this.fallbackSegments = in.readVInt();
            this.rowsEmitted = in.readVLong();
            this.groupByFields = in.readStringCollectionAsList();
            this.aggregations = in.readStringCollectionAsList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(starTreeName);
            out.writeVInt(starTreeSegments);
            out.writeVInt(fallbackSegments);
            out.writeVLong(rowsEmitted);
            out.writeStringCollection(groupByFields);
            out.writeStringCollection(aggregations);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("star_tree_name", starTreeName);
            builder.field("star_tree_segments", starTreeSegments);
            builder.field("fallback_segments", fallbackSegments);
            builder.field("using_star_tree", starTreeSegments > 0);
            builder.field("rows_emitted", rowsEmitted);
            builder.array("group_by_fields", groupByFields.toArray(String[]::new));
            builder.array("aggregations", aggregations.toArray(String[]::new));
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return "star_tree_source";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        public String starTreeName() {
            return starTreeName;
        }

        public int starTreeSegments() {
            return starTreeSegments;
        }

        public int fallbackSegments() {
            return fallbackSegments;
        }

        public long rowsEmitted() {
            return rowsEmitted;
        }

        public boolean isUsingStarTree() {
            return starTreeSegments > 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return starTreeSegments == status.starTreeSegments
                && fallbackSegments == status.fallbackSegments
                && rowsEmitted == status.rowsEmitted
                && Objects.equals(starTreeName, status.starTreeName)
                && Objects.equals(groupByFields, status.groupByFields)
                && Objects.equals(aggregations, status.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(starTreeName, starTreeSegments, fallbackSegments, rowsEmitted, groupByFields, aggregations);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
