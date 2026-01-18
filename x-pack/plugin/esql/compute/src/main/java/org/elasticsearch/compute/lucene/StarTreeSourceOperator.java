/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.index.codec.startree.StarTreeNode;
import org.elasticsearch.index.codec.startree.StarTreeReader;
import org.elasticsearch.index.mapper.startree.StarTreeAggregationType;

import java.io.IOException;
import java.util.List;

/**
 * Source operator that reads pre-aggregated data from a star-tree index.
 * <p>
 * This operator traverses the star-tree based on dimension filters and GROUP BY dimensions,
 * returning pre-computed aggregations without scanning individual documents.
 */
public class StarTreeSourceOperator extends SourceOperator {

    private final BlockFactory blockFactory;
    private final StarTreeReader reader;
    private final List<String> groupByFields;
    private final List<ValueRequest> valueRequests;
    private final int maxPageSize;

    private boolean finished = false;
    private StarTreeReader.StarTreeTraverser traverser;

    /**
     * Request for a metric aggregation.
     */
    public record ValueRequest(String field, StarTreeAggregationType type, int outputIndex) {}

    public StarTreeSourceOperator(
        BlockFactory blockFactory,
        StarTreeReader reader,
        List<String> groupByFields,
        List<ValueRequest> valueRequests,
        int maxPageSize
    ) {
        this.blockFactory = blockFactory;
        this.reader = reader;
        this.groupByFields = groupByFields;
        this.valueRequests = valueRequests;
        this.maxPageSize = maxPageSize;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public Page getOutput() {
        if (finished) {
            return null;
        }

        try {
            return getOutputInternal();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read star-tree", e);
        }
    }

    private Page getOutputInternal() throws IOException {
        if (traverser == null) {
            traverser = reader.createTraverser();
        }

        if (traverser.hasNode() == false) {
            finished = true;
            return null;
        }

        // For now, implement a simple case: no GROUP BY dimensions.
        // We return a single row with the root node's aggregated values.
        if (groupByFields.isEmpty()) {
            return getAggregatedResults(traverser.current());
        }

        // TODO: Implement GROUP BY handling by traversing child nodes
        // For grouped queries, we need to:
        // 1. Traverse to the appropriate level based on GROUP BY dimensions
        // 2. For each unique combination of dimension values, collect aggregated metrics
        // 3. Build blocks with dimension values + metric values

        finished = true;
        return null;
    }

    /**
     * Get aggregated results from a single node (no GROUP BY case).
     */
    private Page getAggregatedResults(StarTreeNode node) {
        int numOutputs = valueRequests.size();
        Block[] blocks = new Block[numOutputs];

        try {
            for (int i = 0; i < valueRequests.size(); i++) {
                ValueRequest request = valueRequests.get(i);
                int metricIndex = reader.getValueIndex(request.field);

                if (metricIndex < 0) {
                    // Metric not found - create null block
                    blocks[i] = blockFactory.newConstantNullBlock(1);
                    continue;
                }

                double value;
                if (request.type == StarTreeAggregationType.COUNT) {
                    // COUNT returns the document count
                    value = node.getDocCount();
                } else {
                    value = node.getAggregatedValue(metricIndex, request.type);
                }

                // Create a double block with the aggregated value
                try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(1)) {
                    builder.appendDouble(value);
                    blocks[i] = builder.build();
                }
            }

            finished = true;
            return new Page(1, blocks);
        } catch (Exception e) {
            // Clean up any created blocks on failure
            for (Block block : blocks) {
                if (block != null) {
                    block.close();
                }
            }
            throw e;
        }
    }

    @Override
    public void close() {
        // Reader lifecycle is managed by the caller
    }

    /**
     * Factory for creating StarTreeSourceOperator instances.
     */
    public static class Factory implements SourceOperator.SourceOperatorFactory {

        private final StarTreeReader reader;
        private final List<String> groupByFields;
        private final List<ValueRequest> valueRequests;
        private final int maxPageSize;

        public Factory(
            StarTreeReader reader,
            List<String> groupByFields,
            List<ValueRequest> valueRequests,
            int maxPageSize
        ) {
            this.reader = reader;
            this.groupByFields = groupByFields;
            this.valueRequests = valueRequests;
            this.maxPageSize = maxPageSize;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new StarTreeSourceOperator(
                driverContext.blockFactory(),
                reader,
                groupByFields,
                valueRequests,
                maxPageSize
            );
        }

        @Override
        public String describe() {
            return "StarTreeSourceOperator[groupBy=" + groupByFields + ", metrics=" + valueRequests + "]";
        }
    }
}
