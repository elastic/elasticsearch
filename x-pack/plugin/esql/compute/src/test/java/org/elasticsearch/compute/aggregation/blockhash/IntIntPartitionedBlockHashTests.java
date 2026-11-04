/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;

import java.util.List;

/**
 * Partitioned aggregation over {@link IntIntBlockHash}, which only takes groups that the planner proved to have no null.
 */
public class IntIntPartitionedBlockHashTests extends PartitionedBlockHashTestCase {
    @Override
    protected List<ElementType> keyTypes() {
        return List.of(ElementType.INT, ElementType.INT);
    }

    @Override
    protected List<BlockHash.GroupSpec> groups() {
        return super.groups().stream().map(g -> g.withNonNull(true)).toList();
    }

    @Override
    protected boolean nullsAllowed() {
        return false;
    }

    @Override
    protected PartitionedBlockHash newBlockHash(List<BlockHash.GroupSpec> groups, BlockFactory blockFactory, int emitBatchSize) {
        return new IntIntBlockHash(blockFactory, groups.get(0).channel(), groups.get(1).channel(), emitBatchSize);
    }
}
