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

public class LongIntPartitionedBlockHashTests extends PartitionedBlockHashTestCase {
    @Override
    protected List<ElementType> keyTypes() {
        return randomBoolean() ? List.of(ElementType.LONG, ElementType.INT) : List.of(ElementType.INT, ElementType.LONG);
    }

    @Override
    protected PartitionedBlockHash newBlockHash(List<BlockHash.GroupSpec> groups, BlockFactory blockFactory, int emitBatchSize) {
        boolean reverseOutput = groups.get(0).elementType() == ElementType.INT;
        return new LongIntBlockHash(groups, blockFactory, emitBatchSize, reverseOutput);
    }
}
