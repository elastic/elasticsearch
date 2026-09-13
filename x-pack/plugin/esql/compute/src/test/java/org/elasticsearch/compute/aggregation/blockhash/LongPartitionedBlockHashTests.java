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

public class LongPartitionedBlockHashTests extends PartitionedBlockHashTestCase {
    @Override
    protected List<ElementType> keyTypes() {
        return List.of(ElementType.LONG);
    }

    @Override
    protected PartitionedBlockHash newBlockHash(List<BlockHash.GroupSpec> groups, BlockFactory blockFactory, int emitBatchSize) {
        return new LongBlockHash(groups.get(0).channel(), blockFactory);
    }
}
