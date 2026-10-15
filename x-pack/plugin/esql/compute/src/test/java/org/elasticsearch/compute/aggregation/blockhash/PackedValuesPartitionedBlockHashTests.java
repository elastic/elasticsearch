/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeTests;

import java.util.List;

public class PackedValuesPartitionedBlockHashTests extends PartitionedBlockHashTestCase {
    @Override
    protected List<ElementType> keyTypes() {
        List<ElementType> supported = MultivalueDedupeTests.supportedTypes().stream().filter(t -> t != ElementType.NULL).toList();
        return randomList(1, 5, () -> randomFrom(supported));
    }

    @Override
    protected PartitionedBlockHash newBlockHash(List<BlockHash.GroupSpec> groups, BlockFactory blockFactory, int emitBatchSize) {
        return new PackedValuesBlockHash(groups, blockFactory, emitBatchSize);
    }
}
