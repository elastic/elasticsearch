/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.swisshash.SwissHashFactory;

/**
 * An extension of {@link BlockHash} to support partitioning.
 * This class should be removed once all {@link BlockHash} supports partitioning.
 */
public abstract class PartitionedBlockHash extends BlockHash implements PartitionedHashTable {
    protected PartitionedBlockHash(BlockFactory blockFactory) {
        super(blockFactory);
    }

    public static boolean supportPartitioning() {
        return SwissHashFactory.getInstance() != null;
    }

    public abstract void clear();
}
