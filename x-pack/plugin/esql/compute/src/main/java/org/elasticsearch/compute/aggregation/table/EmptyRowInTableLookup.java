/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;

/**
 * {@link RowInTableLookup} for an empty table.
 */
public final class EmptyRowInTableLookup extends RowInTableLookup {
    private final BlockFactory blockFactory;

    public EmptyRowInTableLookup(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        return ReleasableIterator.single((IntBlock) blockFactory.newConstantNullBlock(page.getPositionCount()));
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "EmptyLookup";
    }
}
