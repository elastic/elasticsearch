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
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.ReleasableIterator;

/**
 * {@link RowInTableLookup} for an empty table.
 */
public final class EmptyRowInTableLookup extends RowInTableLookup {
    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, BlockFactory blockFactory, ByteSizeValue targetBlockSize) {
        return ReleasableIterator.single((IntBlock) blockFactory.newConstantNullBlock(page.getPositionCount()));
    }

    /**
     * Warns on multivalued keys like a non-empty repeated-key table does, so an empty build doesn't hide the warning.
     */
    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, BlockFactory blockFactory, ByteSizeValue targetBlockSize, Warnings warnings) {
        warnOnMultivaluedKeys(page, warnings);
        return lookup(page, blockFactory, targetBlockSize);
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "EmptyLookup";
    }
}
