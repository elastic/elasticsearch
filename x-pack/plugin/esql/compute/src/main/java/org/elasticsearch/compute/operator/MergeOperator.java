/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.List;
import java.util.ListIterator;
import java.util.function.Supplier;

/**
 * A merge operator is effectively a "fan-in" operator - accepts input
 * from several sources and provides it in a single output.
 */
public class MergeOperator extends SourceOperator {

    private final BlockFactory blockFactory;
    private boolean finished;
    private ListIterator<Block[]> subPlanBlocks;

    public record MergeOperatorFactory(BlockSuppliers suppliers) implements SourceOperatorFactory {
        @Override
        public String describe() {
            return "MergeOperator[suppliers=" + suppliers + "]";
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new MergeOperator(driverContext.blockFactory(), suppliers);
        }
    }

    private final BlockSuppliers suppliers;

    public MergeOperator(BlockFactory blockFactory, BlockSuppliers suppliers) {
        super();
        this.blockFactory = blockFactory;
        this.suppliers = suppliers;
        this.finished = false;
        this.subPlanBlocks = null;
    }

    public interface BlockSuppliers extends Supplier<List<Block[]>> {};

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && subPlanBlocks != null && subPlanBlocks.hasNext() == false;
    }

    @Override
    public Page getOutput() {
        if (subPlanBlocks == null) {
            subPlanBlocks = suppliers.get().listIterator();
        }

        Page page = null;

        if (subPlanBlocks.hasNext()) {
            page = new Page(subPlanBlocks.next());
        }
        if (page == null) {
            finished = true;
        }

        return page;
    }

    @Override
    public void close() {
        // release blocks from any subplan not fully consumed.
        if (subPlanBlocks != null) {
            while (subPlanBlocks.hasNext()) {
                Releasables.close(subPlanBlocks.next());
            }
        }
    }

    @Override
    public String toString() {
        return "MergeOperator[subPlanBlocks=" + subPlanBlocks + "]";
    }
}
