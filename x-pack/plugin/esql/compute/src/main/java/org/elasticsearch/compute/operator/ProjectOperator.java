/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.Arrays;
import java.util.BitSet;

@Experimental
public class ProjectOperator implements Operator {

    private final BitSet bs;
    private Block[] blocks;

    private Page lastInput;
    boolean finished = false;

    public record ProjectOperatorFactory(BitSet mask) implements OperatorFactory {

        @Override
        public Operator get() {
            return new ProjectOperator(mask);
        }

        @Override
        public String describe() {
            return "ProjectOperator(mask = " + mask + ")";
        }
    }

    /**
     * Creates a project that applies the given mask (as a bitset).
     *
     * @param mask bitset mask for enabling/disabling blocks / columns inside a Page
     */
    public ProjectOperator(BitSet mask) {
        this.bs = mask;
    }

    @Override
    public boolean needsInput() {
        return lastInput == null && finished == false;
    }

    @Override
    public void addInput(Page page) {
        lastInput = page;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return lastInput == null && finished;
    }

    @Override
    public Page getOutput() {
        if (lastInput == null) {
            return null;
        }
        if (blocks == null) {
            blocks = new Block[bs.cardinality()];
        }

        Arrays.fill(blocks, null);
        int b = 0;
        int positionCount = lastInput.getPositionCount();
        for (int i = bs.nextSetBit(0); i >= 0 && i < lastInput.getBlockCount(); i = bs.nextSetBit(i + 1)) {
            var block = lastInput.getBlock(i);
            blocks[b++] = block;
        }
        lastInput = null;
        return new Page(positionCount, blocks);
    }

    @Override
    public void close() {}
}
