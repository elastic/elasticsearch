/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * A {@link LocalSupplier} that contains already filled {@link Block}s.
 */
public class ImmediateLocalSupplier implements LocalSupplier {
    private final Block[] blocks;

    public ImmediateLocalSupplier(Block[] blocks) {
        this.blocks = blocks;
    }

    @Override
    public Block[] get() {
        return blocks;
    }

    @Override
    public String toString() {
        return Arrays.toString(blocks);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray((o, v) -> ((PlanStreamOutput) o).writeCachedBlock(v), blocks);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ImmediateLocalSupplier other = (ImmediateLocalSupplier) obj;
        return Arrays.equals(blocks, other.blocks);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(blocks);
    }
}
