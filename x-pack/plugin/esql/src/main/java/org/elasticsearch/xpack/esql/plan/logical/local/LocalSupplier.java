/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Supplies fixed {@link Block}s for things calculated at plan time.
 * <p>
 *     This is {@link Writeable} so we can model {@code LOOKUP} and
 *     hash joins which have to go over the wire. But many implementers
 *     don't have to go over the wire and they should feel free to throw
 *     {@link UnsupportedOperationException}.
 * </p>
 */
public interface LocalSupplier extends Supplier<Block[]>, Writeable {

    LocalSupplier EMPTY = new LocalSupplier() {
        @Override
        public Block[] get() {
            return BlockUtils.NO_BLOCKS;
        }

        @Override
        public String toString() {
            return "EMPTY";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(0);
        }

        @Override
        public boolean equals(Object obj) {
            return obj == EMPTY;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    };

    static LocalSupplier of(Block[] blocks) {
        return new ImmediateLocalSupplier(blocks);
    }

    static LocalSupplier readFrom(PlanStreamInput in) throws IOException {
        Block[] blocks = in.readCachedBlockArray();
        return blocks.length == 0 ? EMPTY : of(blocks);
    }
}
