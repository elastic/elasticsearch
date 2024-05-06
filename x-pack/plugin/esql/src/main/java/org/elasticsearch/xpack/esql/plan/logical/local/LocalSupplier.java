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
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

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
    };

    static LocalSupplier of(Block[] blocks) {
        return new LocalSupplier() {
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
                out.writeArray((o, v) -> ((PlanStreamOutput) o).writeBlock(v), blocks);
            }
        };
    }

    static LocalSupplier readFrom(PlanStreamInput in) throws IOException {
        int count = in.readArraySize();
        if (count == 0) {
            return EMPTY;
        }
        Block[] blocks = new Block[count];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = in.readBlock();
        }
        return of(blocks);
    }
}
