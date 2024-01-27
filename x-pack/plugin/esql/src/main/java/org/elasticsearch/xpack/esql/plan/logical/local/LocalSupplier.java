/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;

import java.util.Arrays;
import java.util.function.Supplier;

public interface LocalSupplier extends Supplier<Block[]> {

    LocalSupplier EMPTY = new LocalSupplier() {
        @Override
        public Block[] get() {
            return BlockUtils.NO_BLOCKS;
        }

        @Override
        public String toString() {
            return "EMPTY";
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
        };
    }
}
