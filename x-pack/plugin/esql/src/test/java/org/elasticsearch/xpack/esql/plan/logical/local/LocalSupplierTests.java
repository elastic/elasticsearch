/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockWritables;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.Arrays;

public class LocalSupplierTests extends AbstractWireTestCase<LocalSupplier> {
    private static final BlockFactory BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop-esql-breaker"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    @Override
    protected LocalSupplier copyInstance(LocalSupplier instance, TransportVersion version) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(version);
            instance.writeTo(new PlanStreamOutput(output, null));
            try (StreamInput in = output.bytes().streamInput()) {
                in.setTransportVersion(version);
                return LocalSupplier.readFrom(new PlanStreamInput(in, getNamedWriteableRegistry(), null));
            }
        }
    }

    @Override
    protected LocalSupplier createTestInstance() {
        return randomBoolean() ? LocalSupplier.EMPTY : randomNonEmpty();
    }

    public static LocalSupplier randomNonEmpty() {
        return LocalSupplier.of(randomList(1, 10, LocalSupplierTests::randomBlock).toArray(Block[]::new));
    }

    @Override
    protected LocalSupplier mutateInstance(LocalSupplier instance) throws IOException {
        Block[] blocks = instance.get();
        if (blocks.length > 0 && randomBoolean()) {
            if (randomBoolean()) {
                return LocalSupplier.EMPTY;
            }
            return LocalSupplier.of(Arrays.copyOf(blocks, blocks.length - 1, Block[].class));
        }
        blocks = Arrays.copyOf(blocks, blocks.length + 1, Block[].class);
        blocks[blocks.length - 1] = randomBlock();
        return LocalSupplier.of(blocks);
    }

    private static Block randomBlock() {
        int len = between(1, 1000);
        try (IntBlock.Builder ints = BLOCK_FACTORY.newIntBlockBuilder(len)) {
            for (int i = 0; i < len; i++) {
                ints.appendInt(randomInt());
            }
            return ints.build();
        }
    }

    @Override
    protected boolean shouldBeSame(LocalSupplier newInstance) {
        return newInstance.get().length == 0;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(BlockWritables.getNamedWriteables());
    }
}
