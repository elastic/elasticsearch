/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.PlanWritables;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;

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
            if (version.onOrAfter(TransportVersions.ESQL_LOCAL_RELATION_WITH_NEW_BLOCKS)) {
                new PlanStreamOutput(output, null).writeNamedWriteable(instance);
            } else {
                instance.writeTo(new PlanStreamOutput(output, null));
            }
            try (StreamInput in = output.bytes().streamInput()) {
                in.setTransportVersion(version);
                if (version.onOrAfter(TransportVersions.ESQL_LOCAL_RELATION_WITH_NEW_BLOCKS)) {
                    return new PlanStreamInput(in, getNamedWriteableRegistry(), null).readNamedWriteable(LocalSupplier.class);
                } else {
                    return LocalSourceExec.readLegacyLocalSupplierFrom(new PlanStreamInput(in, getNamedWriteableRegistry(), null));
                }
            }
        }
    }

    @Override
    protected LocalSupplier createTestInstance() {
        return randomLocalSupplier();
    }

    public static LocalSupplier randomLocalSupplier() {
        return randomBoolean() ? EmptyLocalSupplier.EMPTY : randomNonEmpty();
    }

    public static LocalSupplier randomNonEmpty() {
        Block[] blocks = randomList(1, 10, LocalSupplierTests::randomBlock).toArray(Block[]::new);
        return randomBoolean() ? LocalSupplier.of(blocks) : new CopyingLocalSupplier(blocks);
    }

    @Override
    protected LocalSupplier mutateInstance(LocalSupplier instance) throws IOException {
        Block[] blocks = instance.get();
        if (blocks.length > 0 && randomBoolean()) {
            if (randomBoolean()) {
                return EmptyLocalSupplier.EMPTY;
            }
            return LocalSupplier.of(Arrays.copyOf(blocks, blocks.length - 1, Block[].class));
        }
        blocks = Arrays.copyOf(blocks, blocks.length + 1, Block[].class);
        blocks[blocks.length - 1] = randomBlock();
        return LocalSupplier.of(blocks);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(PlanWritables.others());
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
}
