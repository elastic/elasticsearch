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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.PlanWritables;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;

import java.io.IOException;
import java.util.Arrays;
import java.util.NavigableSet;

public abstract class LocalSupplierTests extends AbstractWireTestCase<LocalSupplier> {

    private static final NavigableSet<TransportVersion> DEFAULT_BWC_VERSIONS = getAllBWCVersions();
    private static final TransportVersion ESQL_LOCAL_RELATION_WITH_NEW_BLOCKS = TransportVersion.fromName(
        "esql_local_relation_with_new_blocks"
    );

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop-esql-breaker"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    private static NavigableSet<TransportVersion> getAllBWCVersions() {
        return TransportVersionUtils.allReleasedVersions().tailSet(TransportVersion.minimumCompatible(), true);
    }

    public final void testBwcSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            LocalSupplier testInstance = createTestInstance();
            for (TransportVersion bwcVersion : DEFAULT_BWC_VERSIONS) {
                assertBwcSerialization(testInstance, bwcVersion);
            }
        }
    }

    protected final void assertBwcSerialization(LocalSupplier testInstance, TransportVersion version) throws IOException {
        LocalSupplier deserializedInstance = copyInstance(testInstance, version);
        assertOnBWCObject(testInstance, deserializedInstance, version);
    }

    protected abstract void assertOnBWCObject(LocalSupplier testInstance, LocalSupplier bwcDeserializedObject, TransportVersion version);

    @Override
    protected LocalSupplier copyInstance(LocalSupplier instance, TransportVersion version) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(version);
            writeTo(output, instance, version);
            try (StreamInput in = output.bytes().streamInput()) {
                in.setTransportVersion(version);
                return readFrom(in, version);
            }
        }
    }

    protected void writeTo(BytesStreamOutput output, LocalSupplier instance, TransportVersion version) throws IOException {
        if (version.supports(ESQL_LOCAL_RELATION_WITH_NEW_BLOCKS)) {
            new PlanStreamOutput(output, null).writeNamedWriteable(instance);
        } else {
            instance.writeTo(new PlanStreamOutput(output, null));
        }
    }

    protected LocalSupplier readFrom(StreamInput input, TransportVersion version) throws IOException {
        if (version.supports(ESQL_LOCAL_RELATION_WITH_NEW_BLOCKS)) {
            return new PlanStreamInput(input, getNamedWriteableRegistry(), null).readNamedWriteable(LocalSupplier.class);
        } else {
            return LocalSourceExec.readLegacyLocalSupplierFrom(new PlanStreamInput(input, getNamedWriteableRegistry(), null));
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

    static Block randomBlock() {
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
