/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.session.EsqlConfigurationSerializationTests;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class PlanStreamOutputTests extends ESTestCase {

    public void testTransportVersion() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        TransportVersion v1 = TransportVersionUtils.randomCompatibleVersion(random());
        out.setTransportVersion(v1);
        PlanStreamOutput planOut = new PlanStreamOutput(
            out,
            PlanNameRegistry.INSTANCE,
            randomBoolean() ? null : EsqlConfigurationSerializationTests.randomConfiguration()
        );
        assertThat(planOut.getTransportVersion(), equalTo(v1));
        TransportVersion v2 = TransportVersionUtils.randomCompatibleVersion(random());
        planOut.setTransportVersion(v2);
        assertThat(planOut.getTransportVersion(), equalTo(v2));
        assertThat(out.getTransportVersion(), equalTo(v2));
    }

    public void testWriteBlockFromConfig() throws IOException {
        String tableName = randomAlphaOfLength(5);
        String columnName = randomAlphaOfLength(10);
        try (Column c = randomColumn()) {
            EsqlConfiguration configuration = randomConfiguration(Map.of(tableName, Map.of(columnName, c)));
            try (
                BytesStreamOutput out = new BytesStreamOutput();
                PlanStreamOutput planStream = new PlanStreamOutput(out, PlanNameRegistry.INSTANCE, configuration)
            ) {
                planStream.writeCachedBlock(c.values());
                assertThat(out.bytes().length(), equalTo(3 + tableName.length() + columnName.length()));
                try (
                    PlanStreamInput in = new PlanStreamInput(out.bytes().streamInput(), PlanNameRegistry.INSTANCE, REGISTRY, configuration)
                ) {
                    assertThat(in.readCachedBlock(), sameInstance(c.values()));
                }
            }
        }
    }

    public void testWriteBlockOnce() throws IOException {
        try (Block b = randomColumn().values()) {
            EsqlConfiguration configuration = EsqlConfigurationSerializationTests.randomConfiguration();
            try (
                BytesStreamOutput out = new BytesStreamOutput();
                PlanStreamOutput planStream = new PlanStreamOutput(out, PlanNameRegistry.INSTANCE, configuration)
            ) {
                planStream.writeCachedBlock(b);
                assertThat(out.bytes().length(), greaterThan(4 * LEN));
                assertThat(out.bytes().length(), lessThan(8 * LEN));
                try (
                    PlanStreamInput in = new PlanStreamInput(out.bytes().streamInput(), PlanNameRegistry.INSTANCE, REGISTRY, configuration)
                ) {
                    Block read = in.readCachedBlock();
                    assertThat(read, not(sameInstance(b)));
                    assertThat(read, equalTo(b));
                }
            }
        }
    }

    public void testWriteBlockTwice() throws IOException {
        try (Block b = randomColumn().values()) {
            EsqlConfiguration configuration = EsqlConfigurationSerializationTests.randomConfiguration();
            try (
                BytesStreamOutput out = new BytesStreamOutput();
                PlanStreamOutput planStream = new PlanStreamOutput(out, PlanNameRegistry.INSTANCE, configuration)
            ) {
                planStream.writeCachedBlock(b);
                planStream.writeCachedBlock(b);
                assertThat(out.bytes().length(), greaterThan(4 * LEN));
                assertThat(out.bytes().length(), lessThan(8 * LEN));
                try (
                    PlanStreamInput in = new PlanStreamInput(out.bytes().streamInput(), PlanNameRegistry.INSTANCE, REGISTRY, configuration)
                ) {
                    Block read = in.readCachedBlock();
                    assertThat(read, not(sameInstance(b)));
                    assertThat(read, equalTo(b));
                    assertThat(in.readCachedBlock(), sameInstance(read));
                }
            }
        }
    }

    private EsqlConfiguration randomConfiguration(Map<String, Map<String, Column>> tables) {
        return EsqlConfigurationSerializationTests.randomConfiguration("query_" + randomAlphaOfLength(1), tables);
    }

    private static final int LEN = 10000;

    private Column randomColumn() {
        try (IntBlock.Builder ints = BLOCK_FACTORY.newIntBlockBuilder(LEN)) {
            for (int i = 0; i < LEN; i++) {
                ints.appendInt(randomInt());
            }
            return new Column(DataTypes.INTEGER, ints.build());
        }
    }

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop-esql-breaker"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(Block.getNamedWriteables());
}
