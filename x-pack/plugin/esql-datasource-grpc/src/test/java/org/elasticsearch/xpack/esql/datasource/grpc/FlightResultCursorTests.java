/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class FlightResultCursorTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    public void testToBlockInt() {
        try (BufferAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                IntVector vec = (IntVector) root.getVector("val");
                vec.allocateNew(3);
                vec.setSafe(0, 10);
                vec.setSafe(1, 20);
                vec.setSafe(2, 30);
                root.setRowCount(3);

                Block block = FlightTypeMapping.toBlock(vec, 3, blockFactory);
                assertThat(block, instanceOf(IntBlock.class));
                IntBlock intBlock = (IntBlock) block;
                assertEquals(10, intBlock.getInt(0));
                assertEquals(20, intBlock.getInt(1));
                assertEquals(30, intBlock.getInt(2));
                block.close();
            }
        }
    }

    public void testToBlockDouble() {
        try (BufferAllocator allocator = new RootAllocator()) {
            var fpType = new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(fpType), null)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                Float8Vector vec = (Float8Vector) root.getVector("val");
                vec.allocateNew(2);
                vec.setSafe(0, 1.5);
                vec.setSafe(1, 2.7);
                root.setRowCount(2);

                Block block = FlightTypeMapping.toBlock(vec, 2, blockFactory);
                assertThat(block, instanceOf(DoubleBlock.class));
                DoubleBlock doubleBlock = (DoubleBlock) block;
                assertEquals(1.5, doubleBlock.getDouble(0), 0.001);
                assertEquals(2.7, doubleBlock.getDouble(1), 0.001);
                block.close();
            }
        }
    }

    public void testToBlockUtf8() {
        try (BufferAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Utf8()), null)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                VarCharVector vec = (VarCharVector) root.getVector("val");
                vec.allocateNew(2);
                vec.setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
                vec.setSafe(1, "world".getBytes(StandardCharsets.UTF_8));
                root.setRowCount(2);

                Block block = FlightTypeMapping.toBlock(vec, 2, blockFactory);
                assertThat(block, instanceOf(BytesRefBlock.class));
                BytesRefBlock bytesBlock = (BytesRefBlock) block;
                assertEquals(new BytesRef("hello"), bytesBlock.getBytesRef(0, new BytesRef()));
                assertEquals(new BytesRef("world"), bytesBlock.getBytesRef(1, new BytesRef()));
                block.close();
            }
        }
    }

    public void testToBlockBoolean() {
        try (BufferAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Bool()), null)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                BitVector vec = (BitVector) root.getVector("val");
                vec.allocateNew(3);
                vec.setSafe(0, 1);
                vec.setSafe(1, 0);
                vec.setSafe(2, 1);
                root.setRowCount(3);

                Block block = FlightTypeMapping.toBlock(vec, 3, blockFactory);
                assertThat(block, instanceOf(BooleanBlock.class));
                BooleanBlock boolBlock = (BooleanBlock) block;
                assertTrue(boolBlock.getBoolean(0));
                assertFalse(boolBlock.getBoolean(1));
                assertTrue(boolBlock.getBoolean(2));
                block.close();
            }
        }
    }

    public void testToBlockWithNulls() {
        try (BufferAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                IntVector vec = (IntVector) root.getVector("val");
                vec.allocateNew(3);
                vec.setSafe(0, 42);
                vec.setNull(1);
                vec.setSafe(2, 99);
                root.setRowCount(3);

                Block block = FlightTypeMapping.toBlock(vec, 3, blockFactory);
                assertThat(block, instanceOf(IntBlock.class));
                IntBlock intBlock = (IntBlock) block;
                assertEquals(42, intBlock.getInt(0));
                assertTrue(intBlock.isNull(1));
                assertEquals(99, intBlock.getInt(2));
                block.close();
            }
        }
    }

    public void testToAttributesMapsCorrectTypes() {
        List<Attribute> attrs = FlightTypeMapping.toAttributes(EmployeeFlightServer.SCHEMA);
        assertEquals(6, attrs.size());
        assertEquals(DataType.INTEGER, attrs.get(0).dataType());
        assertEquals(DataType.KEYWORD, attrs.get(1).dataType());
        assertEquals(DataType.KEYWORD, attrs.get(2).dataType());
        assertEquals(DataType.INTEGER, attrs.get(3).dataType());
        assertEquals(DataType.BOOLEAN, attrs.get(4).dataType());
        assertEquals(DataType.DOUBLE, attrs.get(5).dataType());
    }
}
