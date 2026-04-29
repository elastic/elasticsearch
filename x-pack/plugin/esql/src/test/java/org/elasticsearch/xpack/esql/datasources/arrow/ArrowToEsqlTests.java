/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ArrowToEsqlTests extends ESTestCase {

    private RootAllocator allocator;
    private BlockFactory blockFactory;

    @Before
    public void setup() {
        allocator = new RootAllocator();
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    @After
    public void cleanup() {
        allocator.close();
    }

    /**
     * Regression test: forField on a LIST field used to call getChildren().get(1), which throws
     * IndexOutOfBoundsException because a ListVector field has exactly one child (the element type)
     * at index 0. Uses BIT to also exercise the supported-child path.
     */
    public void testForFieldOnListDoesNotThrow() {
        Field elementField = new Field("element", FieldType.nullable(Types.MinorType.BIT.getType()), null);
        Field listField = new Field("myList", FieldType.nullable(new ArrowType.List()), List.of(elementField));

        ArrowToEsql mapping = ArrowToEsql.forField(listField);

        assertNotNull(mapping);
        assertEquals(DataType.BOOLEAN, mapping.dataType());
    }

    /**
     * Nested lists (LIST&lt;LIST&lt;X&gt;&gt;) are rejected at schema time: ESQL has no nested array type,
     * and the LIST converter rejects LIST as a child type. See {@code ArrowToBlockConverter#isListChildTypeSupported}.
     */
    public void testForFieldOnNestedListReturnsNull() {
        Field innerElement = new Field("element", FieldType.nullable(Types.MinorType.BIT.getType()), null);
        Field innerList = new Field("inner", FieldType.nullable(new ArrowType.List()), List.of(innerElement));
        Field outerList = new Field("outer", FieldType.nullable(new ArrowType.List()), List.of(innerList));

        assertNull(ArrowToEsql.forField(outerList));
    }

    /**
     * LIST&lt;BIGINT&gt; is rejected at schema time even though BIGINT is a supported flat type, because
     * the LIST converter doesn't yet know how to handle BIGINT children safely (zero-copy path
     * crashes on nulls; copy fallback is a follow-up). LIST&lt;FLOAT4&gt; is rejected for the same
     * reason now that FLOAT4 routes through the zero-copy {@code Float32ArrowBufBlock}.
     */
    public void testForFieldOnListOfBigIntReturnsNull() {
        Field elementField = new Field("element", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        Field listField = new Field("myList", FieldType.nullable(new ArrowType.List()), List.of(elementField));

        assertNull(ArrowToEsql.forField(listField));
    }

    public void testForFieldOnListOfFloat32ReturnsNull() {
        Field elementField = new Field("element", FieldType.nullable(Types.MinorType.FLOAT4.getType()), null);
        Field listField = new Field("myList", FieldType.nullable(new ArrowType.List()), List.of(elementField));

        assertNull(ArrowToEsql.forField(listField));
    }

    public void testForFieldOnUnsupportedTypeReturnsNull() {
        Field field = new Field("f", FieldType.nullable(Types.MinorType.STRUCT.getType()), null);
        assertNull(ArrowToEsql.forField(field));
    }

    /**
     * Flat FLOAT4 must produce a DoubleBlock through the {@code forField} path, because
     * {@code DataType.FLOAT} is non-representable: ESQL widens it to DOUBLE before any block is
     * touched at execution time. A {@code FloatBlock} would violate the {@code ElementType.DOUBLE}
     * field on the returned {@code ArrowToEsql}.
     */
    public void testForFieldOnFloat32ConvertsToDoubleBlock() {
        Field field = new Field("f", FieldType.nullable(Types.MinorType.FLOAT4.getType()), null);
        ArrowToEsql mapping = ArrowToEsql.forField(field);
        assertNotNull(mapping);
        assertEquals(DataType.FLOAT, mapping.dataType());

        try (Float4Vector vector = new Float4Vector("f", allocator)) {
            vector.allocateNew(2);
            vector.set(0, 1.5f);
            vector.set(1, -3.0f);
            vector.setValueCount(2);

            try (Block block = mapping.convert(vector, blockFactory)) {
                assertTrue(block instanceof DoubleBlock);
                DoubleBlock db = (DoubleBlock) block;
                assertEquals(2, db.getPositionCount());
                assertEquals(1.5, db.getDouble(0), 0.0);
                assertEquals(-3.0, db.getDouble(1), 0.0);
            }
        }
    }

    /**
     * Regression test for a {@link ClassCastException} on the {@link ArrowToEsql#forField} path for
     * LIST&lt;BIT&gt;: forField used to return the leaf BIT mapping whose converter unconditionally
     * cast its input to {@code BitVector}, so calling {@code mapping.convert(listVector, ...)} crashed.
     * forField now wires LIST conversion through the LIST entry of the runtime registry, which
     * dispatches to a list-aware copy converter.
     */
    public void testForFieldOnListOfBitConvertsToBooleanBlock() {
        Field elementField = new Field("element", FieldType.nullable(Types.MinorType.BIT.getType()), null);
        Field listField = new Field("myList", FieldType.nullable(new ArrowType.List()), List.of(elementField));

        ArrowToEsql mapping = ArrowToEsql.forField(listField);
        assertNotNull(mapping);
        assertEquals(DataType.BOOLEAN, mapping.dataType());

        try (ListVector listVector = buildBitListVector()) {
            try (Block block = mapping.convert(listVector, blockFactory)) {
                assertTrue(block instanceof BooleanBlock);
                BooleanBlock bb = (BooleanBlock) block;
                assertEquals(2, bb.getPositionCount());
                assertEquals(2, bb.getValueCount(0));
                int p0 = bb.getFirstValueIndex(0);
                assertTrue(bb.getBoolean(p0));
                assertFalse(bb.getBoolean(p0 + 1));
                assertTrue(bb.isNull(1));
            }
        }
    }

    /**
     * Companion of {@link #testForFieldOnListOfBitConvertsToBooleanBlock} for a variable-width child:
     * confirms that LIST&lt;VARCHAR&gt; reaches the list-aware {@code BytesRefArrowBlock} converter
     * via {@code forField}.
     */
    public void testForFieldOnListOfVarCharConvertsToBytesRefBlock() {
        Field elementField = new Field("element", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        Field listField = new Field("myList", FieldType.nullable(new ArrowType.List()), List.of(elementField));

        ArrowToEsql mapping = ArrowToEsql.forField(listField);
        assertNotNull(mapping);
        assertEquals(DataType.KEYWORD, mapping.dataType());

        try (ListVector listVector = buildVarCharListVector()) {
            try (Block block = mapping.convert(listVector, blockFactory)) {
                assertTrue(block instanceof BytesRefBlock);
                BytesRefBlock bb = (BytesRefBlock) block;
                assertEquals(2, bb.getPositionCount());
                assertEquals(2, bb.getValueCount(0));
                int p0 = bb.getFirstValueIndex(0);
                assertEquals(new BytesRef("hello"), bb.getBytesRef(p0, new BytesRef()));
                assertEquals(new BytesRef("world"), bb.getBytesRef(p0 + 1, new BytesRef()));
                assertTrue(bb.isNull(1));
            }
        }
    }

    /**
     * Builds a {@link ListVector} with {@link BitVector} children laid out as:
     * <pre>
     *   pos 0: [true, false]
     *   pos 1: null list
     * </pre>
     */
    private ListVector buildBitListVector() {
        ListVector listVector = ListVector.empty("myList", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.BIT.getType()));
        listVector.allocateNew();

        BitVector child = (BitVector) listVector.getDataVector();
        child.allocateNew(2);
        child.set(0, 1);
        child.set(1, 0);
        child.setValueCount(2);

        ArrowBuf offsetBuf = listVector.getOffsetBuffer();
        offsetBuf.setInt(0, 0);
        offsetBuf.setInt(4, 2);
        offsetBuf.setInt(8, 2);

        ArrowBuf validityBuf = listVector.getValidityBuffer();
        validityBuf.setZero(0, validityBuf.capacity());
        BitVectorHelper.setBit(validityBuf, 0);

        listVector.setLastSet(1);
        listVector.setValueCount(2);
        return listVector;
    }

    /**
     * Builds a {@link ListVector} with {@link VarCharVector} children laid out as:
     * <pre>
     *   pos 0: ["hello", "world"]
     *   pos 1: null list
     * </pre>
     */
    private ListVector buildVarCharListVector() {
        ListVector listVector = ListVector.empty("myList", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.VARCHAR.getType()));
        listVector.allocateNew();

        VarCharVector child = (VarCharVector) listVector.getDataVector();
        child.allocateNew();
        child.set(0, "hello".getBytes(StandardCharsets.UTF_8));
        child.set(1, "world".getBytes(StandardCharsets.UTF_8));
        child.setValueCount(2);

        ArrowBuf offsetBuf = listVector.getOffsetBuffer();
        offsetBuf.setInt(0, 0);
        offsetBuf.setInt(4, 2);
        offsetBuf.setInt(8, 2);

        ArrowBuf validityBuf = listVector.getValidityBuffer();
        validityBuf.setZero(0, validityBuf.capacity());
        BitVectorHelper.setBit(validityBuf, 0);

        listVector.setLastSet(1);
        listVector.setValueCount(2);
        return listVector;
    }

    public void testDataTypeForFieldFlat() {
        assertEquals(DataType.DOUBLE, ArrowToEsql.dataTypeForField(field(Types.MinorType.FLOAT4)));
        assertEquals(DataType.DOUBLE, ArrowToEsql.dataTypeForField(field(Types.MinorType.FLOAT8)));
        assertEquals(DataType.INTEGER, ArrowToEsql.dataTypeForField(field(Types.MinorType.TINYINT)));
        assertEquals(DataType.INTEGER, ArrowToEsql.dataTypeForField(field(Types.MinorType.SMALLINT)));
        assertEquals(DataType.INTEGER, ArrowToEsql.dataTypeForField(field(Types.MinorType.INT)));
        assertEquals(DataType.LONG, ArrowToEsql.dataTypeForField(field(Types.MinorType.BIGINT)));
        assertEquals(DataType.BOOLEAN, ArrowToEsql.dataTypeForField(field(Types.MinorType.BIT)));
        assertEquals(DataType.KEYWORD, ArrowToEsql.dataTypeForField(field(Types.MinorType.VARCHAR)));
        assertEquals(DataType.KEYWORD, ArrowToEsql.dataTypeForField(field(Types.MinorType.VARBINARY)));
        assertEquals(DataType.UNSUPPORTED, ArrowToEsql.dataTypeForField(field(Types.MinorType.STRUCT)));
    }

    public void testDataTypeForFieldList() {
        Field listField = new Field("l", FieldType.nullable(new ArrowType.List()), List.of(field(Types.MinorType.BIT)));
        assertEquals(DataType.BOOLEAN, ArrowToEsql.dataTypeForField(listField));
    }

    public void testDataTypeForFieldListEmpty() {
        Field listField = new Field("l", FieldType.nullable(new ArrowType.List()), List.of());
        assertEquals(DataType.UNSUPPORTED, ArrowToEsql.dataTypeForField(listField));
    }

    /**
     * Mirror of {@link #testForFieldOnListOfBigIntReturnsNull} for the schema-inference path.
     * Without this filter, schema inference would advertise the column as LONG and batch
     * conversion would later reject it.
     */
    public void testDataTypeForFieldListOfBigIntUnsupported() {
        Field listField = new Field("l", FieldType.nullable(new ArrowType.List()), List.of(field(Types.MinorType.BIGINT)));
        assertEquals(DataType.UNSUPPORTED, ArrowToEsql.dataTypeForField(listField));
    }

    public void testDataTypeForFieldListOfFloat32Unsupported() {
        Field listField = new Field("l", FieldType.nullable(new ArrowType.List()), List.of(field(Types.MinorType.FLOAT4)));
        assertEquals(DataType.UNSUPPORTED, ArrowToEsql.dataTypeForField(listField));
    }

    /**
     * Mirror of the {@link ArrowToEsql#forField} contract: LARGELIST has no runtime converter
     * (forField returns null for it), so {@link ArrowToEsql#dataTypeForField} must agree and
     * return UNSUPPORTED. Uses BIT — a child type the LIST registry does support — to ensure
     * the LARGELIST gating, not child-type gating, drives the rejection.
     */
    public void testDataTypeForFieldLargeListUnsupported() {
        Field listField = new Field("l", FieldType.nullable(new ArrowType.LargeList()), List.of(field(Types.MinorType.BIT)));
        assertEquals(DataType.UNSUPPORTED, ArrowToEsql.dataTypeForField(listField));
    }

    public void testDataTypeForFieldNestedListUnsupported() {
        Field innerList = new Field("inner", FieldType.nullable(new ArrowType.List()), List.of(field(Types.MinorType.FLOAT4)));
        Field outerList = new Field("outer", FieldType.nullable(new ArrowType.List()), List.of(innerList));
        assertEquals(DataType.UNSUPPORTED, ArrowToEsql.dataTypeForField(outerList));
    }

    private static Field field(Types.MinorType type) {
        return new Field("f", FieldType.nullable(type.getType()), null);
    }
}
