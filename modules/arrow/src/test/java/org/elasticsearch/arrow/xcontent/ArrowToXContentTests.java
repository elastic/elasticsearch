/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.xcontent;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.elasticsearch.libs.arrow.Arrow;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Base64;
import java.util.List;

public class ArrowToXContentTests extends ESTestCase {

    private BufferAllocator allocator;

    @Before
    public void init() {
        this.allocator = Arrow.newChildAllocator("test", 0, Long.MAX_VALUE);
    }

    @After
    public void close() {
        this.allocator.close();
    }

    private void checkPosition(FieldVector vector, int position, String json) throws IOException {
        var arrowToXContent = new ArrowToXContent();

        var root = new VectorSchemaRoot(List.of(vector));
        // We don't close `root` as it would close `vector` which is owned by the caller.
        // This allows checkPosition() to be called several times with the same vector.

        // Roundtrip the vector through its binary representation
        var arrowOut = new ByteArrayOutputStream();
        try (var writer = new ArrowStreamWriter(root, null, arrowOut)) {
            writer.writeBatch();
        }

        try (var reader = new ArrowStreamReader(new ByteArrayInputStream(arrowOut.toByteArray()), allocator)) {
            reader.loadNextBatch();
            var newVector = reader.getVectorSchemaRoot().getVector(0);

            var jsonOut = new ByteArrayOutputStream();
            try (var generator = XContentType.JSON.xContent().createGenerator(jsonOut)) {
                generator.writeStartObject();
                arrowToXContent.writeField(newVector, position, null, generator);
                generator.writeEndObject();
            }

            assertEquals(json, jsonOut.toString(StandardCharsets.UTF_8));
        }
    }

    private FieldVector newVector(String name, Types.MinorType type) {
        return type.getNewVector(name, new FieldType(true, type.getType(), null), allocator, null);
    }

    // Tests below are in the same order as ArrowToXContent.writeValue()
    //
    // Note: dictionary encoding is tested in ArrowBulkIncrementalParserTests as
    // dictionaries are attached to the StreamReader and need more than checkPosition() above.

    public void testNullValue() throws Exception {
        try (var vector = new IntVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.setNull(0);
            vector.setValueCount(1);

            checkPosition(vector, 0, "{\"intField\":null}");
        }
    }

    public void testIntegers() throws IOException {

        try (var vector = new TinyIntVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 123);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":123}");
        }

        try (var vector = new SmallIntVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 123);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":123}");
        }

        try (var vector = new IntVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 123);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":123}");
        }

        try (var vector = new BigIntVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 123);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":123}");
        }

        try (var vector = new UInt1Vector("intField", allocator)) {
            vector.allocateNew(2);
            vector.set(0, 123);
            vector.set(1, 253); // unsigned > 0x7F
            vector.setValueCount(2);
            checkPosition(vector, 0, "{\"intField\":123}");
            checkPosition(vector, 1, "{\"intField\":253}");
        }

        try (var vector = new UInt2Vector("intField", allocator)) {
            vector.allocateNew(2);
            vector.set(0, 123);
            vector.set(1, 65533); // unsigned > 0x7FFF
            vector.setValueCount(2);
            checkPosition(vector, 0, "{\"intField\":123}");
            checkPosition(vector, 1, "{\"intField\":65533}");
        }

        try (var vector = new UInt4Vector("intField", allocator)) {
            vector.allocateNew(2);
            vector.set(0, 123);

            long x = 0xFFFFFFFDL;
            assertEquals(4294967293L, x);
            // "A narrowing conversion of a signed integer to an integral type T simply
            // discards all but the n lowest order bits"
            // https://docs.oracle.com/javase/specs/jls/se11/html/jls-5.html#jls-5.1.3
            vector.set(1, (int)x);

            vector.setValueCount(2);
            checkPosition(vector, 0, "{\"intField\":123}");
            checkPosition(vector, 1, "{\"intField\":4294967293}");
        }

        try (var vector = new UInt8Vector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 123);
            vector.setValueCount(1);
            // No test for large unsigned value as Java has no support for it.
            checkPosition(vector, 0, "{\"intField\":123}");
        }
    }

    public void testFloats() throws IOException {
        float value = 1.25f; // roundtrips through string.

        try (var vector = new Float2Vector("floatField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, Float16.toFloat16(value));
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"floatField\":1.25}");
        }

        try (var vector = new Float4Vector("floatField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, value);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"floatField\":1.25}");
        }

        try (var vector = new Float8Vector("floatField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, value);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"floatField\":1.25}");
        }
    }

    public void testDecimals() throws IOException {
        var value = new BigDecimal("1.25");

        try (var vector = new DecimalVector("decimalField", allocator, value.precision(), value.scale())) {
            vector.allocateNew(1);
            vector.set(0, new BigDecimal("1.25"));
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"decimalField\":1.25}");
        }

        try (var vector = new Decimal256Vector("decimalField", allocator, value.precision(), value.scale())) {
            vector.allocateNew(1);
            vector.set(0, new BigDecimal("1.25"));
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"decimalField\":1.25}");
        }
    }

    public void testBoolean() throws IOException {

        try (var vector = new BitVector("bitField", allocator)) {
            vector.allocateNew(3);
            vector.set(0, 0); // 0 is false, other values are true
            vector.set(1, 1);
            vector.set(1, 2);
            vector.setValueCount(3);
            checkPosition(vector, 0, "{\"bitField\":false}");
            checkPosition(vector, 1, "{\"bitField\":true}");
            checkPosition(vector, 2, "{\"bitField\":true}");
        }
    }

    public void testVarChar() throws Exception {
        try (var vector = new VarCharVector("stringField", allocator)) {
            vector.allocateNew();
            vector.set(0, "test".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"stringField\":\"test\"}");
        }

        try (var vector = new LargeVarCharVector("stringField", allocator)) {
            vector.allocateNew();
            vector.set(0, "test".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"stringField\":\"test\"}");
        }

        try (var vector = new ViewVarCharVector("stringField", allocator)) {
            vector.allocateNew();
            vector.set(0, "test".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"stringField\":\"test\"}");
        }
    }

    public void testBinary() throws Exception {
        var value = "test".getBytes(StandardCharsets.UTF_8);
        var expected = Base64.getEncoder().encodeToString(value);

        try (var vector = new VarBinaryVector("bytesField", allocator)) {
            vector.allocateNew();
            vector.set(0, value);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"bytesField\":\"" + expected + "\"}");
        }

        try (var vector = new LargeVarBinaryVector("bytesField", allocator)) {
            vector.allocateNew();
            vector.set(0, value);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"bytesField\":\"" + expected + "\"}");
        }

        try (var vector = new ViewVarBinaryVector("bytesField", allocator)) {
            vector.allocateNew();
            vector.set(0, value);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"bytesField\":\"" + expected + "\"}");
        }

        try (var vector = new FixedSizeBinaryVector("bytesField", allocator, value.length)) {
            vector.allocateNew();
            vector.set(0, value);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"bytesField\":\"" + expected + "\"}");
        }
    }

    public void testTimeStamp() throws Exception {
        var millis = 1744304614884L; // Thu Apr 10 19:03:34 CEST 2025

        try (var vector = new TimeStampSecVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, millis / 1000L);
            vector.setValueCount(1);
            // Check millis value truncated to seconds
            checkPosition(vector, 0, "{\"field\":" + (millis / 1000L * 1000L) + "}");
        }

        try (var vector = new TimeStampMilliVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, millis);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + millis + "}");
        }

        var nanos = millis * 1_000_000L + 123_456L;

        try (var vector = new TimeStampMicroVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, nanos / 1000L);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + (nanos / 1000L * 1000L) + "}");
        }

        try (var vector = new TimeStampNanoVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, nanos);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + nanos + "}");
        }
    }

    public void testTimeStampTZ() throws Exception {
        // Only used to create the vector. It is dropped when converting to XContent.
        var tz = ZoneId.of("UTC+1");

        var millis = 1744304614884L; // Thu Apr 10 19:03:34 CEST 2025

        try (var vector = new TimeStampSecTZVector("field", allocator, tz.getId())) {
            vector.allocateNew(1);
            vector.set(0, millis / 1000L);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + (millis / 1000L * 1000L) + "}");
        }

        try (var vector = new TimeStampMilliTZVector("field", allocator, tz.getId())) {
            vector.allocateNew(1);
            vector.set(0, millis);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + millis + "}");
        }

        var nanos = millis * 1_000_000L + 123_456L;

        try (var vector = new TimeStampMicroTZVector("field", allocator, tz.getId())) {
            vector.allocateNew(1);
            vector.set(0, nanos / 1000L);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + (nanos / 1000L * 1000L) + "}");
        }

        try (var vector = new TimeStampNanoTZVector("field", allocator, tz.getId())) {
            vector.allocateNew(1);
            vector.set(0, nanos);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + nanos + "}");
        }
    }

    public void testDate() throws IOException {
        var days = randomIntBetween(1, 20329744); // 2025-08-29
        var millis = days * 86_400_000;

        try (var vector = new DateDayVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, days);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":"+ millis + "}");
        }

        try (var vector = new DateMilliVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, millis);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":"+  millis + "}");
        }
    }

    public void testTime() throws Exception {
        var millis = randomIntBetween(0, 86_400_000 - 1);

        try (var vector = new TimeSecVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, millis / 1000);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + (millis / 1000 * 1000) + "}");
        }

        try (var vector = new TimeMilliVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, millis);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + millis + "}");
        }

        var nanos = randomLongBetween(0, 86_400_000_000_000L - 1);

        try (var vector = new TimeMicroVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, nanos / 1000L);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + (nanos / 1000 * 1000) + "}");
        }

        try (var vector = new TimeNanoVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, nanos);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + nanos + "}");
        }
    }

    public void testDuration() throws IOException {
        var millis = randomLongBetween(-1_000_000L, 1_000_000L);

        try (var vector = new DurationVector(
            new Field("field", FieldType.nullable(Types.MinorType.DURATION.getType()), null), allocator)
        ) {
            vector.allocateNew(1);
            vector.set(0, millis / 1000L);
            vector.setValueCount(1);
            // Check millis value truncated to seconds
            checkPosition(vector, 0, "{\"field\":" + (millis / 1000L * 1000L) + "}");
        }

        try (var vector = new TimeStampMilliVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, millis);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + millis + "}");
        }

        var nanos = millis * 1_000_000L + 123_456L;

        try (var vector = new TimeStampMicroVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, nanos / 1000L);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + (nanos / 1000L * 1000L) + "}");
        }

        try (var vector = new TimeStampNanoVector("field", allocator)) {
            vector.allocateNew(1);
            vector.set(0, nanos);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"field\":" + nanos + "}");
        }

    }


    public void testList() throws Exception {
        try (var vector = ListVector.empty("listField", allocator)) {
            var w = vector.getWriter();

            w.startList();
            w.writeInt(1);
            w.writeInt(2);
            w.endList();

            w.startList();
            w.writeInt(3);
            w.writeInt(4);
            w.writeInt(5);
            w.endList();
            w.setValueCount(w.getPosition());

            checkPosition(vector, 0, "{\"listField\":[1,2]}");
            checkPosition(vector, 1, "{\"listField\":[3,4,5]}");
        }
    }



    public void testMap() throws Exception {
        try (var vector = MapVector.empty("mapField", allocator, false)) {
            var w = vector.getWriter();

            w.startMap();
            w.startEntry();
            w.key().varChar().writeVarChar("key1");
            w.value().integer().writeInt(42);
            w.endEntry();
            w.endMap();
            w.setValueCount(w.getPosition());

            checkPosition(vector, 0, "{\"mapField\":{\"key1\":42}}");
        }
    }

    // TODO: struct (already exercised in ArrowBulkIncrementalParserTests)
    // TODO: dense union
    // TODO: union (already exercised in ArrowBulkIncrementalParserTests)

    public void testNullVector() throws Exception {
        try (NullVector vector = new NullVector("nullField", 1);) {
            checkPosition(vector, 0, "{\"nullField\":null}");
        }
    }

}
