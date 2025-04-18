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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
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
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.elasticsearch.libs.arrow.Arrow;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

    public void testBoolean() throws IOException {

        try (var vector = new BitVector("bitField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 1); // 0 is false, other values are true
            vector.setValueCount(1);

            checkPosition(vector, 0, "{\"bitField\":true}");
        }
    }

    public void testInteger() throws IOException {

        try (var vector = new IntVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 123);
            vector.setValueCount(1);

            checkPosition(vector, 0, "{\"intField\":123}");
        }
    }

    public void testFloat() throws IOException {
        float value = 1.25f; // roundtrips through string.

        try (var vector = new Float4Vector("floatField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, value);
            vector.setValueCount(1);

            checkPosition(vector, 0, "{\"floatField\":" + value + "}");
        }
    }

    public void testVarChar() throws Exception {
        try (var vector = new VarCharVector("stringField", allocator)) {
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

        try (var vector = new FixedSizeBinaryVector("bytesField", allocator, value.length)) {
            vector.allocateNew();
            vector.set(0, value);
            vector.setValueCount(1);

            checkPosition(vector, 0, "{\"bytesField\":\"" + expected + "\"}");
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

    public void testTime() throws Exception {
        var millis = randomIntBetween(0, 24 * 60 * 60 * 1000 - 1);

        try (var vector = new TimeSecVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, millis / 1000);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + (millis / 1000 * 1000) + "}");
        }

        try (var vector = new TimeMilliVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, millis);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + millis + "}");
        }

        var nanos = randomLongBetween(0, 24 * 60 * 60 * 1000_000_000L - 1);

        try (var vector = new TimeMicroVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, nanos / 1000L);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + (nanos / 1000L * 1000L) + "}");
        }

        try (var vector = new TimeNanoVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, nanos);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + nanos + "}");
        }
    }

    public void testTimeStamp() throws Exception {
        var millis = 1744304614884L; // Thu Apr 10 19:03:34 CEST 2025

        try (var vector = new TimeStampSecVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, millis / 1000L);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + (millis / 1000L * 1000L) + "}");
        }

        try (var vector = new TimeStampMilliVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, millis);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + millis + "}");
        }

        var nanos = millis * 1_000_000L + 123_456L;

        try (var vector = new TimeStampMicroVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, nanos / 1000L);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + (nanos / 1000L * 1000L) + "}");
        }

        try (var vector = new TimeStampNanoVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, nanos);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + nanos + "}");
        }
    }

    public void testTimeStampTZ() throws Exception {
        var tz = ZoneId.of("UTC+1");

        var millis = 1744304614884L; // Thu Apr 10 19:03:34 CEST 2025
        var wallMillis = millis + 3_600_000L; // UTC+1 is 1 hour ahead of UTC

        try (var vector = new TimeStampSecTZVector("intField", allocator, tz.getId())) {
            vector.allocateNew(1);
            vector.set(0, wallMillis / 1000L);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + (millis / 1000L * 1000L) + "}");
        }

        try (var vector = new TimeStampMilliTZVector("intField", allocator, tz.getId())) {
            vector.allocateNew(1);
            vector.set(0, wallMillis);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + millis + "}");
        }

        var nanos = millis * 1_000_000L + 123_456L;
        var wallNanos = wallMillis * 1_000_000L + 123_456L;

        try (var vector = new TimeStampMicroTZVector("intField", allocator, tz.getId())) {
            vector.allocateNew(1);
            vector.set(0, wallNanos / 1000L);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + (nanos / 1000L * 1000L) + "}");
        }

        try (var vector = new TimeStampNanoTZVector("intField", allocator, tz.getId())) {
            vector.allocateNew(1);
            vector.set(0, wallNanos);
            vector.setValueCount(1);
            checkPosition(vector, 0, "{\"intField\":" + nanos + "}");
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
