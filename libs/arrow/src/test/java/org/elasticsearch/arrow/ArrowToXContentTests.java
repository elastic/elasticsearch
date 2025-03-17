/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.MapVector;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ArrowToXContentTests extends ESTestCase {

    private static void checkPosition(ValueVector vector, int position, String json) throws IOException {
        var out = new ByteArrayOutputStream();
        try (var generator = XContentType.JSON.xContent().createGenerator(out)) {
            generator.writeStartObject();
            ArrowToXContent.writeField(vector, position, null, generator);
            generator.writeEndObject();
        }

        assertEquals(json, out.toString(StandardCharsets.UTF_8));
    }

    public void testWriteField() throws IOException {

        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            IntVector vector = new IntVector("intField", allocator);
        ) {
            vector.allocateNew(1);
            vector.set(0, 123);
            vector.setValueCount(1);

            checkPosition(vector, 0, "{\"intField\":123}");
        }
    }

    public void testWriteVarChar() throws Exception {
        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            VarCharVector vector = new VarCharVector("stringField", allocator);
        ) {
            vector.allocateNew();
            vector.set(0, "test".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);

            checkPosition(vector, 0, "{\"stringField\":\"test\"}");
        }
    }

    public void testWriteMap() throws Exception {
        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            MapVector vector = MapVector.empty("mapField", allocator, false);
        ) {
            var w = vector.getWriter();

            w.startMap();
            w.startEntry();
            w.key().varChar().writeVarChar("key1");
            w.value().integer().writeInt(42);
            w.endEntry();
            w.endMap();

            checkPosition(vector, 0, "{\"mapField\":{\"key1\":42}}");
        }
    }

    public void testWriteNullValue() throws Exception {
        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            IntVector vector = new IntVector("intField", allocator);
        ) {
            vector.allocateNew(1);
            vector.setNull(0);
            vector.setValueCount(1);

            checkPosition(vector, 0, "{\"intField\":null}");
        }
    }

    public void testWriteNullVector() throws Exception {
        try (NullVector vector = new NullVector("nullField", 1);) {
            checkPosition(vector, 0, "{\"nullField\":null}");
        }
    }
}
