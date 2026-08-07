/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EirfRowToXContentTests extends ESTestCase {

    public void testFlatScalarsAllTypes() throws IOException {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setInt("i", 123);
            builder.setLong("l", 9876543210L);
            builder.setFloat("f", 1.5f);
            builder.setDouble("d", 3.14);
            builder.setString("s", "text");
            builder.setBoolean("bt", true);
            builder.setBoolean("bf", false);
            builder.endDocument();

            Map<String, Object> map = roundTrip(builder, 0);
            assertEquals(123, map.get("i"));
            assertEquals(9876543210L, map.get("l"));
            assertEquals(1.5, ((Number) map.get("f")).doubleValue(), 0.001);
            assertEquals(3.14, ((Number) map.get("d")).doubleValue(), 0.001);
            assertEquals("text", map.get("s"));
            assertEquals(Boolean.TRUE, map.get("bt"));
            assertEquals(Boolean.FALSE, map.get("bf"));
        }
    }

    public void testNestedObject() throws IOException {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("user.name", "alice");
            builder.setInt("user.age", 30);
            builder.setString("status", "ok");
            builder.endDocument();

            Map<String, Object> map = roundTrip(builder, 0);
            assertEquals("ok", map.get("status"));
            Map<?, ?> user = (Map<?, ?>) map.get("user");
            assertEquals("alice", user.get("name"));
            assertEquals(30, user.get("age"));
        }
    }

    public void testDeeplyNestedObject() throws IOException {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("a.b.c.d", "deep");
            builder.setInt("a.b.c.n", 7);
            builder.setString("a.b.sibling", "s");
            builder.endDocument();

            Map<String, Object> map = roundTrip(builder, 0);
            Map<?, ?> a = (Map<?, ?>) map.get("a");
            Map<?, ?> b = (Map<?, ?>) a.get("b");
            assertEquals("s", b.get("sibling"));
            Map<?, ?> c = (Map<?, ?>) b.get("c");
            assertEquals("deep", c.get("d"));
            assertEquals(7, c.get("n"));
        }
    }

    public void testNullColumnsSkipped() throws IOException {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            // Doc 0 establishes three columns under a shared parent.
            builder.startDocument();
            builder.setString("a", "val");
            builder.setInt("b", 1);
            builder.setString("c", "end");
            builder.endDocument();

            // Doc 1 omits "b" entirely, so it should not appear in output.
            builder.startDocument();
            builder.setString("a", "hello");
            builder.setString("c", "world");
            builder.endDocument();

            Map<String, Object> map = roundTrip(builder, 1);
            assertEquals("hello", map.get("a"));
            assertEquals("world", map.get("c"));
            assertFalse("null column must not be emitted", map.containsKey("b"));
        }
    }

    public void testEmptySubObjectsAreOmitted() throws IOException {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            // Doc 0 fully populates the "meta" subtree so the schema has non-leaves for it.
            builder.startDocument();
            builder.setString("name", "doc0");
            builder.setString("meta.version", "1");
            builder.setString("meta.host.id", "h0");
            builder.endDocument();

            // Doc 1 leaves every field under meta null — the whole subtree should be omitted.
            builder.startDocument();
            builder.setString("name", "doc1");
            builder.endDocument();

            Map<String, Object> map = roundTrip(builder, 1);
            assertEquals("doc1", map.get("name"));
            assertFalse("empty sub-object must not be emitted", map.containsKey("meta"));
        }
    }

    public void testOutOfOrderSiblingsFromHeterogeneousDocs() throws IOException {
        // When a later doc introduces a new leaf under an already-established non-leaf, the schema's
        // leaf order interleaves that new leaf after unrelated leaves. Writing by leaf
        // index would emit two copies of the parent object, producing duplicate-field JSON.
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("agent.id", "a0");
            builder.setString("agent.type", "beat");
            builder.setString("host.name", "h0");
            builder.setString("event.dataset", "ds0");
            builder.endDocument();

            // Doc 1 introduces agent.hostname — appended to schema AFTER host.* and event.* leaves.
            builder.startDocument();
            builder.setString("agent.id", "a1");
            builder.setString("agent.type", "beat");
            builder.setString("agent.hostname", "host-1");
            builder.setString("host.name", "h1");
            builder.setString("event.dataset", "ds1");
            builder.endDocument();

            // Doc 1 must serialize to a single "agent" object containing all three agent.* fields.
            Map<String, Object> map = roundTrip(builder, 1);
            Map<?, ?> agent = (Map<?, ?>) map.get("agent");
            assertEquals("a1", agent.get("id"));
            assertEquals("beat", agent.get("type"));
            assertEquals("host-1", agent.get("hostname"));
            Map<?, ?> host = (Map<?, ?>) map.get("host");
            assertEquals("h1", host.get("name"));
            Map<?, ?> event = (Map<?, ?>) map.get("event");
            assertEquals("ds1", event.get("dataset"));
        }
    }

    public void testFixedArray() throws IOException {
        // {"nums": [1, 2, 3]}
        BytesReference source = new BytesArray("""
            {"nums": [1, 2, 3]}""");
        try (EirfBatch batch = EirfEncoder.encode(List.of(source), XContentType.JSON)) {
            Map<String, Object> map = rowAsMap(batch, 0);
            assertEquals(List.of(1, 2, 3), map.get("nums"));
        }
    }

    public void testUnionArray() throws IOException {
        // Mixed-type array forces UNION_ARRAY encoding.
        BytesReference source = new BytesArray("""
            {"mixed": [1, "two", true]}""");
        try (EirfBatch batch = EirfEncoder.encode(List.of(source), XContentType.JSON)) {
            Map<String, Object> map = rowAsMap(batch, 0);
            assertEquals(List.of(1, "two", true), map.get("mixed"));
        }
    }

    public void testNestedArrays() throws IOException {
        BytesReference source = new BytesArray("""
            {"matrix": [[1, 2], [3, 4]]}""");
        try (EirfBatch batch = EirfEncoder.encode(List.of(source), XContentType.JSON)) {
            Map<String, Object> map = rowAsMap(batch, 0);
            assertEquals(List.of(List.of(1, 2), List.of(3, 4)), map.get("matrix"));
        }
    }

    public void testArrayOfObjects() throws IOException {
        BytesReference source = new BytesArray("""
            {"items": [{"name": "a", "val": 1}, {"name": "b", "val": 2}]}""");
        try (EirfBatch batch = EirfEncoder.encode(List.of(source), XContentType.JSON)) {
            Map<String, Object> map = rowAsMap(batch, 0);
            assertEquals(List.of(Map.of("name", "a", "val", 1), Map.of("name", "b", "val", 2)), map.get("items"));
        }
    }

    public void testArrayOfObjectsWithNestedArray() throws IOException {
        BytesReference source = new BytesArray("""
            {"items": [{"tags": ["x", "y"], "id": 1}]}""");
        try (EirfBatch batch = EirfEncoder.encode(List.of(source), XContentType.JSON)) {
            Map<String, Object> map = rowAsMap(batch, 0);
            assertEquals(List.of(Map.of("tags", List.of("x", "y"), "id", 1)), map.get("items"));
        }
    }

    public void testNullValueInArray() throws IOException {
        BytesReference source = new BytesArray("""
            {"vals": [1, null, 3]}""");
        try (EirfBatch batch = EirfEncoder.encode(List.of(source), XContentType.JSON)) {
            Map<String, Object> map = rowAsMap(batch, 0);
            List<?> vals = (List<?>) map.get("vals");
            assertEquals(3, vals.size());
            assertEquals(1, vals.get(0));
            assertNull(vals.get(1));
            assertEquals(3, vals.get(2));
        }
    }

    public void testRoundTripMultipleHeterogeneousDocs() throws IOException {
        // End-to-end JSON → EIRF → JSON round-trip should preserve each doc's content despite
        // the shared schema carrying supersets of fields. Exercises leaf/parent overlap on "b"
        // (string leaf in doc0, object in doc1) and mixed scalar types across docs
        // ("a" is a number in doc0 but a string in doc1; "nested.y" is a string in doc0 but a
        // number in doc1; plus booleans that appear in only one doc each).
        BytesReference src0 = new BytesArray("""
            {"a": 1, "nested": {"x": "x0", "y": "y0"}, "b": "abc", "flag": true}""");
        BytesReference src1 = new BytesArray("""
            {"a": "two", "nested": {"x": "x1", "y": 42}, "b": {"a": 1}, "c": false}""");
        try (EirfBatch batch = EirfEncoder.encode(List.of(src0, src1), XContentType.JSON)) {
            Map<String, Object> d0 = rowAsMap(batch, 0);
            assertEquals(1, d0.get("a"));
            assertEquals("abc", d0.get("b"));
            assertEquals(Map.of("x", "x0", "y", "y0"), d0.get("nested"));
            assertEquals(Boolean.TRUE, d0.get("flag"));
            assertFalse(d0.containsKey("c"));

            Map<String, Object> d1 = rowAsMap(batch, 1);
            assertEquals("two", d1.get("a"));
            assertEquals(Map.of("a", 1), d1.get("b"));
            assertEquals(Map.of("x", "x1", "y", 42), d1.get("nested"));
            assertEquals(Boolean.FALSE, d1.get("c"));
            assertFalse(d1.containsKey("flag"));
        }
    }

    public void testOutputIsParseableNoDuplicateFields() throws IOException {
        // The strict JSON parser used here rejects duplicate field names, so a bug in writeRow
        // that re-opens a parent object would throw here.
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("p.a", "a0");
            builder.setString("q.z", "z0");
            builder.endDocument();

            // Doc 1 introduces p.b after q.* is already in the schema's leaf order.
            builder.startDocument();
            builder.setString("p.a", "a1");
            builder.setString("p.b", "b1");
            builder.setString("q.z", "z1");
            builder.endDocument();

            Map<String, Object> map = roundTrip(builder, 1);
            assertEquals(Map.of("a", "a1", "b", "b1"), map.get("p"));
            assertEquals(Map.of("z", "z1"), map.get("q"));
        }
    }

    private static Map<String, Object> roundTrip(EirfRowBuilder builder, int rowIndex) throws IOException {
        try (EirfBatch batch = builder.build()) {
            return rowAsMap(batch, rowIndex);
        }
    }

    private static Map<String, Object> rowAsMap(EirfBatch batch, int rowIndex) throws IOException {
        BytesReference bytes = writeRowToJson(batch, rowIndex);
        return XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
    }

    private static BytesReference writeRowToJson(EirfBatch batch, int rowIndex) throws IOException {
        try (XContentBuilder xcb = JsonXContent.contentBuilder()) {
            EirfRowToXContent.writeRow(batch.getRowReader(rowIndex), batch.schema(), xcb);
            return BytesReference.bytes(xcb);
        }
    }
}
