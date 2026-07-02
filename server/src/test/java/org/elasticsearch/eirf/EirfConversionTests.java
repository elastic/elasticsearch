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
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EirfConversionTests extends ESTestCase {

    public void testRowToXContentFlatDocument() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"name\":\"alice\",\"age\":30}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals("alice", result.get("name"));
        assertEquals(30, result.get("age"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentNestedDocument() throws IOException {
        EirfBatch batch = EirfEncoder.encode(
            List.of(new BytesArray("{\"user\":{\"name\":\"alice\",\"age\":30},\"status\":\"active\"}")),
            XContentType.JSON
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals("active", result.get("status"));
        Map<String, Object> user = (Map<String, Object>) result.get("user");
        assertEquals("alice", user.get("name"));
        assertEquals(30, user.get("age"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentDeepNesting() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"a\":{\"b\":{\"c\":42}}}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        Map<String, Object> a = (Map<String, Object>) result.get("a");
        Map<String, Object> b = (Map<String, Object>) a.get("b");
        assertEquals(42, b.get("c"));

        batch.close();
    }

    public void testRowToXContentWithArray() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"name\":\"alice\",\"tags\":[\"a\",\"b\"]}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals("alice", result.get("name"));
        assertEquals(List.of("a", "b"), result.get("tags"));

        batch.close();
    }

    public void testRowToXContentSkipsAbsentFields() throws IOException {
        EirfBatch batch = EirfEncoder.encode(
            List.of(new BytesArray("{\"name\":\"alice\",\"age\":30}"), new BytesArray("{\"age\":25}")),
            XContentType.JSON
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(1), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertFalse(result.containsKey("name"));
        assertEquals(25, result.get("age"));

        batch.close();
    }

    public void testRowToXContentEmitsExplicitNull() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"name\":\"alice\",\"age\":null}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals("alice", result.get("name"));
        assertTrue("age column should round-trip as explicit null", result.containsKey("age"));
        assertNull(result.get("age"));

        batch.close();
    }

    public void testRowToXContentAbsentVsExplicitNullAcrossDocsInBatch() throws IOException {
        EirfBatch batch = EirfEncoder.encode(
            List.of(new BytesArray("{\"name\":\"alice\",\"age\":null}"), new BytesArray("{\"name\":\"bob\"}")),
            XContentType.JSON
        );

        // Explicit null round-trips back as null so mappers (e.g. null_value substitution) can act on it.
        XContentBuilder explicit = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), explicit);
        explicit.close();
        Map<String, Object> explicitResult = XContentHelper.convertToMap(BytesReference.bytes(explicit), false, XContentType.JSON).v2();
        assertEquals("alice", explicitResult.get("name"));
        assertTrue(explicitResult.containsKey("age"));
        assertNull(explicitResult.get("age"));

        // The second doc never mentioned "age" — it stays absent rather than materializing as null.
        XContentBuilder absent = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(1), batch.schema(), absent);
        absent.close();
        Map<String, Object> absentResult = XContentHelper.convertToMap(BytesReference.bytes(absent), false, XContentType.JSON).v2();
        assertEquals("bob", absentResult.get("name"));
        assertFalse(absentResult.containsKey("age"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentPreservesExplicitNullInNestedObject() throws IOException {
        EirfBatch batch = EirfEncoder.encode(
            List.of(new BytesArray("{\"agent\":{\"id\":\"a\",\"optional_version\":null}}")),
            XContentType.JSON
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        Map<String, Object> agent = (Map<String, Object>) result.get("agent");
        assertEquals("a", agent.get("id"));
        assertTrue(agent.containsKey("optional_version"));
        assertNull(agent.get("optional_version"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentPreservesEmptyObject() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"labels\":{},\"application_data\":{}}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertTrue("labels should round-trip as an empty object", result.containsKey("labels"));
        assertEquals(Map.of(), result.get("labels"));
        assertTrue("application_data should round-trip as an empty object", result.containsKey("application_data"));
        assertEquals(Map.of(), result.get("application_data"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentPreservesNestedEmptyObject() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"a\":{\"b\":{}}}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        Map<String, Object> a = (Map<String, Object>) result.get("a");
        assertTrue(a.containsKey("b"));
        assertEquals(Map.of(), a.get("b"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentEmptyVsPopulatedObjectAcrossDocsInBatch() throws IOException {
        // Doc A has labels:{} (empty), doc B has labels:{"x":1} (populated). They must round-trip independently —
        // a populated labels in one doc must not bleed into a sibling doc that only had an empty object.
        EirfBatch batch = EirfEncoder.encode(
            List.of(new BytesArray("{\"labels\":{}}"), new BytesArray("{\"labels\":{\"x\":1}}")),
            XContentType.JSON
        );

        XContentBuilder builderA = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builderA);
        builderA.close();
        Map<String, Object> resultA = XContentHelper.convertToMap(BytesReference.bytes(builderA), false, XContentType.JSON).v2();
        assertEquals(Map.of(), resultA.get("labels"));

        XContentBuilder builderB = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(1), batch.schema(), builderB);
        builderB.close();
        Map<String, Object> resultB = XContentHelper.convertToMap(BytesReference.bytes(builderB), false, XContentType.JSON).v2();
        assertEquals(Map.of("x", 1), resultB.get("labels"));

        batch.close();
    }

    public void testRowToXContentWithBooleans() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"active\":true,\"deleted\":false}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals(true, result.get("active"));
        assertEquals(false, result.get("deleted"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentFixedArrayOfNulls() throws IOException {
        // FIXED_ARRAY of NULL has zero-byte elements; without forcing UNION the array would read back empty.
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"xs\":[null,null,null]}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        List<Object> xs = (List<Object>) result.get("xs");
        assertEquals(3, xs.size());
        for (Object x : xs) {
            assertNull(x);
        }

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentFixedArrayOfBooleans() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"xs\":[true,true,true],\"ys\":[false,false]}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals(List.of(true, true, true), result.get("xs"));
        assertEquals(List.of(false, false), result.get("ys"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentMixedBooleans() throws IOException {
        // Mixed shared types fall to UNION naturally; guards that the existing UNION path still works
        // post-fix when element types differ.
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"xs\":[true,false,true]}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals(List.of(true, false, true), result.get("xs"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testJsonRoundTrip() throws IOException {
        String json = "{\"user\":{\"name\":\"alice\",\"age\":30},\"status\":\"active\",\"score\":3.14}";
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray(json)), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals("active", result.get("status"));
        Map<String, Object> user = (Map<String, Object>) result.get("user");
        assertEquals("alice", user.get("name"));
        assertEquals(30, user.get("age"));

        batch.close();
    }

    public void testRowToXContentFloatEmitsWithDoublePrecision() throws IOException {
        // 0.10000000149011612 is the exact double representation of 0.1f. The encoder stores it as FLOAT
        // (because (double)(float)val == val), and we want the rehydrated JSON to keep enough precision for
        // downstream double parsers rather than collapsing to "0.1".
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"x\":0.10000000149011612}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        String json = BytesReference.bytes(builder).utf8ToString();
        assertTrue("expected double-precision textual form of 0.1f, got: " + json, json.contains("0.10000000149011612"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentFloatArrayElementsDeserializeAsDouble() throws IOException {
        // 0.5 and 0.25 are FLOAT-exact, so they pack as a FIXED_ARRAY of FLOAT. After the cast-to-double
        // fix, array elements round-trip through the double branch of XContentParser.
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"xs\":[0.5,0.25]}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        List<Object> xs = (List<Object>) result.get("xs");
        assertEquals(2, xs.size());
        assertTrue("expected double elements, got: " + xs.get(0).getClass(), xs.get(0) instanceof Double);
        assertTrue("expected double elements, got: " + xs.get(1).getClass(), xs.get(1) instanceof Double);
        assertEquals(0.5, (Double) xs.get(0), 0.0);
        assertEquals(0.25, (Double) xs.get(1), 0.0);

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentMultipleNestedSiblings() throws IOException {
        String json = "{\"user\":{\"name\":\"alice\"},\"meta\":{\"version\":1}}";
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray(json)), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        Map<String, Object> user = (Map<String, Object>) result.get("user");
        assertEquals("alice", user.get("name"));
        Map<String, Object> meta = (Map<String, Object>) result.get("meta");
        assertEquals(1, meta.get("version"));

        batch.close();
    }
}
