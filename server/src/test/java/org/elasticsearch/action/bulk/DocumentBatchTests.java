/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DocumentBatchTests extends ESTestCase {

    public void testRoundTripSimpleDocuments() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"name\":\"alice\",\"age\":30}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"name\":\"bob\",\"age\":25}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("3").source("{\"name\":\"charlie\",\"age\":35}", XContentType.JSON));

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);

        assertEquals(3, batch.docCount());
        assertEquals(2, batch.columnCount()); // name and age

        // Verify doc metadata
        assertEquals("1", batch.docId(0));
        assertEquals("2", batch.docId(1));
        assertEquals("3", batch.docId(2));

        assertNull(batch.docRouting(0));
        assertNull(batch.docRouting(1));

        assertEquals(XContentType.JSON, batch.docXContentType(0));

        // Verify columns
        List<FieldColumn> columns = batch.columnList();
        assertEquals(2, columns.size());

        // Find name and age columns
        FieldColumn nameCol = null, ageCol = null;
        for (FieldColumn col : columns) {
            if ("name".equals(col.fieldPath())) nameCol = col;
            if ("age".equals(col.fieldPath())) ageCol = col;
        }

        assertNotNull("name column should exist", nameCol);
        assertNotNull("age column should exist", ageCol);

        assertEquals(ColumnType.STRING, nameCol.columnType());
        assertEquals(ColumnType.INT, ageCol.columnType());

        // Check values
        assertEquals("alice", nameCol.stringValue(0));
        assertEquals("bob", nameCol.stringValue(1));
        assertEquals("charlie", nameCol.stringValue(2));

        assertEquals(30, ageCol.intValue(0));
        assertEquals(25, ageCol.intValue(1));
        assertEquals(35, ageCol.intValue(2));

        // All present
        assertTrue(nameCol.isPresent(0));
        assertTrue(ageCol.isPresent(0));

        batch.close();
    }

    public void testRoundTripNestedObjects() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"user\":{\"name\":\"alice\",\"age\":30}}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"user\":{\"name\":\"bob\",\"age\":25}}", XContentType.JSON));

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);

        assertEquals(2, batch.docCount());

        List<FieldColumn> columns = batch.columnList();
        assertEquals(2, columns.size());

        // Should be flattened to dot-path columns
        FieldColumn nameCol = null, ageCol = null;
        for (FieldColumn col : columns) {
            if ("user.name".equals(col.fieldPath())) nameCol = col;
            if ("user.age".equals(col.fieldPath())) ageCol = col;
        }

        assertNotNull("user.name column should exist", nameCol);
        assertNotNull("user.age column should exist", ageCol);

        assertEquals("alice", nameCol.stringValue(0));
        assertEquals("bob", nameCol.stringValue(1));

        assertEquals(30, ageCol.intValue(0));
        assertEquals(25, ageCol.intValue(1));

        batch.close();
    }

    public void testRoundTripWithArrays() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"tags\":[\"foo\",\"bar\"],\"count\":5}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"tags\":[\"baz\"],\"count\":10}", XContentType.JSON));

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);

        assertEquals(2, batch.docCount());

        List<FieldColumn> columns = batch.columnList();

        FieldColumn tagsCol = null, countCol = null;
        for (FieldColumn col : columns) {
            if ("tags".equals(col.fieldPath())) tagsCol = col;
            if ("count".equals(col.fieldPath())) countCol = col;
        }

        assertNotNull("tags column should exist", tagsCol);
        assertNotNull("count column should exist", countCol);

        // Arrays are stored as BINARY (raw XContent)
        assertEquals(ColumnType.BINARY, tagsCol.columnType());
        assertEquals(ColumnType.INT, countCol.columnType());

        // The binary value is raw JSON of the array
        BytesReference tagsBytes = tagsCol.binaryValue(0);
        assertNotNull(tagsBytes);
        String tagsJson = tagsBytes.utf8ToString();
        assertTrue("Should contain foo: " + tagsJson, tagsJson.contains("foo"));
        assertTrue("Should contain bar: " + tagsJson, tagsJson.contains("bar"));

        assertEquals(5, countCol.intValue(0));
        assertEquals(10, countCol.intValue(1));

        batch.close();
    }

    public void testRoundTripWithMissingFields() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"name\":\"alice\",\"age\":30}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"name\":\"bob\"}", XContentType.JSON)); // no age
        requests.add(new IndexRequest("test").id("3").source("{\"age\":35}", XContentType.JSON)); // no name

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);

        assertEquals(3, batch.docCount());

        List<FieldColumn> columns = batch.columnList();

        FieldColumn nameCol = null, ageCol = null;
        for (FieldColumn col : columns) {
            if ("name".equals(col.fieldPath())) nameCol = col;
            if ("age".equals(col.fieldPath())) ageCol = col;
        }

        assertNotNull(nameCol);
        assertNotNull(ageCol);

        // Doc 0: both present
        assertTrue(nameCol.isPresent(0));
        assertTrue(ageCol.isPresent(0));

        // Doc 1: name present, age absent
        assertTrue(nameCol.isPresent(1));
        assertFalse(ageCol.isPresent(1));

        // Doc 2: name absent, age present
        assertFalse(nameCol.isPresent(2));
        assertTrue(ageCol.isPresent(2));

        batch.close();
    }

    public void testRoundTripWithRouting() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").routing("r1").source("{\"x\":1}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").routing("r2").source("{\"x\":2}", XContentType.JSON));

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);

        assertEquals("r1", batch.docRouting(0));
        assertEquals("r2", batch.docRouting(1));

        batch.close();
    }

    public void testTypeCoercionIntToLong() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"val\":42}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"val\":3000000000}", XContentType.JSON)); // exceeds int range

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);

        List<FieldColumn> columns = batch.columnList();
        assertEquals(1, columns.size());

        FieldColumn valCol = columns.get(0);
        assertEquals("val", valCol.fieldPath());
        // Should have been widened to LONG
        assertEquals(ColumnType.LONG, valCol.columnType());

        assertEquals(42L, valCol.longValue(0));
        assertEquals(3000000000L, valCol.longValue(1));

        batch.close();
    }

    public void testBooleanColumn() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"active\":true}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"active\":false}", XContentType.JSON));

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);

        List<FieldColumn> columns = batch.columnList();
        assertEquals(1, columns.size());

        FieldColumn col = columns.get(0);
        assertEquals("active", col.fieldPath());
        assertEquals(ColumnType.BOOLEAN, col.columnType());

        assertTrue(col.booleanValue(0));
        assertFalse(col.booleanValue(1));

        batch.close();
    }

    public void testDoubleColumn() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"score\":3.14}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"score\":2.72}", XContentType.JSON));

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);

        List<FieldColumn> columns = batch.columnList();
        assertEquals(1, columns.size());

        FieldColumn col = columns.get(0);
        assertEquals("score", col.fieldPath());
        // Doubles are stored as LONG (raw bits)
        assertEquals(ColumnType.LONG, col.columnType());

        assertEquals(3.14, col.doubleValue(0), 0.001);
        assertEquals(2.72, col.doubleValue(1), 0.001);

        batch.close();
    }

    public void testEmptyBatch() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{}", XContentType.JSON));

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);

        assertEquals(1, batch.docCount());
        assertEquals(0, batch.columnCount());

        Iterator<FieldColumn> columns = batch.columns();
        assertFalse(columns.hasNext());

        batch.close();
    }
}
