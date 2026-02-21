/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.DocumentBatch;
import org.elasticsearch.action.bulk.DocumentBatchEncoder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchDocumentParserTests extends MapperServiceTestCase {

    public void testParseSimpleBatch() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("value").field("type", "long").endObject();
        }));

        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"name\":\"alice\",\"value\":10}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"name\":\"bob\",\"value\":20}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("3").source("{\"name\":\"charlie\",\"value\":30}", XContentType.JSON));

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);
        BatchDocumentParser parser = mapperService.createBatchDocumentParser();
        BatchDocumentParser.BatchResult result = parser.parseBatch(batch, mapperService.mappingLookup());

        assertEquals(3, result.size());
        for (int i = 0; i < 3; i++) {
            assertTrue("Doc " + i + " should succeed", result.isSuccess(i));
            assertNotNull("Doc " + i + " should have parsed document", result.getDocument(i));
        }

        // Verify Lucene fields were indexed
        ParsedDocument doc0 = result.getDocument(0);
        assertNotNull(doc0.rootDoc().getField("name"));
        assertNotNull(doc0.rootDoc().getField("value"));

        batch.close();
    }

    public void testParseBatchWithMissingFields() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("value").field("type", "long").endObject();
        }));

        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"name\":\"alice\",\"value\":10}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"name\":\"bob\"}", XContentType.JSON)); // no value
        requests.add(new IndexRequest("test").id("3").source("{\"value\":30}", XContentType.JSON)); // no name

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);
        BatchDocumentParser parser = mapperService.createBatchDocumentParser();
        BatchDocumentParser.BatchResult result = parser.parseBatch(batch, mapperService.mappingLookup());

        assertEquals(3, result.size());
        for (int i = 0; i < 3; i++) {
            assertTrue("Doc " + i + " should succeed", result.isSuccess(i));
        }

        // Doc 0 has both fields
        assertNotNull(result.getDocument(0).rootDoc().getField("name"));
        assertNotNull(result.getDocument(0).rootDoc().getField("value"));

        // Doc 1 has name but not value
        assertNotNull(result.getDocument(1).rootDoc().getField("name"));
        assertNull(result.getDocument(1).rootDoc().getField("value"));

        // Doc 2 has value but not name
        assertNull(result.getDocument(2).rootDoc().getField("name"));
        assertNotNull(result.getDocument(2).rootDoc().getField("value"));

        batch.close();
    }

    public void testParseBatchWithMultipleTypes() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("age").field("type", "integer").endObject();
            b.startObject("active").field("type", "boolean").endObject();
            b.startObject("message").field("type", "text").endObject();
        }));

        List<IndexRequest> requests = new ArrayList<>();
        requests.add(
            new IndexRequest("test").id("1")
                .source("{\"name\":\"alice\",\"age\":30,\"active\":true,\"message\":\"hello\"}", XContentType.JSON)
        );
        requests.add(
            new IndexRequest("test").id("2")
                .source("{\"name\":\"bob\",\"age\":25,\"active\":false,\"message\":\"world\"}", XContentType.JSON)
        );

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);
        BatchDocumentParser parser = mapperService.createBatchDocumentParser();
        BatchDocumentParser.BatchResult result = parser.parseBatch(batch, mapperService.mappingLookup());

        assertEquals(2, result.size());
        assertTrue(result.isSuccess(0));
        assertTrue(result.isSuccess(1));

        // Verify all field types were parsed
        ParsedDocument doc0 = result.getDocument(0);
        assertNotNull(doc0.rootDoc().getField("name"));
        assertNotNull(doc0.rootDoc().getField("age"));
        assertNotNull(doc0.rootDoc().getField("active"));
        assertNotNull(doc0.rootDoc().getField("message"));

        batch.close();
    }

    public void testParseBatchWithUnmappedField() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> { b.startObject("name").field("type", "keyword").endObject(); }));

        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"name\":\"alice\",\"unknown\":\"oops\"}", XContentType.JSON));

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);
        BatchDocumentParser parser = mapperService.createBatchDocumentParser();

        // Should throw because dynamic mapping is not supported in batch mode
        expectThrows(IllegalArgumentException.class, () -> parser.parseBatch(batch, mapperService.mappingLookup()));

        batch.close();
    }

    public void testParseLargeBatch() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("value").field("type", "long").endObject();
        }));

        int numDocs = randomIntBetween(50, 200);
        List<IndexRequest> requests = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            requests.add(
                new IndexRequest("test").id("id-" + i).source("{\"name\":\"doc-" + i + "\",\"value\":" + i + "}", XContentType.JSON)
            );
        }

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);
        BatchDocumentParser parser = mapperService.createBatchDocumentParser();
        BatchDocumentParser.BatchResult result = parser.parseBatch(batch, mapperService.mappingLookup());

        assertEquals(numDocs, result.size());
        List<ParsedDocument> successful = result.successfulDocuments();
        assertEquals(numDocs, successful.size());

        for (int i = 0; i < numDocs; i++) {
            assertTrue("Doc " + i + " should succeed", result.isSuccess(i));
            ParsedDocument doc = result.getDocument(i);
            assertNotNull(doc);
            assertNotNull(doc.rootDoc().getField("name"));
            assertNotNull(doc.rootDoc().getField("value"));
        }

        batch.close();
    }

    public void testParseBatchWithNestedFields() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("user");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("name").field("type", "keyword").endObject();
                    b.startObject("age").field("type", "integer").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"user\":{\"name\":\"alice\",\"age\":30}}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"user\":{\"name\":\"bob\",\"age\":25}}", XContentType.JSON));

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);
        BatchDocumentParser parser = mapperService.createBatchDocumentParser();
        BatchDocumentParser.BatchResult result = parser.parseBatch(batch, mapperService.mappingLookup());

        assertEquals(2, result.size());
        assertTrue(result.isSuccess(0));
        assertTrue(result.isSuccess(1));

        // Nested fields should be indexed with their full dotted path
        ParsedDocument doc0 = result.getDocument(0);
        assertNotNull(doc0.rootDoc().getField("user.name"));
        assertNotNull(doc0.rootDoc().getField("user.age"));

        batch.close();
    }

    public void testSuccessfulDocumentsFiltersFailures() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> { b.startObject("name").field("type", "keyword").endObject(); }));

        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"name\":\"alice\"}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"name\":\"bob\"}", XContentType.JSON));

        DocumentBatch batch = DocumentBatchEncoder.encode(requests);
        BatchDocumentParser parser = mapperService.createBatchDocumentParser();
        BatchDocumentParser.BatchResult result = parser.parseBatch(batch, mapperService.mappingLookup());

        List<ParsedDocument> successful = result.successfulDocuments();
        assertEquals(2, successful.size());

        batch.close();
    }
}
