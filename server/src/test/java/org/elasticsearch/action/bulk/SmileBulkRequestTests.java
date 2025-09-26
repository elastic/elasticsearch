/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

// TODO: test chunking
public class SmileBulkRequestTests extends ESTestCase {

    public void testSingleRequest() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        BytesReference data;
        try (BytesStreamOutput out = new BytesStreamOutput(); BytesStreamOutput doc = new BytesStreamOutput()) {
            buildIndexRequest(doc, contentType);
            out.writeInt(doc.size());
            out.write(doc.bytes().array(), doc.bytes().arrayOffset(), doc.bytes().length());
            doc.reset();
            buildDoc(doc, contentType);
            out.writeInt(doc.size());
            out.write(doc.bytes().array(), doc.bytes().arrayOffset(), doc.bytes().length());
            data = out.bytes();
        }
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(data, null, contentType, RestBulkAction.BulkFormat.PREFIX_LENGTH);
        assertEquals(1, bulkRequest.requests().size());
        DocWriteRequest<?> docWriteRequest = bulkRequest.requests().get(0);
        assertEquals(DocWriteRequest.OpType.INDEX, docWriteRequest.opType());
        assertEquals("index", docWriteRequest.index());
        assertEquals("test", docWriteRequest.id());
        assertThat(docWriteRequest, instanceOf(IndexRequest.class));
        IndexRequest request = (IndexRequest) docWriteRequest;
        assertEquals(1, request.sourceAsMap().size());
        assertEquals("value", request.sourceAsMap().get("field"));
    }

    private void buildIndexRequest(BytesStreamOutput doc, XContentType contentType) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(contentType, doc)) {
            builder.startObject();
            builder.startObject("index");
            builder.field("_index", "index");
            builder.field("_id", "test");
            builder.endObject();
            builder.endObject();
        }
    }

    private void buildDoc(BytesStreamOutput doc, XContentType contentType) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(contentType, doc)) {
            builder.startObject();
            builder.field("field", "value");
            builder.endObject();
        }
    }

    public void testMultipleRequests() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        int numRequests = randomIntBetween(1, 20);
        BytesReference data;
        try (BytesStreamOutput out = new BytesStreamOutput(); BytesStreamOutput doc = new BytesStreamOutput()) {
            for (int i = 0; i < numRequests; i++) {
                buildIndexRequest(doc, contentType);
                out.writeInt(doc.size());
                out.write(doc.bytes().array(), doc.bytes().arrayOffset(), doc.bytes().length());
                doc.reset();
                buildDoc(doc, contentType);
                out.writeInt(doc.size());
                out.write(doc.bytes().array(), doc.bytes().arrayOffset(), doc.bytes().length());
                doc.reset();
            }
            data = out.bytes();
        }
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(data, null, contentType, RestBulkAction.BulkFormat.PREFIX_LENGTH);
        assertEquals(numRequests, bulkRequest.requests().size());
        for (int i = 0; i < numRequests; i++) {
            DocWriteRequest<?> docWriteRequest = bulkRequest.requests().get(i);
            assertEquals(DocWriteRequest.OpType.INDEX, docWriteRequest.opType());
            assertEquals("index", docWriteRequest.index());
            assertEquals("test", docWriteRequest.id());
            assertThat(docWriteRequest, instanceOf(IndexRequest.class));
            IndexRequest request = (IndexRequest) docWriteRequest;
            assertEquals(1, request.sourceAsMap().size());
            assertEquals("value", request.sourceAsMap().get("field"));
        }
    }
}
