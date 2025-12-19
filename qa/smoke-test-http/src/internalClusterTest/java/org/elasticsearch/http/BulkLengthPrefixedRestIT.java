/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.bulk.XContentLengthPrefixedStreamingType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 2, numClientNodes = 0)
public class BulkLengthPrefixedRestIT extends HttpSmokeTestCase {

    private final RequestOptions options;
    private final XContentLengthPrefixedStreamingType xContentLengthPrefixedStreamingType;
    private final XContentType returnXContentType;

    public BulkLengthPrefixedRestIT() {
        xContentLengthPrefixedStreamingType = randomFrom(XContentLengthPrefixedStreamingType.values());
        returnXContentType = randomFrom(XContentType.values());
        options = RequestOptions.DEFAULT.toBuilder()
            .addHeader("Content-Type", randomFrom(xContentLengthPrefixedStreamingType.headerValues()).v1())
            .addHeader("Accept", randomFrom(returnXContentType.headerValues()).v1())
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IncrementalBulkService.INCREMENTAL_BULK.getKey(), seventyFivePercentOfTheTime())
            .build();
    }

    private static boolean seventyFivePercentOfTheTime() {
        return (randomBoolean() && randomBoolean()) == false;
    }

    public void testPrefixLengthFormatCapability() throws IOException {
        {
            String verb = randomBoolean() ? "PUT" : "POST";
            Request request = new Request("GET", "/_capabilities?method=" + verb + "&path=_bulk&capabilities=prefix_length_format");
            Response response = getRestClient().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
            Map<String, Object> responseMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                response.getEntity().getContent(),
                true
            );
            assertEquals(Boolean.TRUE, responseMap.get("supported"));
        }
        {
            String verb = randomBoolean() ? "GET" : "DELETE";
            Request request = new Request("GET", "/_capabilities?method=" + verb + "&path=_bulk&capabilities=prefix_length_format");
            Response response = getRestClient().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
            Map<String, Object> responseMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                response.getEntity().getContent(),
                true
            );
            assertEquals(Boolean.FALSE, responseMap.get("supported"));
        }
    }

    public void testBulkMissingBody() {
        Request request = new Request(randomBoolean() ? "POST" : "PUT", "/_bulk");
        request.setOptions(options);
        request.setEntity(new ByteArrayEntity(new byte[0]));
        ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body is required"));
    }

    public void testBulkRequestBodyWrongLength() throws IOException {
        Request request = new Request(randomBoolean() ? "POST" : "PUT", "/_bulk");
        request.setOptions(options);
        // missing final line of the bulk body. cannot process
        try (ByteArrayOutputStream bulk = new ByteArrayOutputStream(); ByteArrayOutputStream doc = new ByteArrayOutputStream()) {
            DataOutput dataOutput = new DataOutputStream(bulk);
            createActionDocument(doc, "index", "index_name", "1");
            writeDocToBulk(bulk, doc);
            createDocument(doc, 1);
            dataOutput.writeInt(doc.size() + 1);
            doc.writeTo(bulk);
            doc.reset();
            request.setEntity(new ByteArrayEntity(bulk.toByteArray()));
        }
        ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(
            responseException.getMessage(),
            containsString("Documents in the bulk request must be prefixed with the length of the document")
        );
    }

    public void testBulkRequest() throws IOException {
        Request createRequest = new Request("PUT", "/index_name");
        createRequest.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": 1,
                  "number_of_replicas": 1,
                  "write.wait_for_active_shards": 2
                }
              }
            }""");
        final Response indexCreatedResponse = getRestClient().performRequest(createRequest);
        assertThat(indexCreatedResponse.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));

        Request firstBulkRequest = new Request("POST", "/index_name/_bulk");
        firstBulkRequest.setOptions(options);
        try (ByteArrayOutputStream bulk = new ByteArrayOutputStream(); ByteArrayOutputStream doc = new ByteArrayOutputStream()) {
            createActionDocument(doc, "index", "index_name", "1");
            writeDocToBulk(bulk, doc);
            createDocument(doc, 1);
            writeDocToBulk(bulk, doc);
            createActionDocument(doc, "index", "index_name", "2");
            writeDocToBulk(bulk, doc);
            createDocument(doc, 2);
            writeDocToBulk(bulk, doc);
            firstBulkRequest.setEntity(new ByteArrayEntity(bulk.toByteArray()));
        }

        final Response indexSuccessFul = getRestClient().performRequest(firstBulkRequest);
        assertThat(indexSuccessFul.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));

        sendLargeBulk();
    }

    public void testBulkWithIncrementalDisabled() throws IOException {
        Request createRequest = new Request("PUT", "/index_name");
        createRequest.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": 1,
                  "number_of_replicas": 1,
                  "write.wait_for_active_shards": 2
                }
              }
            }""");
        final Response indexCreatedResponse = getRestClient().performRequest(createRequest);
        assertThat(indexCreatedResponse.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));

        Request firstBulkRequest = new Request("POST", "/index_name/_bulk");
        firstBulkRequest.setOptions(options);
        try (ByteArrayOutputStream bulk = new ByteArrayOutputStream(); ByteArrayOutputStream doc = new ByteArrayOutputStream()) {
            createActionDocument(doc, "index", "index_name", "1");
            writeDocToBulk(bulk, doc);
            createDocument(doc, 1);
            writeDocToBulk(bulk, doc);
            createActionDocument(doc, "index", "index_name", "2");
            writeDocToBulk(bulk, doc);
            createDocument(doc, 2);
            writeDocToBulk(bulk, doc);
            firstBulkRequest.setEntity(new ByteArrayEntity(bulk.toByteArray()));
        }

        final Response indexSuccessFul = getRestClient().performRequest(firstBulkRequest);
        assertThat(indexSuccessFul.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));

        updateClusterSettings(Settings.builder().put(IncrementalBulkService.INCREMENTAL_BULK.getKey(), false));

        internalCluster().getInstances(IncrementalBulkService.class).forEach(i -> i.setForTests(false));

        try {
            sendLargeBulk();
        } finally {
            internalCluster().getInstances(IncrementalBulkService.class).forEach(i -> i.setForTests(true));
            updateClusterSettings(Settings.builder().put(IncrementalBulkService.INCREMENTAL_BULK.getKey(), (String) null));
        }
    }

    public void testMalformedActionLineBulk() throws IOException {
        Request createRequest = new Request("PUT", "/index_name");
        createRequest.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": 1,
                  "number_of_replicas": 1,
                  "write.wait_for_active_shards": 2
                }
              }
            }""");
        final Response indexCreatedResponse = getRestClient().performRequest(createRequest);
        assertThat(indexCreatedResponse.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));

        Request bulkRequest = new Request("POST", "/index_name/_bulk");
        bulkRequest.setOptions(options);

        try (ByteArrayOutputStream bulk = new ByteArrayOutputStream(); ByteArrayOutputStream doc = new ByteArrayOutputStream()) {
            createActionDocument(doc, "index", "index_name", null);
            writeDocToBulk(bulk, doc);
            createDocument(doc, 1);
            writeDocToBulk(bulk, doc);
            try (XContentBuilder builder = XContentFactory.contentBuilder(xContentLengthPrefixedStreamingType.xContentType(), doc)) {
                builder.startObject();
                builder.endObject();
            }
            writeDocToBulk(bulk, doc);
            bulkRequest.setEntity(new ByteArrayEntity(bulk.toByteArray()));
        }

        expectThrows(ResponseException.class, () -> getRestClient().performRequest(bulkRequest));
    }

    @SuppressWarnings("unchecked")
    private void sendLargeBulk() throws IOException {
        Request bulkRequest = new Request("POST", "/index_name/_bulk");
        bulkRequest.setOptions(options);
        int updates = 0;
        try (ByteArrayOutputStream bulk = new ByteArrayOutputStream(); ByteArrayOutputStream doc = new ByteArrayOutputStream()) {
            createActionDocument(doc, "delete", "index_name", "1");
            writeDocToBulk(bulk, doc);
            for (int i = 0; i < 1000; i++) {
                createActionDocument(doc, "index", "index_name", null);
                writeDocToBulk(bulk, doc);
                createDocument(doc, i);
                writeDocToBulk(bulk, doc);
                if (randomBoolean() && randomBoolean() && randomBoolean() && randomBoolean()) {
                    ++updates;
                    createActionDocument(doc, "update", "index_name", "2");
                    writeDocToBulk(bulk, doc);
                    createUpdateDocument(doc, i);
                    writeDocToBulk(bulk, doc);
                }
            }
            bulkRequest.setEntity(new ByteArrayEntity(bulk.toByteArray()));
        }

        final Response bulkResponse = getRestClient().performRequest(bulkRequest);
        assertThat(bulkResponse.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));
        Map<String, Object> responseMap = XContentHelper.convertToMap(
            returnXContentType.xContent(),
            bulkResponse.getEntity().getContent(),
            true
        );

        assertFalse((Boolean) responseMap.get("errors"));
        assertThat(((List<Object>) responseMap.get("items")).size(), equalTo(1001 + updates));
    }

    private void createActionDocument(OutputStream doc, String action, String indexName, String id) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentLengthPrefixedStreamingType.xContentType(), doc)) {
            builder.startObject();
            builder.startObject(action);
            builder.field("_index", indexName);
            if (id != null) {
                builder.field("_id", id);
            }
            builder.endObject();
            builder.endObject();
        }
    }

    private void createDocument(OutputStream doc, int value) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentLengthPrefixedStreamingType.xContentType(), doc)) {
            builder.startObject();
            builder.field("field", value);
            builder.endObject();
        }
    }

    private void createUpdateDocument(OutputStream doc, int value) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentLengthPrefixedStreamingType.xContentType(), doc)) {
            builder.startObject();
            builder.startObject("doc");
            builder.field("field", value);
            builder.endObject();
            builder.endObject();
        }
    }

    private void writeDocToBulk(OutputStream bulk, ByteArrayOutputStream doc) throws IOException {
        new DataOutputStream(bulk).writeInt(doc.size());
        doc.writeTo(bulk);
        doc.reset();
    }
}
