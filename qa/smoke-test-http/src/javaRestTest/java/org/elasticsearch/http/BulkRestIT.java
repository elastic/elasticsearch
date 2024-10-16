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
import org.apache.http.entity.ContentType;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 2, numClientNodes = 0)
public class BulkRestIT extends HttpSmokeTestCase {

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

    public void testBulkUriMatchingDoesNotMatchBulkCapabilitiesApi() throws IOException {
        Request request = new Request("GET", "/_capabilities?method=GET&path=%2F_bulk&capabilities=failure_store_status&pretty");
        Response response = getRestClient().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    public void testBulkMissingBody() throws IOException {
        Request request = new Request(randomBoolean() ? "POST" : "PUT", "/_bulk");
        request.setJsonEntity("");
        ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body is required"));
    }

    public void testBulkInvalidIndexNameString() throws IOException {
        Request request = new Request("POST", "/_bulk");

        byte[] bytes1 = "{\"create\":{\"_index\":\"".getBytes(StandardCharsets.UTF_8);
        byte[] bytes2 = new byte[] { (byte) 0xfe, (byte) 0xfe, (byte) 0xff, (byte) 0xff };
        byte[] bytes3 = "\",\"_id\":\"1\"}}\n{\"field\":1}\n\r\n".getBytes(StandardCharsets.UTF_8);
        byte[] bulkBody = new byte[bytes1.length + bytes2.length + bytes3.length];
        System.arraycopy(bytes1, 0, bulkBody, 0, bytes1.length);
        System.arraycopy(bytes2, 0, bulkBody, bytes1.length, bytes2.length);
        System.arraycopy(bytes3, 0, bulkBody, bytes1.length + bytes2.length, bytes3.length);

        request.setEntity(new ByteArrayEntity(bulkBody, ContentType.APPLICATION_JSON));

        ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(BAD_REQUEST.getStatus()));
        assertThat(responseException.getMessage(), containsString("could not parse bulk request body"));
        assertThat(responseException.getMessage(), containsString("json_parse_exception"));
        assertThat(responseException.getMessage(), containsString("Invalid UTF-8"));
    }

    public void testBulkRequestBodyImproperlyTerminated() throws IOException {
        Request request = new Request(randomBoolean() ? "POST" : "PUT", "/_bulk");
        // missing final line of the bulk body. cannot process
        request.setJsonEntity(
            "{\"index\":{\"_index\":\"index_name\",\"_id\":\"1\"}}\n"
                + "{\"field\":1}\n"
                + "{\"index\":{\"_index\":\"index_name\",\"_id\":\"2\"}"
        );
        ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("The bulk request must be terminated by a newline"));
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

        String bulkBody = "{\"index\":{\"_index\":\"index_name\",\"_id\":\"1\"}}\n"
            + "{\"field\":1}\n"
            + "{\"index\":{\"_index\":\"index_name\",\"_id\":\"2\"}}\n"
            + "{\"field\":1}\n"
            + "\r\n";

        firstBulkRequest.setJsonEntity(bulkBody);

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

        String bulkBody = "{\"index\":{\"_index\":\"index_name\",\"_id\":\"1\"}}\n"
            + "{\"field\":1}\n"
            + "{\"index\":{\"_index\":\"index_name\",\"_id\":\"2\"}}\n"
            + "{\"field\":1}\n"
            + "\r\n";

        firstBulkRequest.setJsonEntity(bulkBody);

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

        final StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{\"_index\":\"index_name\"}}\n");
        bulk.append("{\"field\":1}\n");
        bulk.append("{}\n");
        bulk.append("\r\n");

        bulkRequest.setJsonEntity(bulk.toString());

        expectThrows(ResponseException.class, () -> getRestClient().performRequest(bulkRequest));
    }

    @SuppressWarnings("unchecked")
    private static void sendLargeBulk() throws IOException {
        Request bulkRequest = new Request("POST", "/index_name/_bulk");

        final StringBuilder bulk = new StringBuilder();
        bulk.append("{\"delete\":{\"_index\":\"index_name\",\"_id\":\"1\"}}\n");
        int updates = 0;
        for (int i = 0; i < 1000; i++) {
            bulk.append("{\"index\":{\"_index\":\"index_name\"}}\n");
            bulk.append("{\"field\":").append(i).append("}\n");
            if (randomBoolean() && randomBoolean() && randomBoolean() && randomBoolean()) {
                ++updates;
                bulk.append("{\"update\":{\"_index\":\"index_name\",\"_id\":\"2\"}}\n");
                bulk.append("{\"doc\":{\"field\":").append(i).append("}}\n");
            }
        }
        bulk.append("\r\n");

        bulkRequest.setJsonEntity(bulk.toString());

        final Response bulkResponse = getRestClient().performRequest(bulkRequest);
        assertThat(bulkResponse.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));
        Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            bulkResponse.getEntity().getContent(),
            true
        );

        assertFalse((Boolean) responseMap.get("errors"));
        assertThat(((List<Object>) responseMap.get("items")).size(), equalTo(1001 + updates));
    }
}
