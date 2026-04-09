/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DataStreamUpdateLifecycleWithPermissionsRestIT extends DataStreamLifecyclePermissionsTestCase {

    public void testPrivilegedUserCanUpdateDataStreamLifecycle() throws Exception {
        Request explainLifecycleRequest = new Request("GET", "/" + randomFrom("_all", "*", index) + "/_lifecycle/explain");
        Request getLifecycleRequest = new Request("GET", "_data_stream/" + randomFrom("_all", "*", DATA_STREAM_NAME) + "/_lifecycle");
        Request putLifecycleRequest = new Request("PUT", "_data_stream/" + randomFrom("_all", "*", DATA_STREAM_NAME) + "/_lifecycle");
        putLifecycleRequest.setJsonEntity("{}");

        makeRequest(client(), explainLifecycleRequest, true);
        makeRequest(client(), getLifecycleRequest, true);
        makeRequest(client(), putLifecycleRequest, true);
    }

    public void testUnPrivilegedUserCannotUpdateLifecycle() throws IOException {
        try (
            RestClient nonDataStreamLifecycleManagerClient = buildClient(
                restUnprivilegedClientSettings(),
                getClusterHosts().toArray(new HttpHost[0])
            )
        ) {
            Request explainLifecycleRequest = new Request("GET", "/" + randomFrom("_all", "*", index) + "/_lifecycle/explain");
            Request getLifecycleRequest = new Request("GET", "_data_stream/" + randomFrom("_all", "*", DATA_STREAM_NAME) + "/_lifecycle");
            Request putLifecycleRequest = new Request("PUT", "_data_stream/" + randomFrom("_all", "*", DATA_STREAM_NAME) + "/_lifecycle");
            putLifecycleRequest.setJsonEntity("{}");
            makeRequest(nonDataStreamLifecycleManagerClient, explainLifecycleRequest, true);
            makeRequest(nonDataStreamLifecycleManagerClient, getLifecycleRequest, true);
            makeRequest(nonDataStreamLifecycleManagerClient, putLifecycleRequest, false);
        }
    }

    @SuppressWarnings("unchecked")
    public void testPrivilegedUserCannotUpdateOtherDataStreamsLifecycle() throws IOException {
        try (
            RestClient dataStreamLifecycleManagerClient = buildClient(
                restPrivilegedClientSettings(),
                getClusterHosts().toArray(new HttpHost[0])
            )
        ) {
            // Now test that the user who has the manage_data_stream_lifecycle privilege on data-stream-lifecycle-* data streams cannot
            // manage other data streams:
            String otherDataStreamName = "other-data-stream-lifecycle-test";
            createDataStreamAsAdmin(otherDataStreamName);
            Response getOtherDataStreamResponse = adminClient().performRequest(new Request("GET", "/_data_stream/" + otherDataStreamName));
            final List<Map<String, Object>> otherNodes = ObjectPath.createFromResponse(getOtherDataStreamResponse).evaluate("data_streams");
            String otherIndex = (String) ((List<Map<String, Object>>) otherNodes.get(0).get("indices")).get(0).get("index_name");
            Request putOtherLifecycleRequest = new Request("PUT", "_data_stream/" + otherDataStreamName + "/_lifecycle");
            putOtherLifecycleRequest.setJsonEntity("{}");
            makeRequest(dataStreamLifecycleManagerClient, new Request("GET", "/" + otherIndex + "/_lifecycle/explain"), false);
            makeRequest(dataStreamLifecycleManagerClient, new Request("GET", "_data_stream/" + otherDataStreamName + "/_lifecycle"), false);
            makeRequest(dataStreamLifecycleManagerClient, putOtherLifecycleRequest, false);
        }
    }
}
