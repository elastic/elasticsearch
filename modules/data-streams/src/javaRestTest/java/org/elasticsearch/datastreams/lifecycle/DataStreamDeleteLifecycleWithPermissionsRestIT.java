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
import org.elasticsearch.client.RestClient;
import org.junit.Before;

import java.io.IOException;

public class DataStreamDeleteLifecycleWithPermissionsRestIT extends DataStreamLifecyclePermissionsTestCase {

    @Before
    public void resetLifecycle() throws IOException {
        Request request = new Request("PUT", "_data_stream/_all/_lifecycle");
        request.setJsonEntity("{}");
        assertAcknowledged(client().performRequest(request));
    }

    public void testPrivilegedUserCanDeleteDataStreamLifecycle() throws Exception {
        Request deleteLifecycleRequest = new Request("DELETE", "_data_stream/" + randomFrom("_all", "*", DATA_STREAM_NAME) + "/_lifecycle");
        makeRequest(client(), deleteLifecycleRequest, true);
    }

    public void testUnprivilegedUserCannotDeleteLifecycle() throws IOException {
        try (
            RestClient nonDataStreamLifecycleManagerClient = buildClient(
                restUnprivilegedClientSettings(),
                getClusterHosts().toArray(new HttpHost[0])
            )
        ) {
            Request deleteLifecycleRequest = new Request(
                "DELETE",
                "_data_stream/" + randomFrom("_all", "*", DATA_STREAM_NAME) + "/_lifecycle"
            );
            makeRequest(nonDataStreamLifecycleManagerClient, deleteLifecycleRequest, false);
        }
    }

    public void testPrivilegedUserCannotDeleteOtherDataStreamsLifecycle() throws IOException {
        try (
            RestClient dataStreamLifecycleManagerClient = buildClient(
                restPrivilegedClientSettings(),
                getClusterHosts().toArray(new HttpHost[0])
            )
        ) {
            String otherDataStreamName = "other-data-stream-lifecycle-test";
            createDataStreamAsAdmin(otherDataStreamName);
            makeRequest(
                dataStreamLifecycleManagerClient,
                new Request("DELETE", "_data_stream/" + otherDataStreamName + "/_lifecycle"),
                false
            );
        }
    }
}
