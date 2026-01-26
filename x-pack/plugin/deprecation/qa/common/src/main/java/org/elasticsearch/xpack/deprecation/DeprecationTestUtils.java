/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.logging.DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME;

public class DeprecationTestUtils {
    /**
     * Same as <code>DeprecationIndexingAppender#DEPRECATION_MESSAGES_DATA_STREAM</code>, but that class isn't visible from here.
     */
    public static final String DATA_STREAM_NAME = ".logs-elasticsearch.deprecation-default";

    static List<Map<String, Object>> getIndexedDeprecations(RestClient client) throws IOException {
        return getIndexedDeprecations(client, null);
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> getIndexedDeprecations(RestClient client, String xOpaqueId) throws IOException {
        Response response;
        try {
            client.performRequest(new Request("POST", "/" + DATA_STREAM_NAME + "/_refresh?ignore_unavailable=true"));
            String query = xOpaqueId == null ? "" : "?q=" + X_OPAQUE_ID_FIELD_NAME + ":" + xOpaqueId;
            response = client.performRequest(new Request("GET", "/" + DATA_STREAM_NAME + "/_search" + query));
        } catch (Exception e) {
            // It can take a moment for the index to be created. If it doesn't exist then the client
            // throws an exception. Translate it into an assertion error so that assertBusy() will
            // continue trying.
            throw new AssertionError(e);
        }
        ESRestTestCase.assertOK(response);

        final Map<String, Object> stringObjectMap = ESRestTestCase.entityAsMap(response);
        return (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits._source", stringObjectMap);
    }
}
