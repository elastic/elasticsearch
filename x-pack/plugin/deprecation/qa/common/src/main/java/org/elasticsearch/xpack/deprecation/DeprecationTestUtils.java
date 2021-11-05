/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;

public class DeprecationTestUtils {
    /**
     * Same as <code>DeprecationIndexingAppender#DEPRECATION_MESSAGES_DATA_STREAM</code>, but that class isn't visible from here.
     */
    public static final String DATA_STREAM_NAME = ".logs-deprecation.elasticsearch-default";

    static List<Map<String, Object>> getIndexedDeprecations(RestClient client) throws IOException {
        Response response;
        try {
            client.performRequest(new Request("POST", "/" + DATA_STREAM_NAME + "/_refresh?ignore_unavailable=true"));
            response = client.performRequest(new Request("GET", "/" + DATA_STREAM_NAME + "/_search"));
        } catch (Exception e) {
            // It can take a moment for the index to be created. If it doesn't exist then the client
            // throws an exception. Translate it into an assertion error so that assertBusy() will
            // continue trying.
            throw new AssertionError(e);
        }
        ESRestTestCase.assertOK(response);

        ObjectMapper mapper = new ObjectMapper();
        final JsonNode jsonNode = mapper.readTree(response.getEntity().getContent());

        final int hits = jsonNode.at("/hits/total/value").intValue();
        Assert.assertThat(hits, greaterThan(0));

        List<Map<String, Object>> documents = new ArrayList<>();

        for (int i = 0; i < hits; i++) {
            final JsonNode hit = jsonNode.at("/hits/hits/" + i + "/_source");

            final Map<String, Object> document = new HashMap<>();
            hit.fields().forEachRemaining(entry -> document.put(entry.getKey(), entry.getValue().textValue()));

            documents.add(document);
        }
        return documents;
    }
}
