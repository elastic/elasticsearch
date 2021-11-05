/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.logging.DeprecatedMessage.KEY_FIELD_NAME;
import static org.elasticsearch.xpack.deprecation.DeprecationTestUtils.DATA_STREAM_NAME;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests that deprecation message on startup creates a deprecation data stream
 */
public class EarlyDeprecationIndexingIT extends ESRestTestCase {

    /**
     * In EarlyDeprecationTestPlugin#onNodeStarted we simulate a very early deprecation that can happen before the template is loaded
     * The indexing has to be delayed until templates are loaded.
     * This test confirms by checking the settings that a data stream is created (not an index with defaults)
     * and that an early deprecation warning is not lost
     */
    public void testEarlyDeprecationIsIndexedAfterTemplateIsLoaded() throws Exception {

        assertBusy(() -> {
            Response response = getIndexSettings();
            ObjectMapper mapper = new ObjectMapper();

            final JsonNode jsonNode = mapper.readTree(response.getEntity().getContent());
            assertThat(jsonNode.fieldNames().next(), startsWith(".ds-" + DATA_STREAM_NAME));

            final JsonNode settings = jsonNode.elements().next();
            final JsonNode index = settings.at("/settings/index");
            assertThat(index.at("/lifecycle/name").asText(), equalTo(".deprecation-indexing-ilm-policy"));
            assertThat(index.get("auto_expand_replicas").asText(), equalTo("0-1"));
            assertThat(index.get("hidden").asText(), equalTo("true"));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client());
            logger.warn(documents);
            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasEntry(KEY_FIELD_NAME, "early_deprecation"),
                        hasEntry("message", "Early deprecation emitted after node is started up")
                    )
                )
            );
        }, 30, TimeUnit.SECONDS);
    }

    private Response getIndexSettings() throws Exception {
        try {
            Response response = client().performRequest(new Request("GET", "/" + DATA_STREAM_NAME + "/_settings"));
            assertOK(response);
            return response;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Builds a REST client that will tolerate warnings in the response headers. The default
     * is to throw an exception.
     */
    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setStrictDeprecationMode(false);
        return builder.build();
    }
}
