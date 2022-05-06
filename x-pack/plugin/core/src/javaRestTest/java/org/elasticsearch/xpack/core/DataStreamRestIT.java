/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class DataStreamRestIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password"));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    public void testDSXpackInfo() {
        Map<?, ?> features = (Map<?, ?>) getLocation("/_xpack").get("features");
        assertNotNull(features);
        Map<?, ?> dataStreams = (Map<?, ?>) features.get("data_streams");
        assertNotNull(dataStreams);
        assertTrue((boolean) dataStreams.get("available"));
        assertTrue((boolean) dataStreams.get("enabled"));
    }

    public void testDSXpackUsage() throws Exception {
        Map<?, ?> dataStreams = (Map<?, ?>) getLocation("/_xpack/usage").get("data_streams");
        assertNotNull(dataStreams);
        assertTrue((boolean) dataStreams.get("available"));
        assertTrue((boolean) dataStreams.get("enabled"));
        assertThat(dataStreams.get("data_streams"), anyOf(equalTo(null), equalTo(0)));

        assertBusy(() -> {
            Map<?, ?> logsTemplate = (Map<?, ?>) ((List<?>) getLocation("/_index_template/logs").get("index_templates")).get(0);
            assertThat(logsTemplate, notNullValue());
            assertThat(logsTemplate.get("name"), equalTo("logs"));
            assertThat(((Map<?, ?>) logsTemplate.get("index_template")).get("data_stream"), notNullValue());
        });

        // Create a data stream
        Request indexRequest = new Request("POST", "/logs-mysql-default/_doc");
        indexRequest.setJsonEntity("{\"@timestamp\": \"2020-01-01\"}");
        client().performRequest(indexRequest);

        // Roll over the data stream
        Request rollover = new Request("POST", "/logs-mysql-default/_rollover");
        client().performRequest(rollover);

        dataStreams = (Map<?, ?>) getLocation("/_xpack/usage").get("data_streams");
        assertNotNull(dataStreams);
        assertTrue((boolean) dataStreams.get("available"));
        assertTrue((boolean) dataStreams.get("enabled"));
        assertThat("got: " + dataStreams, dataStreams.get("data_streams"), equalTo(1));
        assertThat("got: " + dataStreams, dataStreams.get("indices_count"), equalTo(2));
    }

    Map<String, Object> getLocation(String path) {
        try {
            Response executeRepsonse = client().performRequest(new Request("GET", path));
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY,
                    EntityUtils.toByteArray(executeRepsonse.getEntity())
                )
            ) {
                return parser.map();
            }
        } catch (Exception e) {
            fail("failed to execute GET request to " + path + " - got: " + e);
            throw new RuntimeException(e);
        }
    }
}
