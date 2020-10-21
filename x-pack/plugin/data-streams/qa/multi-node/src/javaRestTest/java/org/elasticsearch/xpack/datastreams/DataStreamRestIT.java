/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.datastreams;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamRestIT extends ESRestTestCase {

    @SuppressWarnings("unchecked")
    public void testDSXpackInfo() {
        Map<String, Object> features = (Map<String, Object>) getLocation("/_xpack").get("features");
        assertNotNull(features);
        Map<String, Object> dataStreams = (Map<String, Object>) features.get("data_streams");
        assertNotNull(dataStreams);
        assertTrue((boolean) dataStreams.get("available"));
        assertTrue((boolean) dataStreams.get("enabled"));
    }

    @SuppressWarnings("unchecked")
    public void testDSXpackUsage() throws Exception {
        Map<String, Object> dataStreams = (Map<String, Object>) getLocation("/_xpack/usage").get("data_streams");
        assertNotNull(dataStreams);
        assertTrue((boolean) dataStreams.get("available"));
        assertTrue((boolean) dataStreams.get("enabled"));
        assertThat(dataStreams.get("data_streams"), anyOf(equalTo(null), equalTo(0)));

        // Create a data stream
        Request indexRequest = new Request("POST", "/logs-mysql-default/_doc");
        indexRequest.setJsonEntity("{\"@timestamp\": \"2020-01-01\"}");
        client().performRequest(indexRequest);

        // Roll over the data stream
        Request rollover = new Request("POST", "/logs-mysql-default/_rollover");
        client().performRequest(rollover);

        dataStreams = (Map<String, Object>) getLocation("/_xpack/usage").get("data_streams");
        assertNotNull(dataStreams);
        assertTrue((boolean) dataStreams.get("available"));
        assertTrue((boolean) dataStreams.get("enabled"));
        assertThat("got: " + dataStreams, dataStreams.get("data_streams"), equalTo(1));
        assertThat("got: " + dataStreams, dataStreams.get("indices_count"), equalTo(2));
    }

    public Map<String, Object> getLocation(String path) {
        try {
            Response executeRepsonse = client().performRequest(new Request("GET", path));
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
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
