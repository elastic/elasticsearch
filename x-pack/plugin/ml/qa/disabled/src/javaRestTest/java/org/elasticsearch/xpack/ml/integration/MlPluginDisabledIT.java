/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MlPluginDisabledIT extends ESRestTestCase {

    private static final String BASE_PATH = "/_ml/";

    /**
     * Check that when the ml plugin is disabled, you cannot create a job as the
     * rest handler is not registered
     */
    public void testActionsFail() throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder();
        xContentBuilder.startObject();
        {
            xContentBuilder.field("actions-fail-job", "foo");
            xContentBuilder.field("description", "Analysis of response time by airline");

            xContentBuilder.startObject("analysis_config");
            {
                xContentBuilder.field("bucket_span", "3600s");
                xContentBuilder.startArray("detectors");
                {
                    xContentBuilder.startObject();
                    {
                        xContentBuilder.field("function", "metric");
                        xContentBuilder.field("field_name", "responsetime");
                        xContentBuilder.field("by_field_name", "airline");
                    }
                    xContentBuilder.endObject();
                }
                xContentBuilder.endArray();
            }
            xContentBuilder.endObject();

            xContentBuilder.startObject("data_description");
            {
                xContentBuilder.field("format", "xcontent");
                xContentBuilder.field("time_field", "time");
                xContentBuilder.field("time_format", "epoch");
            }
            xContentBuilder.endObject();
        }
        xContentBuilder.endObject();

        Request request = new Request("PUT", BASE_PATH + "anomaly_detectors/foo");
        request.setJsonEntity(Strings.toString(xContentBuilder));
        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getMessage(), containsString("method [PUT]"));
        assertThat(exception.getMessage(), containsString("URI [/_ml/anomaly_detectors/foo]"));
        assertThat(exception.getMessage(), containsString("400 Bad Request"));
    }

    public void testMlFeatureReset() throws IOException {
        Request request = new Request("POST", "/_features/_reset");
        assertOK(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    public void testAllNodesHaveMlConfigVersionAttribute() throws IOException {
        Request request = new Request("GET", "/_nodes");
        Response response = assertOK(client().performRequest(request));
        var nodesMap = (Map<String, Object>) entityAsMap(response).get("nodes");
        assertThat(nodesMap, is(aMapWithSize(greaterThanOrEqualTo(1))));
        for (var nodeObj : nodesMap.values()) {
            var nodeMap = (Map<String, Object>) nodeObj;
            // We do not expect any specific version. The only important assertion is that the attribute exists.
            assertThat(XContentMapValues.extractValue(nodeMap, "attributes", "ml.config_version"), is(notNullValue()));
        }
    }
}
