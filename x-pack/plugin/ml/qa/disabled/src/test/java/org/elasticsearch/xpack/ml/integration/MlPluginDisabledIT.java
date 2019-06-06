/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class MlPluginDisabledIT extends ESRestTestCase {

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

        Request request = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/foo");
        request.setJsonEntity(Strings.toString(xContentBuilder));
        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getMessage(), containsString("method [PUT]"));
        assertThat(exception.getMessage(), containsString("URI [/_ml/anomaly_detectors/foo]"));
        assertThat(exception.getMessage(), containsString("400 Bad Request"));
    }
}
