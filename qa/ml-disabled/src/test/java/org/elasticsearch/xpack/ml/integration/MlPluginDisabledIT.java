/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.ml.MlPlugin;

import java.util.Collections;

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
        xContentBuilder.field("job_id", "foo");
        xContentBuilder.field("description", "Analysis of response time by airline");

        xContentBuilder.startObject("analysis_config");
        xContentBuilder.field("bucket_span", 3600);
        xContentBuilder.startArray("detectors");
        xContentBuilder.startObject();
        xContentBuilder.field("function", "metric");
        xContentBuilder.field("field_name", "responsetime");
        xContentBuilder.field("by_field_name", "airline");
        xContentBuilder.endObject();
        xContentBuilder.endArray();
        xContentBuilder.endObject();

        xContentBuilder.startObject("data_description");
        xContentBuilder.field("format", "JSON");
        xContentBuilder.field("time_field", "time");
        xContentBuilder.field("time_format", "epoch");
        xContentBuilder.endObject();
        xContentBuilder.endObject();

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest("put",
                MlPlugin.BASE_PATH + "anomaly_detectors/foo", Collections.emptyMap(), new StringEntity(xContentBuilder.string())));
        assertThat(exception.getMessage(), containsString("No handler found for uri [/_xpack/ml/anomaly_detectors/foo] and method [PUT]"));
    }
}
