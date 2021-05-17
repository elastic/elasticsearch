/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.test.rest;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class LogstashSystemIndexIT extends ESRestTestCase {
    static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    public void testPipelineCRUD() throws Exception {
        // put pipeline
        final String pipelineJson = getPipelineJson();
        createPipeline("test_pipeline", pipelineJson);

        // get pipeline
        Request getRequest = new Request("GET", "/_logstash/pipeline/test_pipeline");
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(getResponse.getEntity()), containsString(pipelineJson));

        // update
        final String updatedJson = getPipelineJson("2020-03-09T15:42:35.229Z");
        Request putRequest = new Request("PUT", "/_logstash/pipeline/test_pipeline");
        putRequest.setJsonEntity(updatedJson);
        Response putResponse = client().performRequest(putRequest);
        assertThat(putResponse.getStatusLine().getStatusCode(), is(200));

        getRequest = new Request("GET", "/_logstash/pipeline/test_pipeline");
        getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(getResponse.getEntity()), containsString(updatedJson));

        // delete
        Request deleteRequest = new Request("DELETE", "/_logstash/pipeline/test_pipeline");
        Response deleteResponse = client().performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));

        // list is now empty
        Request listAll = new Request("GET", "/_logstash/pipeline");
        Response listAllResponse = client().performRequest(listAll);
        assertThat(listAllResponse.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(listAllResponse.getEntity()), is("{}"));
    }

    public void testGetNonExistingPipeline() {
        Request getRequest = new Request("GET", "/_logstash/pipeline/test_pipeline");
        ResponseException re = expectThrows(ResponseException.class, () -> client().performRequest(getRequest));
        Response getResponse = re.getResponse();
        assertThat(getResponse.getStatusLine().getStatusCode(), is(404));
    }

    public void testDeleteNonExistingPipeline() {
        Request deleteRequest = new Request("DELETE", "/_logstash/pipeline/test_pipeline");
        ResponseException re = expectThrows(ResponseException.class, () -> client().performRequest(deleteRequest));
        Response getResponse = re.getResponse();
        assertThat(getResponse.getStatusLine().getStatusCode(), is(404));
    }

    public void testMultiplePipelines() throws IOException {
        final int numPipelines = scaledRandomIntBetween(2, 2000);
        final List<String> ids = new ArrayList<>(numPipelines);
        final String pipelineJson = getPipelineJson();
        for (int i = 0; i < numPipelines; i++) {
            final String id = "id" + i;
            ids.add(id);
            createPipeline(id, pipelineJson);
        }

        // test mget-like
        final int numToGet = scaledRandomIntBetween(2, Math.min(100, numPipelines)); // limit number to avoid HTTP line length issues
        final List<String> mgetIds = randomSubsetOf(numToGet, ids);
        final String path = "/_logstash/pipeline/" + Strings.collectionToCommaDelimitedString(mgetIds);
        Request getRequest = new Request("GET", path);
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        Map<String, Object> responseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(getResponse.getEntity()),
            false
        );

        for (String id : mgetIds) {
            assertTrue(responseMap.containsKey(id));
        }

        refreshAllIndices();

        // list without any IDs
        Request listAll = new Request("GET", "/_logstash/pipeline");
        Response listAllResponse = client().performRequest(listAll);
        assertThat(listAllResponse.getStatusLine().getStatusCode(), is(200));
        Map<String, Object> listResponseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(listAllResponse.getEntity()),
            false
        );
        for (String id : ids) {
            assertTrue(listResponseMap.containsKey(id));
        }
        assertThat(listResponseMap.size(), is(ids.size()));
    }

    private void createPipeline(String id, String json) throws IOException {
        Request putRequest = new Request("PUT", "/_logstash/pipeline/" + id);
        putRequest.setJsonEntity(json);
        Response putResponse = client().performRequest(putRequest);
        assertThat(putResponse.getStatusLine().getStatusCode(), is(201));
    }

    private String getPipelineJson() throws IOException {
        return getPipelineJson("2020-03-09T15:42:30.229Z");
    }

    private String getPipelineJson(String date) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.field("description", "test pipeline");
                builder.field("last_modified", date);
                builder.startObject("pipeline_metadata");
                {
                    builder.field("version", 1);
                    builder.field("type", "logstash_pipeline");
                }
                builder.endObject();
                builder.field("username", "john.doe");
                builder.field("pipeline", "\"input\": {},\n \"filter\": {},\n \"output\": {}\n");
                builder.startObject("pipeline_settings");
                {
                    builder.field("pipeline.batch.delay", 50);
                    builder.field("pipeline.batch.size", 125);
                    builder.field("pipeline.workers", 1);
                    builder.field("queue.checkpoint.writes", 1024);
                    builder.field("queue.max_bytes", "1gb");
                    builder.field("queue.type", "memory");
                }
                builder.endObject();
            }
            builder.endObject();
            return BytesReference.bytes(builder).utf8ToString();
        }
    }
}
