/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.logstash;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class LogstashSystemIndexIT extends ESRestTestCase {

    public void testTemplateIsPut() throws Exception {
        assertBusy(
            () -> assertThat(
                client().performRequest(new Request("HEAD", "/_template/.logstash-pipeline")).getStatusLine().getStatusCode(),
                is(200)
            )
        );
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

        // TODO need to update this after system indices are truly system indices to enable refresh
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
