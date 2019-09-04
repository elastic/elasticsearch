/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.dataframe.DataFrameNamedXContentProvider;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.dataframe.DeleteDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformStatsRequest;
import org.elasticsearch.client.dataframe.PreviewDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.PutDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StopDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.UpdateDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfigTests;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfigUpdate;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfigUpdateTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.client.dataframe.GetDataFrameTransformRequest.ALLOW_NO_MATCH;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class DataFrameRequestConvertersTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.addAll(new DataFrameNamedXContentProvider().getNamedXContentParsers());

        return new NamedXContentRegistry(namedXContents);
    }

    public void testPutDataFrameTransform() throws IOException {
        PutDataFrameTransformRequest putRequest = new PutDataFrameTransformRequest(
                DataFrameTransformConfigTests.randomDataFrameTransformConfig());
        Request request = DataFrameRequestConverters.putDataFrameTransform(putRequest);
        assertThat(request.getParameters(), not(hasKey("defer_validation")));
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_data_frame/transforms/" + putRequest.getConfig().getId()));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            DataFrameTransformConfig parsedConfig = DataFrameTransformConfig.PARSER.apply(parser, null);
            assertThat(parsedConfig, equalTo(putRequest.getConfig()));
        }
        putRequest.setDeferValidation(true);
        request = DataFrameRequestConverters.putDataFrameTransform(putRequest);
        assertThat(request.getParameters(), hasEntry("defer_validation", Boolean.toString(putRequest.getDeferValidation())));
    }

    public void testUpdateDataFrameTransform() throws IOException {
        String transformId = randomAlphaOfLength(10);
        UpdateDataFrameTransformRequest updateDataFrameTransformRequest = new UpdateDataFrameTransformRequest(
            DataFrameTransformConfigUpdateTests.randomDataFrameTransformConfigUpdate(),
            transformId);
        Request request = DataFrameRequestConverters.updateDataFrameTransform(updateDataFrameTransformRequest);
        assertThat(request.getParameters(), not(hasKey("defer_validation")));
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_data_frame/transforms/" + transformId + "/_update"));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            DataFrameTransformConfigUpdate parsedConfig = DataFrameTransformConfigUpdate.fromXContent(parser);
            assertThat(parsedConfig, equalTo(updateDataFrameTransformRequest.getUpdate()));
        }
        updateDataFrameTransformRequest.setDeferValidation(true);
        request = DataFrameRequestConverters.updateDataFrameTransform(updateDataFrameTransformRequest);
        assertThat(request.getParameters(),
            hasEntry("defer_validation", Boolean.toString(updateDataFrameTransformRequest.getDeferValidation())));
    }

    public void testDeleteDataFrameTransform() {
        DeleteDataFrameTransformRequest deleteRequest = new DeleteDataFrameTransformRequest("foo");
        Request request = DataFrameRequestConverters.deleteDataFrameTransform(deleteRequest);

        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_data_frame/transforms/foo"));

        assertThat(request.getParameters(), not(hasKey("force")));

        deleteRequest.setForce(true);
        request = DataFrameRequestConverters.deleteDataFrameTransform(deleteRequest);

        assertThat(request.getParameters(), hasEntry("force", "true"));
    }

    public void testStartDataFrameTransform() {
        String id = randomAlphaOfLength(10);
        TimeValue timeValue = null;
        if (randomBoolean()) {
            timeValue = TimeValue.parseTimeValue(randomTimeValue(), "timeout");
        }
        StartDataFrameTransformRequest startRequest = new StartDataFrameTransformRequest(id, timeValue);

        Request request = DataFrameRequestConverters.startDataFrameTransform(startRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_data_frame/transforms/" + startRequest.getId() + "/_start"));

        if (timeValue != null) {
            assertTrue(request.getParameters().containsKey("timeout"));
            assertEquals(startRequest.getTimeout(), TimeValue.parseTimeValue(request.getParameters().get("timeout"), "timeout"));
        } else {
            assertFalse(request.getParameters().containsKey("timeout"));
        }
    }

    public void testStopDataFrameTransform() {
        String id = randomAlphaOfLength(10);
        Boolean waitForCompletion = null;
        if (randomBoolean()) {
            waitForCompletion = randomBoolean();
        }
        TimeValue timeValue = null;
        if (randomBoolean()) {
            timeValue = TimeValue.parseTimeValue(randomTimeValue(), "timeout");
        }
        StopDataFrameTransformRequest stopRequest = new StopDataFrameTransformRequest(id, waitForCompletion, timeValue);

        Request request = DataFrameRequestConverters.stopDataFrameTransform(stopRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_data_frame/transforms/" + stopRequest.getId() + "/_stop"));

        if (waitForCompletion != null) {
            assertTrue(request.getParameters().containsKey("wait_for_completion"));
            assertEquals(stopRequest.getWaitForCompletion(), Boolean.parseBoolean(request.getParameters().get("wait_for_completion")));
        } else {
            assertFalse(request.getParameters().containsKey("wait_for_completion"));
        }

        if (timeValue != null) {
            assertTrue(request.getParameters().containsKey("timeout"));
            assertEquals(stopRequest.getTimeout(), TimeValue.parseTimeValue(request.getParameters().get("timeout"), "timeout"));
        } else {
            assertFalse(request.getParameters().containsKey("timeout"));
        }

        assertFalse(request.getParameters().containsKey(ALLOW_NO_MATCH));
        stopRequest.setAllowNoMatch(randomBoolean());
        request = DataFrameRequestConverters.stopDataFrameTransform(stopRequest);
        assertEquals(stopRequest.getAllowNoMatch(), Boolean.parseBoolean(request.getParameters().get(ALLOW_NO_MATCH)));
    }

    public void testPreviewDataFrameTransform() throws IOException {
        PreviewDataFrameTransformRequest previewRequest = new PreviewDataFrameTransformRequest(
                DataFrameTransformConfigTests.randomDataFrameTransformConfig());
        Request request = DataFrameRequestConverters.previewDataFrameTransform(previewRequest);

        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_data_frame/transforms/_preview"));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            DataFrameTransformConfig parsedConfig = DataFrameTransformConfig.PARSER.apply(parser, null);
            assertThat(parsedConfig, equalTo(previewRequest.getConfig()));
        }
    }

    public void testGetDataFrameTransformStats() {
        GetDataFrameTransformStatsRequest getStatsRequest = new GetDataFrameTransformStatsRequest("foo");
        Request request = DataFrameRequestConverters.getDataFrameTransformStats(getStatsRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_data_frame/transforms/foo/_stats"));

        assertFalse(request.getParameters().containsKey("from"));
        assertFalse(request.getParameters().containsKey("size"));
        assertFalse(request.getParameters().containsKey(ALLOW_NO_MATCH));

        getStatsRequest.setPageParams(new PageParams(0, null));
        request = DataFrameRequestConverters.getDataFrameTransformStats(getStatsRequest);
        assertThat(request.getParameters(), hasEntry("from", "0"));
        assertEquals(null, request.getParameters().get("size"));

        getStatsRequest.setPageParams(new PageParams(null, 50));
        request = DataFrameRequestConverters.getDataFrameTransformStats(getStatsRequest);
        assertEquals(null, request.getParameters().get("from"));
        assertThat(request.getParameters(), hasEntry("size", "50"));

        getStatsRequest.setPageParams(new PageParams(0, 10));
        request = DataFrameRequestConverters.getDataFrameTransformStats(getStatsRequest);
        assertThat(request.getParameters(), allOf(hasEntry("from", "0"), hasEntry("size", "10")));

        getStatsRequest.setAllowNoMatch(false);
        request = DataFrameRequestConverters.getDataFrameTransformStats(getStatsRequest);
        assertThat(request.getParameters(), hasEntry("allow_no_match", "false"));
    }

    public void testGetDataFrameTransform() {
        GetDataFrameTransformRequest getRequest = new GetDataFrameTransformRequest("bar");
        Request request = DataFrameRequestConverters.getDataFrameTransform(getRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_data_frame/transforms/bar"));

        assertFalse(request.getParameters().containsKey("from"));
        assertFalse(request.getParameters().containsKey("size"));
        assertFalse(request.getParameters().containsKey(ALLOW_NO_MATCH));

        getRequest.setPageParams(new PageParams(0, null));
        request = DataFrameRequestConverters.getDataFrameTransform(getRequest);
        assertThat(request.getParameters(), hasEntry("from", "0"));
        assertEquals(null, request.getParameters().get("size"));

        getRequest.setPageParams(new PageParams(null, 50));
        request = DataFrameRequestConverters.getDataFrameTransform(getRequest);
        assertEquals(null, request.getParameters().get("from"));
        assertThat(request.getParameters(), hasEntry("size", "50"));

        getRequest.setPageParams(new PageParams(0, 10));
        request = DataFrameRequestConverters.getDataFrameTransform(getRequest);
        assertThat(request.getParameters(), allOf(hasEntry("from", "0"), hasEntry("size", "10")));

        getRequest.setAllowNoMatch(false);
        request = DataFrameRequestConverters.getDataFrameTransform(getRequest);
        assertThat(request.getParameters(), hasEntry("allow_no_match", "false"));
    }

    public void testGetDataFrameTransform_givenMulitpleIds() {
        GetDataFrameTransformRequest getRequest = new GetDataFrameTransformRequest("foo", "bar", "baz");
        Request request = DataFrameRequestConverters.getDataFrameTransform(getRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_data_frame/transforms/foo,bar,baz"));
    }
}
