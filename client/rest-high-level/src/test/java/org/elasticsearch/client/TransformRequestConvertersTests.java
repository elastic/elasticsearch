/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.transform.DeleteTransformRequest;
import org.elasticsearch.client.transform.GetTransformRequest;
import org.elasticsearch.client.transform.GetTransformStatsRequest;
import org.elasticsearch.client.transform.PreviewTransformRequest;
import org.elasticsearch.client.transform.PreviewTransformRequestTests;
import org.elasticsearch.client.transform.PutTransformRequest;
import org.elasticsearch.client.transform.StartTransformRequest;
import org.elasticsearch.client.transform.StopTransformRequest;
import org.elasticsearch.client.transform.TransformNamedXContentProvider;
import org.elasticsearch.client.transform.UpdateTransformRequest;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.TransformConfigTests;
import org.elasticsearch.client.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.client.transform.transforms.TransformConfigUpdateTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.client.transform.GetTransformRequest.ALLOW_NO_MATCH;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TransformRequestConvertersTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.addAll(new TransformNamedXContentProvider().getNamedXContentParsers());

        return new NamedXContentRegistry(namedXContents);
    }

    public void testPutDataFrameTransform() throws IOException {
        PutTransformRequest putRequest = new PutTransformRequest(
                TransformConfigTests.randomTransformConfig());
        Request request = TransformRequestConverters.putTransform(putRequest);
        assertThat(request.getParameters(), not(hasKey("defer_validation")));
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_transform/" + putRequest.getConfig().getId()));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            TransformConfig parsedConfig = TransformConfig.PARSER.apply(parser, null);
            assertThat(parsedConfig, equalTo(putRequest.getConfig()));
        }
        putRequest.setDeferValidation(true);
        request = TransformRequestConverters.putTransform(putRequest);
        assertThat(request.getParameters(), hasEntry("defer_validation", Boolean.toString(putRequest.getDeferValidation())));
    }

    public void testUpdateDataFrameTransform() throws IOException {
        String transformId = randomAlphaOfLength(10);
        UpdateTransformRequest updateDataFrameTransformRequest = new UpdateTransformRequest(
            TransformConfigUpdateTests.randomTransformConfigUpdate(),
            transformId);
        Request request = TransformRequestConverters.updateTransform(updateDataFrameTransformRequest);
        assertThat(request.getParameters(), not(hasKey("defer_validation")));
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_transform/" + transformId + "/_update"));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            TransformConfigUpdate parsedConfig = TransformConfigUpdate.fromXContent(parser);
            assertThat(parsedConfig, equalTo(updateDataFrameTransformRequest.getUpdate()));
        }
        updateDataFrameTransformRequest.setDeferValidation(true);
        request = TransformRequestConverters.updateTransform(updateDataFrameTransformRequest);
        assertThat(request.getParameters(),
            hasEntry("defer_validation", Boolean.toString(updateDataFrameTransformRequest.getDeferValidation())));
    }

    public void testDeleteDataFrameTransform() {
        DeleteTransformRequest deleteRequest = new DeleteTransformRequest("foo");
        Request request = TransformRequestConverters.deleteTransform(deleteRequest);

        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_transform/foo"));

        assertThat(request.getParameters(), not(hasKey("force")));

        deleteRequest.setForce(true);
        request = TransformRequestConverters.deleteTransform(deleteRequest);

        assertThat(request.getParameters(), hasEntry("force", "true"));
    }

    public void testStartDataFrameTransform() {
        String id = randomAlphaOfLength(10);
        TimeValue timeValue = null;
        if (randomBoolean()) {
            timeValue = TimeValue.parseTimeValue(randomTimeValue(), "timeout");
        }
        StartTransformRequest startRequest = new StartTransformRequest(id, timeValue);

        Request request = TransformRequestConverters.startTransform(startRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_transform/" + startRequest.getId() + "/_start"));

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
        Boolean waitForCheckpoint = null;
        if (randomBoolean()) {
            waitForCheckpoint = randomBoolean();
        }

        StopTransformRequest stopRequest = new StopTransformRequest(id, waitForCompletion, timeValue, waitForCheckpoint);

        Request request = TransformRequestConverters.stopTransform(stopRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_transform/" + stopRequest.getId() + "/_stop"));

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
        if (waitForCheckpoint != null) {
            assertTrue(request.getParameters().containsKey("wait_for_checkpoint"));
            assertEquals(stopRequest.getWaitForCheckpoint(), Boolean.parseBoolean(request.getParameters().get("wait_for_checkpoint")));
        } else {
            assertFalse(request.getParameters().containsKey("wait_for_checkpoint"));
        }

        assertFalse(request.getParameters().containsKey(ALLOW_NO_MATCH));
        stopRequest.setAllowNoMatch(randomBoolean());
        request = TransformRequestConverters.stopTransform(stopRequest);
        assertEquals(stopRequest.getAllowNoMatch(), Boolean.parseBoolean(request.getParameters().get(ALLOW_NO_MATCH)));
    }

    public void testPreviewDataFrameTransform() throws IOException {
        PreviewTransformRequest previewRequest = new PreviewTransformRequest(TransformConfigTests.randomTransformConfig());
        Request request = TransformRequestConverters.previewTransform(previewRequest);

        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_transform/_preview"));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            PreviewTransformRequest parsedRequest = PreviewTransformRequestTests.fromXContent(parser);
            assertThat(parsedRequest, equalTo(previewRequest));
        }
    }

    public void testPreviewDataFrameTransformById() throws IOException {
        String transformId = randomAlphaOfLengthBetween(1, 10);
        PreviewTransformRequest previewRequest = new PreviewTransformRequest(transformId);
        Request request = TransformRequestConverters.previewTransform(previewRequest);

        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_transform/" + transformId + "/_preview"));
        assertThat(request.getEntity(), nullValue());
    }

    public void testGetDataFrameTransformStats() {
        GetTransformStatsRequest getStatsRequest = new GetTransformStatsRequest("foo");
        Request request = TransformRequestConverters.getTransformStats(getStatsRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_transform/foo/_stats"));

        assertFalse(request.getParameters().containsKey("from"));
        assertFalse(request.getParameters().containsKey("size"));
        assertFalse(request.getParameters().containsKey(ALLOW_NO_MATCH));

        getStatsRequest.setPageParams(new PageParams(0, null));
        request = TransformRequestConverters.getTransformStats(getStatsRequest);
        assertThat(request.getParameters(), hasEntry("from", "0"));
        assertEquals(null, request.getParameters().get("size"));

        getStatsRequest.setPageParams(new PageParams(null, 50));
        request = TransformRequestConverters.getTransformStats(getStatsRequest);
        assertEquals(null, request.getParameters().get("from"));
        assertThat(request.getParameters(), hasEntry("size", "50"));

        getStatsRequest.setPageParams(new PageParams(0, 10));
        request = TransformRequestConverters.getTransformStats(getStatsRequest);
        assertThat(request.getParameters(), allOf(hasEntry("from", "0"), hasEntry("size", "10")));

        getStatsRequest.setAllowNoMatch(false);
        request = TransformRequestConverters.getTransformStats(getStatsRequest);
        assertThat(request.getParameters(), hasEntry("allow_no_match", "false"));
    }

    public void testGetDataFrameTransform() {
        GetTransformRequest getRequest = new GetTransformRequest("bar");
        Request request = TransformRequestConverters.getTransform(getRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_transform/bar"));

        assertFalse(request.getParameters().containsKey("from"));
        assertFalse(request.getParameters().containsKey("size"));
        assertFalse(request.getParameters().containsKey(ALLOW_NO_MATCH));

        getRequest.setPageParams(new PageParams(0, null));
        request = TransformRequestConverters.getTransform(getRequest);
        assertThat(request.getParameters(), hasEntry("from", "0"));
        assertEquals(null, request.getParameters().get("size"));

        getRequest.setPageParams(new PageParams(null, 50));
        request = TransformRequestConverters.getTransform(getRequest);
        assertEquals(null, request.getParameters().get("from"));
        assertThat(request.getParameters(), hasEntry("size", "50"));

        getRequest.setPageParams(new PageParams(0, 10));
        request = TransformRequestConverters.getTransform(getRequest);
        assertThat(request.getParameters(), allOf(hasEntry("from", "0"), hasEntry("size", "10")));

        getRequest.setAllowNoMatch(false);
        request = TransformRequestConverters.getTransform(getRequest);
        assertThat(request.getParameters(), hasEntry("allow_no_match", "false"));
    }

    public void testGetDataFrameTransform_givenMulitpleIds() {
        GetTransformRequest getRequest = new GetTransformRequest("foo", "bar", "baz");
        Request request = TransformRequestConverters.getTransform(getRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_transform/foo,bar,baz"));
    }
}
