/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.watcher.AckWatchRequest;
import org.elasticsearch.client.watcher.ActivateWatchRequest;
import org.elasticsearch.client.watcher.DeactivateWatchRequest;
import org.elasticsearch.client.watcher.DeleteWatchRequest;
import org.elasticsearch.client.watcher.ExecuteWatchRequest;
import org.elasticsearch.client.watcher.GetWatchRequest;
import org.elasticsearch.client.watcher.PutWatchRequest;
import org.elasticsearch.client.watcher.StartWatchServiceRequest;
import org.elasticsearch.client.watcher.StopWatchServiceRequest;
import org.elasticsearch.client.watcher.WatcherStatsRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class WatcherRequestConvertersTests extends ESTestCase {

    private static String toString(HttpEntity entity) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        entity.writeTo(baos);
        return baos.toString(StandardCharsets.UTF_8.name());
    }

    public void testStartWatchService() {
        Request request = WatcherRequestConverters.startWatchService(new StartWatchServiceRequest());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_watcher/_start", request.getEndpoint());
    }

    public void testStopWatchService() {
        Request request = WatcherRequestConverters.stopWatchService(new StopWatchServiceRequest());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_watcher/_stop", request.getEndpoint());
    }

    public void testPutWatch() throws Exception {
        String watchId = randomAlphaOfLength(10);
        String body = randomAlphaOfLength(20);
        PutWatchRequest putWatchRequest = new PutWatchRequest(watchId, new BytesArray(body), XContentType.JSON);

        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            putWatchRequest.setActive(false);
            expectedParams.put("active", "false");
        }

        if (randomBoolean()) {
            long seqNo = randomNonNegativeLong();
            long ifPrimaryTerm = randomLongBetween(1, 200);
            putWatchRequest.setIfSeqNo(seqNo);
            putWatchRequest.setIfPrimaryTerm(ifPrimaryTerm);
            expectedParams.put("if_seq_no", String.valueOf(seqNo));
            expectedParams.put("if_primary_term", String.valueOf(ifPrimaryTerm));
        }

        Request request = WatcherRequestConverters.putWatch(putWatchRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_watcher/watch/" + watchId, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertThat(request.getEntity().getContentType().getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        request.getEntity().writeTo(bos);
        assertThat(bos.toString("UTF-8"), is(body));
    }

    public void testGetWatch() throws Exception {
        String watchId = randomAlphaOfLength(10);
        GetWatchRequest getWatchRequest = new GetWatchRequest(watchId);

        Request request = WatcherRequestConverters.getWatch(getWatchRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_watcher/watch/" + watchId, request.getEndpoint());
        assertThat(request.getEntity(), nullValue());
    }

    public void testDeactivateWatch() {
        String watchId = randomAlphaOfLength(10);
        DeactivateWatchRequest deactivateWatchRequest = new DeactivateWatchRequest(watchId);
        Request request = WatcherRequestConverters.deactivateWatch(deactivateWatchRequest);

        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_watcher/watch/" + watchId + "/_deactivate", request.getEndpoint());
    }

    public void testDeleteWatch() {
        String watchId = randomAlphaOfLength(10);
        DeleteWatchRequest deleteWatchRequest = new DeleteWatchRequest(watchId);

        Request request = WatcherRequestConverters.deleteWatch(deleteWatchRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_watcher/watch/" + watchId, request.getEndpoint());
        assertThat(request.getEntity(), nullValue());
    }

    public void testAckWatch() {
        String watchId = randomAlphaOfLength(10);
        String[] actionIds = generateRandomStringArray(5, 10, false, true);

        AckWatchRequest ackWatchRequest = new AckWatchRequest(watchId, actionIds);
        Request request = WatcherRequestConverters.ackWatch(ackWatchRequest);

        assertEquals(HttpPut.METHOD_NAME, request.getMethod());

        StringJoiner expectedEndpoint = new StringJoiner("/", "/", "")
            .add("_watcher").add("watch").add(watchId).add("_ack");
        if (ackWatchRequest.getActionIds().length > 0) {
            String actionsParam = String.join(",", ackWatchRequest.getActionIds());
            expectedEndpoint.add(actionsParam);
        }

        assertEquals(expectedEndpoint.toString(), request.getEndpoint());
        assertThat(request.getEntity(), nullValue());
    }

    public void testActivateWatchRequestConversion() {
        String watchId = randomAlphaOfLength(10);
        ActivateWatchRequest activateWatchRequest = new ActivateWatchRequest(watchId);

        Request request = WatcherRequestConverters.activateWatch(activateWatchRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_watcher/watch/" + watchId + "/_activate", request.getEndpoint());
        assertThat(request.getEntity(), nullValue());
    }

    public void testWatcherStatsRequest() {
        boolean includeCurrent = randomBoolean();
        boolean includeQueued = randomBoolean();

        WatcherStatsRequest watcherStatsRequest = new WatcherStatsRequest(includeCurrent, includeQueued);

        Request request = WatcherRequestConverters.watcherStats(watcherStatsRequest);
        assertThat(request.getEndpoint(), equalTo("/_watcher/stats"));
        assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        if (includeCurrent || includeQueued) {
            assertThat(request.getParameters(), hasKey("metric"));
            Set<String> metric = Strings.tokenizeByCommaToSet(request.getParameters().get("metric"));
            assertThat(metric, hasSize((includeCurrent?1:0) + (includeQueued?1:0)));
            Set<String> expectedMetric = new HashSet<>();
            if (includeCurrent) {
                expectedMetric.add("current_watches");
            }
            if (includeQueued) {
                expectedMetric.add("queued_watches");
            }
            assertThat(metric, equalTo(expectedMetric));
        } else {
            assertThat(request.getParameters(), not(hasKey("metric")));
        }
        assertThat(request.getEntity(), nullValue());
    }

    public void testExecuteWatchByIdRequest() throws IOException {

        boolean ignoreCondition = randomBoolean();
        boolean recordExecution = randomBoolean();
        boolean debug = randomBoolean();

        ExecuteWatchRequest request = ExecuteWatchRequest.byId("my_id");
        request.setIgnoreCondition(ignoreCondition);
        request.setRecordExecution(recordExecution);
        request.setDebug(debug);

        boolean setActionMode = randomBoolean();
        if (setActionMode) {
            request.setActionMode("action1", ExecuteWatchRequest.ActionExecutionMode.SIMULATE);
        }

        boolean useTriggerData = randomBoolean();
        String triggerData = "{ \"entry1\" : \"blah\", \"entry2\" : \"blah\" }";
        if (useTriggerData) {
            request.setTriggerData(triggerData);
        }

        boolean useAlternativeInput = randomBoolean();
        String alternativeInput = "{ \"foo\" : \"bar\" }";
        if (useAlternativeInput) {
            request.setAlternativeInput(alternativeInput);
        }

        Request req = WatcherRequestConverters.executeWatch(request);
        assertThat(req.getEndpoint(), equalTo("/_watcher/watch/my_id/_execute"));
        assertThat(req.getMethod(), equalTo(HttpPost.METHOD_NAME));

        if (ignoreCondition) {
            assertThat(req.getParameters(), hasKey("ignore_condition"));
            assertThat(req.getParameters().get("ignore_condition"), is("true"));
        }

        if (recordExecution) {
            assertThat(req.getParameters(), hasKey("record_execution"));
            assertThat(req.getParameters().get("record_execution"), is("true"));
        }

        if (debug) {
            assertThat(req.getParameters(), hasKey("debug"));
            assertThat(req.getParameters().get("debug"), is("true"));
        }

        String body = toString(req.getEntity());
        if (setActionMode) {
            assertThat(body, containsString("\"action_modes\":{\"action1\":\"SIMULATE\"}"));
        }
        else {
            assertThat(body, not(containsString("action_modes")));
        }
        if (useTriggerData) {
            assertThat(body, containsString("\"trigger_data\":" + triggerData));
        }
        else {
            assertThat(body, not(containsString("trigger_data")));
        }
        if (useAlternativeInput) {
            assertThat(body, containsString("\"alternative_input\":" + alternativeInput));
        }
        else {
            assertThat(body, not(containsString("alternative_input")));
        }
        assertThat(body, not(containsString("\"watch\":")));

    }

    private static final String WATCH_JSON = "{ \n" +
        "  \"trigger\": { \"schedule\": { \"interval\": \"10h\" } },\n" +
        "  \"input\": { \"none\": {} },\n" +
        "  \"actions\": { \"logme\": { \"logging\": { \"text\": \"{{ctx.payload}}\" } } }\n" +
        "}";

    public void testExecuteInlineWatchRequest() throws IOException {
        boolean ignoreCondition = randomBoolean();

        ExecuteWatchRequest request = ExecuteWatchRequest.inline(WATCH_JSON);
        request.setIgnoreCondition(ignoreCondition);

        expectThrows(IllegalArgumentException.class, () -> {
            request.setRecordExecution(true);
        });

        boolean setActionMode = randomBoolean();
        if (setActionMode) {
            request.setActionMode("action1", ExecuteWatchRequest.ActionExecutionMode.SIMULATE);
        }

        boolean useTriggerData = randomBoolean();
        String triggerData = "{ \"entry1\" : \"blah\", \"entry2\" : \"blah\" }";
        if (useTriggerData) {
            request.setTriggerData(triggerData);
        }

        boolean useAlternativeInput = randomBoolean();
        String alternativeInput = "{ \"foo\" : \"bar\" }";
        if (useAlternativeInput) {
            request.setAlternativeInput(alternativeInput);
        }

        Request req = WatcherRequestConverters.executeWatch(request);
        assertThat(req.getEndpoint(), equalTo("/_watcher/watch/_execute"));
        assertThat(req.getMethod(), equalTo(HttpPost.METHOD_NAME));

        if (ignoreCondition) {
            assertThat(req.getParameters(), hasKey("ignore_condition"));
            assertThat(req.getParameters().get("ignore_condition"), is("true"));
        }

        String body = toString(req.getEntity());
        if (setActionMode) {
            assertThat(body, containsString("\"action_modes\":{\"action1\":\"SIMULATE\"}"));
        }
        else {
            assertThat(body, not(containsString("action_modes")));
        }
        if (useTriggerData) {
            assertThat(body, containsString("\"trigger_data\":" + triggerData));
        }
        else {
            assertThat(body, not(containsString("trigger_data")));
        }
        if (useAlternativeInput) {
            assertThat(body, containsString("\"alternative_input\":" + alternativeInput));
        }
        else {
            assertThat(body, not(containsString("alternative_input")));
        }
        assertThat(body, containsString("\"watch\":" + WATCH_JSON));
    }
}
