/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.watcher.AckWatchRequest;
import org.elasticsearch.client.watcher.AckWatchResponse;
import org.elasticsearch.client.watcher.ActionStatus;
import org.elasticsearch.client.watcher.ActionStatus.AckStatus;
import org.elasticsearch.client.watcher.ActivateWatchRequest;
import org.elasticsearch.client.watcher.ActivateWatchResponse;
import org.elasticsearch.client.watcher.DeactivateWatchRequest;
import org.elasticsearch.client.watcher.DeactivateWatchResponse;
import org.elasticsearch.client.watcher.DeleteWatchRequest;
import org.elasticsearch.client.watcher.DeleteWatchResponse;
import org.elasticsearch.client.watcher.ExecuteWatchRequest;
import org.elasticsearch.client.watcher.ExecuteWatchResponse;
import org.elasticsearch.client.watcher.PutWatchRequest;
import org.elasticsearch.client.watcher.PutWatchResponse;
import org.elasticsearch.client.watcher.StartWatchServiceRequest;
import org.elasticsearch.client.watcher.StopWatchServiceRequest;
import org.elasticsearch.client.watcher.WatcherState;
import org.elasticsearch.client.watcher.WatcherStatsRequest;
import org.elasticsearch.client.watcher.WatcherStatsResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class WatcherIT extends ESRestHighLevelClientTestCase {

    public void testStartWatchService() throws Exception {
        AcknowledgedResponse response =
                highLevelClient().watcher().startWatchService(new StartWatchServiceRequest(), RequestOptions.DEFAULT);
        assertTrue(response.isAcknowledged());

        WatcherStatsResponse stats = highLevelClient().watcher().watcherStats(new WatcherStatsRequest(), RequestOptions.DEFAULT);
        assertFalse(stats.getWatcherMetadata().manuallyStopped());
        assertThat(stats.getNodes(), not(empty()));
        for(WatcherStatsResponse.Node node : stats.getNodes()) {
            assertEquals(WatcherState.STARTED, node.getWatcherState());
        }
    }

    public void testStopWatchService() throws Exception {
        AcknowledgedResponse response =
                highLevelClient().watcher().stopWatchService(new StopWatchServiceRequest(), RequestOptions.DEFAULT);
        assertTrue(response.isAcknowledged());

        WatcherStatsResponse stats = highLevelClient().watcher().watcherStats(new WatcherStatsRequest(), RequestOptions.DEFAULT);
        assertTrue(stats.getWatcherMetadata().manuallyStopped());
    }

    public void testPutWatch() throws Exception {
        String watchId = randomAlphaOfLength(10);
        PutWatchResponse putWatchResponse = createWatch(watchId);
        assertThat(putWatchResponse.isCreated(), is(true));
        assertThat(putWatchResponse.getId(), is(watchId));
        assertThat(putWatchResponse.getVersion(), is(1L));
    }

    private static final String WATCH_JSON = "{ \n" +
        "  \"trigger\": { \"schedule\": { \"interval\": \"10h\" } },\n" +
        "  \"input\": { \"none\": {} },\n" +
        "  \"actions\": { \"logme\": { \"logging\": { \"text\": \"{{ctx.payload}}\" } } }\n" +
        "}";

    private PutWatchResponse createWatch(String watchId) throws Exception {
        BytesReference bytesReference = new BytesArray(WATCH_JSON);
        PutWatchRequest putWatchRequest = new PutWatchRequest(watchId, bytesReference, XContentType.JSON);
        return highLevelClient().watcher().putWatch(putWatchRequest, RequestOptions.DEFAULT);
    }

    public void testDeactivateWatch() throws Exception {
        // Deactivate a watch that exists
        String watchId = randomAlphaOfLength(10);
        createWatch(watchId);
        DeactivateWatchResponse response = highLevelClient().watcher().deactivateWatch(
            new DeactivateWatchRequest(watchId), RequestOptions.DEFAULT);
        assertThat(response.getStatus().state().isActive(), is(false));
    }
    public void testDeactivateWatch404() throws Exception {
        // Deactivate a watch that does not exist
        String watchId = randomAlphaOfLength(10);
        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
            () -> highLevelClient().watcher().deactivateWatch(new DeactivateWatchRequest(watchId), RequestOptions.DEFAULT));
        assertEquals(RestStatus.NOT_FOUND, exception.status());

    }

    public void testDeleteWatch() throws Exception {
        // delete watch that exists
        {
            String watchId = randomAlphaOfLength(10);
            createWatch(watchId);
            DeleteWatchResponse deleteWatchResponse = highLevelClient().watcher().deleteWatch(new DeleteWatchRequest(watchId),
                RequestOptions.DEFAULT);
            assertThat(deleteWatchResponse.getId(), is(watchId));
            assertThat(deleteWatchResponse.getVersion(), is(2L));
            assertThat(deleteWatchResponse.isFound(), is(true));
        }

        // delete watch that does not exist
        {
            String watchId = randomAlphaOfLength(10);
            DeleteWatchResponse deleteWatchResponse = highLevelClient().watcher().deleteWatch(new DeleteWatchRequest(watchId),
                RequestOptions.DEFAULT);
            assertThat(deleteWatchResponse.getId(), is(watchId));
            assertThat(deleteWatchResponse.getVersion(), is(1L));
            assertThat(deleteWatchResponse.isFound(), is(false));
        }
    }

    public void testAckWatch() throws Exception {
        String watchId = randomAlphaOfLength(10);
        String actionId = "logme";

        PutWatchResponse putWatchResponse = createWatch(watchId);
        assertThat(putWatchResponse.isCreated(), is(true));

        AckWatchResponse response = highLevelClient().watcher().ackWatch(
            new AckWatchRequest(watchId, actionId), RequestOptions.DEFAULT);

        ActionStatus actionStatus = response.getStatus().actionStatus(actionId);
        assertEquals(AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION, actionStatus.ackStatus().state());

        // TODO: use the high-level REST client here once it supports 'execute watch'.
        Request executeWatchRequest = new Request("POST", "_watcher/watch/" + watchId + "/_execute");
        executeWatchRequest.setJsonEntity("{ \"record_execution\": true }");
        Response executeResponse = client().performRequest(executeWatchRequest);
        assertEquals(RestStatus.OK.getStatus(), executeResponse.getStatusLine().getStatusCode());

        response = highLevelClient().watcher().ackWatch(
            new AckWatchRequest(watchId, actionId), RequestOptions.DEFAULT);

        actionStatus = response.getStatus().actionStatus(actionId);
        assertEquals(AckStatus.State.ACKED, actionStatus.ackStatus().state());

        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
            () -> highLevelClient().watcher().ackWatch(
                new AckWatchRequest("nonexistent"), RequestOptions.DEFAULT));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    public void testActivateWatchThatExists() throws Exception {
        String watchId = randomAlphaOfLength(10);
        createWatch(watchId);
        ActivateWatchResponse activateWatchResponse1 = highLevelClient().watcher().activateWatch(new ActivateWatchRequest(watchId),
            RequestOptions.DEFAULT);
        assertThat(activateWatchResponse1.getStatus().state().isActive(), is(true));

        ActivateWatchResponse activateWatchResponse2 = highLevelClient().watcher().activateWatch(new ActivateWatchRequest(watchId),
            RequestOptions.DEFAULT);
        assertThat(activateWatchResponse2.getStatus().state().isActive(), is(true));
        assertThat(activateWatchResponse1.getStatus().state().getTimestamp(),
            lessThan(activateWatchResponse2.getStatus().state().getTimestamp()));
    }

    public void testActivateWatchThatDoesNotExist() throws Exception {
        String watchId = randomAlphaOfLength(10);
        // exception when activating a not existing watcher
        ElasticsearchStatusException exception =  expectThrows(ElasticsearchStatusException.class, () ->
            highLevelClient().watcher().activateWatch(new ActivateWatchRequest(watchId), RequestOptions.DEFAULT));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }


    public void testExecuteWatchById() throws Exception {
        String watchId = randomAlphaOfLength(10);
        createWatch(watchId);

        ExecuteWatchResponse response = highLevelClient().watcher()
            .executeWatch(ExecuteWatchRequest.byId(watchId), RequestOptions.DEFAULT);
        assertThat(response.getRecordId(), containsString(watchId));

        Map<String, Object> source = response.getRecordAsMap();
        assertThat(ObjectPath.eval("trigger_event.type", source), is("manual"));

    }

    public void testExecuteWatchThatDoesNotExist() throws Exception {
        String watchId = randomAlphaOfLength(10);
        // exception when activating a not existing watcher
        ElasticsearchStatusException exception =  expectThrows(ElasticsearchStatusException.class, () ->
            highLevelClient().watcher().executeWatch(ExecuteWatchRequest.byId(watchId), RequestOptions.DEFAULT));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    public void testExecuteInlineWatch() throws Exception {
        ExecuteWatchResponse response = highLevelClient().watcher()
            .executeWatch(ExecuteWatchRequest.inline(WATCH_JSON), RequestOptions.DEFAULT);
        assertThat(response.getRecordId(), containsString("_inlined_"));

        Map<String, Object> source = response.getRecordAsMap();
        assertThat(ObjectPath.eval("trigger_event.type", source), is("manual"));
    }

    public void testWatcherStatsMetrics() throws Exception {
        boolean includeCurrent = randomBoolean();
        boolean includeQueued = randomBoolean();
        WatcherStatsRequest request = new WatcherStatsRequest(includeCurrent, includeQueued);

        WatcherStatsResponse stats = highLevelClient().watcher().watcherStats(request, RequestOptions.DEFAULT);
        assertThat(stats.getNodes(), not(empty()));
        assertEquals(includeCurrent, stats.getNodes().get(0).getSnapshots() != null);
        assertEquals(includeQueued, stats.getNodes().get(0).getQueuedWatches() != null);
    }
}
