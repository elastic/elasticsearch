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

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.watcher.AckWatchRequest;
import org.elasticsearch.client.watcher.ActivateWatchRequest;
import org.elasticsearch.client.watcher.DeactivateWatchRequest;
import org.elasticsearch.client.watcher.DeleteWatchRequest;
import org.elasticsearch.client.watcher.PutWatchRequest;
import org.elasticsearch.client.watcher.GetWatchRequest;
import org.elasticsearch.client.watcher.StartWatchServiceRequest;
import org.elasticsearch.client.watcher.StopWatchServiceRequest;
import org.elasticsearch.client.watcher.WatcherStatsRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class WatcherRequestConvertersTests extends ESTestCase {

    public void testStartWatchService() {
        Request request = WatcherRequestConverters.startWatchService(new StartWatchServiceRequest());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/watcher/_start", request.getEndpoint());
    }

    public void testStopWatchService() {
        Request request = WatcherRequestConverters.stopWatchService(new StopWatchServiceRequest());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/watcher/_stop", request.getEndpoint());
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
            long version = randomLongBetween(10, 100);
            putWatchRequest.setVersion(version);
            expectedParams.put("version", String.valueOf(version));
        }

        Request request = WatcherRequestConverters.putWatch(putWatchRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/watcher/watch/" + watchId, request.getEndpoint());
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
        assertEquals("/watcher/watch/" + watchId, request.getEndpoint());
        assertThat(request.getEntity(), nullValue());
    }

    public void testDeactivateWatch() {
        String watchId = randomAlphaOfLength(10);
        DeactivateWatchRequest deactivateWatchRequest = new DeactivateWatchRequest(watchId);
        Request request = WatcherRequestConverters.deactivateWatch(deactivateWatchRequest);

        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/watcher/watch/" + watchId + "/_deactivate", request.getEndpoint());
    }

    public void testDeleteWatch() {
        String watchId = randomAlphaOfLength(10);
        DeleteWatchRequest deleteWatchRequest = new DeleteWatchRequest(watchId);

        Request request = WatcherRequestConverters.deleteWatch(deleteWatchRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/watcher/watch/" + watchId, request.getEndpoint());
        assertThat(request.getEntity(), nullValue());
    }

    public void testAckWatch() {
        String watchId = randomAlphaOfLength(10);
        String[] actionIds = generateRandomStringArray(5, 10, false, true);

        AckWatchRequest ackWatchRequest = new AckWatchRequest(watchId, actionIds);
        Request request = WatcherRequestConverters.ackWatch(ackWatchRequest);

        assertEquals(HttpPut.METHOD_NAME, request.getMethod());

        StringJoiner expectedEndpoint = new StringJoiner("/", "/", "")
            .add("watcher").add("watch").add(watchId).add("_ack");
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
        assertEquals("/watcher/watch/" + watchId + "/_activate", request.getEndpoint());
        assertThat(request.getEntity(), nullValue());
    }

    public void testWatcherStatsRequest() {
        boolean includeCurrent = randomBoolean();
        boolean includeQueued = randomBoolean();

        WatcherStatsRequest watcherStatsRequest = new WatcherStatsRequest(includeCurrent, includeQueued);

        Request request = WatcherRequestConverters.watcherStats(watcherStatsRequest);
        assertThat(request.getEndpoint(), equalTo("/watcher/stats"));
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
}
