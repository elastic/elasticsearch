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

package org.elasticsearch.index.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;

/**
 * Round trip tests for all Streamable things declared in this plugin.
 */
public class RoundTripTests extends ESTestCase {
    public void testReindexRequest() throws IOException {
        ReindexRequest reindex = new ReindexRequest(new SearchRequest(), new IndexRequest());
        randomRequest(reindex);
        reindex.getDestination().version(randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED, 12L, 1L, 123124L, 12L));
        reindex.getDestination().index("test");
        if (randomBoolean()) {
            int port = between(1, Integer.MAX_VALUE);
            BytesReference query = new BytesArray(randomAlphaOfLength(5));
            String username = randomBoolean() ? randomAlphaOfLength(5) : null;
            String password = username != null && randomBoolean() ? randomAlphaOfLength(5) : null;
            int headersCount = randomBoolean() ? 0 : between(1, 10);
            Map<String, String> headers = new HashMap<>(headersCount);
            while (headers.size() < headersCount) {
                headers.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
            TimeValue socketTimeout = parseTimeValue(randomPositiveTimeValue(), "socketTimeout");
            TimeValue connectTimeout = parseTimeValue(randomPositiveTimeValue(), "connectTimeout");
            reindex.setRemoteInfo(new RemoteInfo(randomAlphaOfLength(5), randomAlphaOfLength(5), port, query, username, password, headers,
                    socketTimeout, connectTimeout));
        }
        ReindexRequest tripped = new ReindexRequest();
        roundTrip(reindex, tripped);
        assertRequestEquals(reindex, tripped);

        // Try slices=auto with a version that doesn't support it, which should fail
        reindex.setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);
        Exception e = expectThrows(IllegalArgumentException.class, () -> roundTrip(Version.V_6_0_0_alpha1, reindex, null));
        assertEquals("Slices set as \"auto\" are not supported before version [6.1.0]. Found version [6.0.0-alpha1]", e.getMessage());

        // Try regular slices with a version that doesn't support slices=auto, which should succeed
        tripped = new ReindexRequest();
        reindex.setSlices(between(1, Integer.MAX_VALUE));
        roundTrip(Version.V_6_0_0_alpha1, reindex, tripped);
        assertRequestEquals(Version.V_6_0_0_alpha1, reindex, tripped);
    }

    public void testUpdateByQueryRequest() throws IOException {
        UpdateByQueryRequest update = new UpdateByQueryRequest(new SearchRequest());
        randomRequest(update);
        if (randomBoolean()) {
            update.setPipeline(randomAlphaOfLength(5));
        }
        UpdateByQueryRequest tripped = new UpdateByQueryRequest();
        roundTrip(update, tripped);
        assertRequestEquals(update, tripped);
        assertEquals(update.getPipeline(), tripped.getPipeline());

        // Try slices=auto with a version that doesn't support it, which should fail
        update.setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);
        Exception e = expectThrows(IllegalArgumentException.class, () -> roundTrip(Version.V_6_0_0_alpha1, update, null));
        assertEquals("Slices set as \"auto\" are not supported before version [6.1.0]. Found version [6.0.0-alpha1]", e.getMessage());

        // Try regular slices with a version that doesn't support slices=auto, which should succeed
        tripped = new UpdateByQueryRequest();
        update.setSlices(between(1, Integer.MAX_VALUE));
        roundTrip(Version.V_6_0_0_alpha1, update, tripped);
        assertRequestEquals(update, tripped);
        assertEquals(update.getPipeline(), tripped.getPipeline());
    }

    public void testDeleteByQueryRequest() throws IOException {
        DeleteByQueryRequest delete = new DeleteByQueryRequest(new SearchRequest());
        randomRequest(delete);
        DeleteByQueryRequest tripped = new DeleteByQueryRequest();
        roundTrip(delete, tripped);
        assertRequestEquals(delete, tripped);

        // Try slices=auto with a version that doesn't support it, which should fail
        delete.setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);
        Exception e = expectThrows(IllegalArgumentException.class, () -> roundTrip(Version.V_6_0_0_alpha1, delete, null));
        assertEquals("Slices set as \"auto\" are not supported before version [6.1.0]. Found version [6.0.0-alpha1]", e.getMessage());

        // Try regular slices with a version that doesn't support slices=auto, which should succeed
        tripped = new DeleteByQueryRequest();
        delete.setSlices(between(1, Integer.MAX_VALUE));
        roundTrip(Version.V_6_0_0_alpha1, delete, tripped);
        assertRequestEquals(delete, tripped);
    }

    private void randomRequest(AbstractBulkByScrollRequest<?> request) {
        request.getSearchRequest().indices("test");
        request.getSearchRequest().source().size(between(1, 1000));
        if (randomBoolean()) {
            request.setSize(between(1, Integer.MAX_VALUE));
        }
        request.setAbortOnVersionConflict(random().nextBoolean());
        request.setRefresh(rarely());
        request.setTimeout(TimeValue.parseTimeValue(randomTimeValue(), null, "test"));
        request.setWaitForActiveShards(randomIntBetween(0, 10));
        request.setRequestsPerSecond(between(0, Integer.MAX_VALUE));

        int slices = ReindexTestCase.randomSlices(1, Integer.MAX_VALUE);
        request.setSlices(slices);
    }

    private void randomRequest(AbstractBulkIndexByScrollRequest<?> request) {
        randomRequest((AbstractBulkByScrollRequest<?>) request);
        request.setScript(random().nextBoolean() ? null : randomScript());
    }

    private void assertRequestEquals(Version version, ReindexRequest request, ReindexRequest tripped) {
        assertRequestEquals((AbstractBulkIndexByScrollRequest<?>) request, (AbstractBulkIndexByScrollRequest<?>) tripped);
        assertEquals(request.getDestination().version(), tripped.getDestination().version());
        assertEquals(request.getDestination().index(), tripped.getDestination().index());
        if (request.getRemoteInfo() == null) {
            assertNull(tripped.getRemoteInfo());
        } else {
            assertNotNull(tripped.getRemoteInfo());
            assertEquals(request.getRemoteInfo().getScheme(), tripped.getRemoteInfo().getScheme());
            assertEquals(request.getRemoteInfo().getHost(), tripped.getRemoteInfo().getHost());
            assertEquals(request.getRemoteInfo().getQuery(), tripped.getRemoteInfo().getQuery());
            assertEquals(request.getRemoteInfo().getUsername(), tripped.getRemoteInfo().getUsername());
            assertEquals(request.getRemoteInfo().getPassword(), tripped.getRemoteInfo().getPassword());
            assertEquals(request.getRemoteInfo().getHeaders(), tripped.getRemoteInfo().getHeaders());
            if (version.onOrAfter(Version.V_5_2_0)) {
                assertEquals(request.getRemoteInfo().getSocketTimeout(), tripped.getRemoteInfo().getSocketTimeout());
                assertEquals(request.getRemoteInfo().getConnectTimeout(), tripped.getRemoteInfo().getConnectTimeout());
            } else {
                assertEquals(RemoteInfo.DEFAULT_SOCKET_TIMEOUT, tripped.getRemoteInfo().getSocketTimeout());
                assertEquals(RemoteInfo.DEFAULT_CONNECT_TIMEOUT, tripped.getRemoteInfo().getConnectTimeout());
            }
        }
    }

    private void assertRequestEquals(AbstractBulkIndexByScrollRequest<?> request,
            AbstractBulkIndexByScrollRequest<?> tripped) {
        assertRequestEquals((AbstractBulkByScrollRequest<?>) request, (AbstractBulkByScrollRequest<?>) tripped);
        assertEquals(request.getScript(), tripped.getScript());
    }

    private void assertRequestEquals(AbstractBulkByScrollRequest<?> request, AbstractBulkByScrollRequest<?> tripped) {
        assertArrayEquals(request.getSearchRequest().indices(), tripped.getSearchRequest().indices());
        assertEquals(request.getSearchRequest().source().size(), tripped.getSearchRequest().source().size());
        assertEquals(request.isAbortOnVersionConflict(), tripped.isAbortOnVersionConflict());
        assertEquals(request.isRefresh(), tripped.isRefresh());
        assertEquals(request.getTimeout(), tripped.getTimeout());
        assertEquals(request.getWaitForActiveShards(), tripped.getWaitForActiveShards());
        assertEquals(request.getRetryBackoffInitialTime(), tripped.getRetryBackoffInitialTime());
        assertEquals(request.getMaxRetries(), tripped.getMaxRetries());
        assertEquals(request.getRequestsPerSecond(), tripped.getRequestsPerSecond(), 0d);
    }

    public void testRethrottleRequest() throws IOException {
        RethrottleRequest request = new RethrottleRequest();
        request.setRequestsPerSecond((float) randomDoubleBetween(0, Float.POSITIVE_INFINITY, false));
        if (randomBoolean()) {
            request.setActions(randomFrom(UpdateByQueryAction.NAME, ReindexAction.NAME));
        } else {
            request.setTaskId(new TaskId(randomAlphaOfLength(5), randomLong()));
        }
        RethrottleRequest tripped = new RethrottleRequest();
        roundTrip(request, tripped);
        assertEquals(request.getRequestsPerSecond(), tripped.getRequestsPerSecond(), 0.00001);
        assertArrayEquals(request.getActions(), tripped.getActions());
        assertEquals(request.getTaskId(), tripped.getTaskId());
    }

    private void roundTrip(Streamable example, Streamable empty) throws IOException {
        roundTrip(Version.CURRENT, example, empty);
    }

    private void roundTrip(Version version, Streamable example, Streamable empty) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        example.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        empty.readFrom(in);
    }

    private Script randomScript() {
        ScriptType type = randomFrom(ScriptType.values());
        String lang = random().nextBoolean() ? Script.DEFAULT_SCRIPT_LANG : randomSimpleString(random());
        String idOrCode = randomSimpleString(random());
        Map<String, Object> params = Collections.emptyMap();

        type = ScriptType.STORED;

        return new Script(type, type == ScriptType.STORED ? null : lang, idOrCode, params);
    }
}
