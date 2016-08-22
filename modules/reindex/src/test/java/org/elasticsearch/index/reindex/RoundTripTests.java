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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.index.reindex.remote.RemoteInfo;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.abs;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

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
            BytesReference query = new BytesArray(randomAsciiOfLength(5));
            String username = randomBoolean() ? randomAsciiOfLength(5) : null;
            String password = username != null && randomBoolean() ? randomAsciiOfLength(5) : null;
            int headersCount = randomBoolean() ? 0 : between(1, 10);
            Map<String, String> headers = new HashMap<>(headersCount);
            while (headers.size() < headersCount) {
                headers.put(randomAsciiOfLength(5), randomAsciiOfLength(5));
            }
            reindex.setRemoteInfo(new RemoteInfo(randomAsciiOfLength(5), randomAsciiOfLength(5), port, query, username, password, headers));
        }
        ReindexRequest tripped = new ReindexRequest();
        roundTrip(reindex, tripped);
        assertRequestEquals(reindex, tripped);
        assertEquals(reindex.getDestination().version(), tripped.getDestination().version());
        assertEquals(reindex.getDestination().index(), tripped.getDestination().index());
        if (reindex.getRemoteInfo() == null) {
            assertNull(tripped.getRemoteInfo());
        } else {
            assertNotNull(tripped.getRemoteInfo());
            assertEquals(reindex.getRemoteInfo().getScheme(), tripped.getRemoteInfo().getScheme());
            assertEquals(reindex.getRemoteInfo().getHost(), tripped.getRemoteInfo().getHost());
            assertEquals(reindex.getRemoteInfo().getQuery(), tripped.getRemoteInfo().getQuery());
            assertEquals(reindex.getRemoteInfo().getUsername(), tripped.getRemoteInfo().getUsername());
            assertEquals(reindex.getRemoteInfo().getPassword(), tripped.getRemoteInfo().getPassword());
            assertEquals(reindex.getRemoteInfo().getHeaders(), tripped.getRemoteInfo().getHeaders());
        }
    }

    public void testUpdateByQueryRequest() throws IOException {
        UpdateByQueryRequest update = new UpdateByQueryRequest(new SearchRequest());
        randomRequest(update);
        if (randomBoolean()) {
            update.setPipeline(randomAsciiOfLength(5));
        }
        UpdateByQueryRequest tripped = new UpdateByQueryRequest();
        roundTrip(update, tripped);
        assertRequestEquals(update, tripped);
        assertEquals(update.getPipeline(), tripped.getPipeline());
    }

    private void randomRequest(AbstractBulkIndexByScrollRequest<?> request) {
        request.getSearchRequest().indices("test");
        request.getSearchRequest().source().size(between(1, 1000));
        request.setSize(random().nextBoolean() ? between(1, Integer.MAX_VALUE) : -1);
        request.setAbortOnVersionConflict(random().nextBoolean());
        request.setRefresh(rarely());
        request.setTimeout(TimeValue.parseTimeValue(randomTimeValue(), null, "test"));
        request.setWaitForActiveShards(randomIntBetween(0, 10));
        request.setScript(random().nextBoolean() ? null : randomScript());
        request.setRequestsPerSecond(between(0, Integer.MAX_VALUE));
    }

    private void assertRequestEquals(AbstractBulkIndexByScrollRequest<?> request,
            AbstractBulkIndexByScrollRequest<?> tripped) {
        assertArrayEquals(request.getSearchRequest().indices(), tripped.getSearchRequest().indices());
        assertEquals(request.getSearchRequest().source().size(), tripped.getSearchRequest().source().size());
        assertEquals(request.isAbortOnVersionConflict(), tripped.isAbortOnVersionConflict());
        assertEquals(request.isRefresh(), tripped.isRefresh());
        assertEquals(request.getTimeout(), tripped.getTimeout());
        assertEquals(request.getWaitForActiveShards(), tripped.getWaitForActiveShards());
        assertEquals(request.getScript(), tripped.getScript());
        assertEquals(request.getRetryBackoffInitialTime(), tripped.getRetryBackoffInitialTime());
        assertEquals(request.getMaxRetries(), tripped.getMaxRetries());
        assertEquals(request.getRequestsPerSecond(), tripped.getRequestsPerSecond(), 0d);
    }

    public void testBulkByTaskStatus() throws IOException {
        BulkByScrollTask.Status status = randomStatus();
        BytesStreamOutput out = new BytesStreamOutput();
        status.writeTo(out);
        BulkByScrollTask.Status tripped = new BulkByScrollTask.Status(out.bytes().streamInput());
        assertTaskStatusEquals(status, tripped);
    }

    public void testReindexResponse() throws IOException {
        BulkIndexByScrollResponse response = new BulkIndexByScrollResponse(timeValueMillis(randomPositiveLong()), randomStatus(),
                randomIndexingFailures(), randomSearchFailures(), randomBoolean());
        BulkIndexByScrollResponse tripped = new BulkIndexByScrollResponse();
        roundTrip(response, tripped);
        assertResponseEquals(response, tripped);
    }

    public void testBulkIndexByScrollResponse() throws IOException {
        BulkIndexByScrollResponse response = new BulkIndexByScrollResponse(timeValueMillis(randomPositiveLong()), randomStatus(),
                randomIndexingFailures(), randomSearchFailures(), randomBoolean());
        BulkIndexByScrollResponse tripped = new BulkIndexByScrollResponse();
        roundTrip(response, tripped);
        assertResponseEquals(response, tripped);
    }

    public void testRethrottleRequest() throws IOException {
        RethrottleRequest request = new RethrottleRequest();
        request.setRequestsPerSecond((float) randomDoubleBetween(0, Float.POSITIVE_INFINITY, false));
        if (randomBoolean()) {
            request.setActions(randomFrom(UpdateByQueryAction.NAME, ReindexAction.NAME));
        } else {
            request.setTaskId(new TaskId(randomAsciiOfLength(5), randomLong()));
        }
        RethrottleRequest tripped = new RethrottleRequest();
        roundTrip(request, tripped);
        assertEquals(request.getRequestsPerSecond(), tripped.getRequestsPerSecond(), 0.00001);
        assertArrayEquals(request.getActions(), tripped.getActions());
        assertEquals(request.getTaskId(), tripped.getTaskId());
    }

    private BulkByScrollTask.Status randomStatus() {
        return new BulkByScrollTask.Status(randomPositiveLong(), randomPositiveLong(), randomPositiveLong(), randomPositiveLong(),
                randomInt(Integer.MAX_VALUE), randomPositiveLong(), randomPositiveLong(), randomPositiveLong(), randomPositiveLong(),
                parseTimeValue(randomPositiveTimeValue(), "test"), abs(random().nextFloat()),
                random().nextBoolean() ? null : randomSimpleString(random()), parseTimeValue(randomPositiveTimeValue(), "test"));
    }

    private List<Failure> randomIndexingFailures() {
        return usually() ? emptyList()
                : singletonList(new Failure(randomSimpleString(random()), randomSimpleString(random()),
                        randomSimpleString(random()), new IllegalArgumentException("test")));
    }

    private List<SearchFailure> randomSearchFailures() {
        if (randomBoolean()) {
            return emptyList();
        }
        String index = null;
        Integer shardId = null;
        String nodeId = null;
        if (randomBoolean()) {
            index = randomAsciiOfLength(5);
            shardId = randomInt();
            nodeId = usually() ? randomAsciiOfLength(5) : null;
        }
        return singletonList(new SearchFailure(new ElasticsearchException("foo"), index, shardId, nodeId));
    }

    private void roundTrip(Streamable example, Streamable empty) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        example.writeTo(out);
        empty.readFrom(out.bytes().streamInput());
    }

    private Script randomScript() {
        return new Script(randomSimpleString(random()), // Name
                randomFrom(ScriptType.values()), // Type
                random().nextBoolean() ? null : randomSimpleString(random()), // Language
                emptyMap()); // Params
    }

    private long randomPositiveLong() {
        long l;
        do {
            l = randomLong();
        } while (l < 0);
        return l;
    }

    private void assertResponseEquals(BulkIndexByScrollResponse expected, BulkIndexByScrollResponse actual) {
        assertEquals(expected.getTook(), actual.getTook());
        assertTaskStatusEquals(expected.getStatus(), actual.getStatus());
        assertEquals(expected.getBulkFailures().size(), actual.getBulkFailures().size());
        for (int i = 0; i < expected.getBulkFailures().size(); i++) {
            Failure expectedFailure = expected.getBulkFailures().get(i);
            Failure actualFailure = actual.getBulkFailures().get(i);
            assertEquals(expectedFailure.getIndex(), actualFailure.getIndex());
            assertEquals(expectedFailure.getType(), actualFailure.getType());
            assertEquals(expectedFailure.getId(), actualFailure.getId());
            assertEquals(expectedFailure.getMessage(), actualFailure.getMessage());
            assertEquals(expectedFailure.getStatus(), actualFailure.getStatus());
        }
        assertEquals(expected.getSearchFailures().size(), actual.getSearchFailures().size());
        for (int i = 0; i < expected.getSearchFailures().size(); i++) {
            SearchFailure expectedFailure = expected.getSearchFailures().get(i);
            SearchFailure actualFailure = actual.getSearchFailures().get(i);
            assertEquals(expectedFailure.getIndex(), actualFailure.getIndex());
            assertEquals(expectedFailure.getShardId(), actualFailure.getShardId());
            assertEquals(expectedFailure.getNodeId(), actualFailure.getNodeId());
            assertEquals(expectedFailure.getReason().getClass(), actualFailure.getReason().getClass());
            assertEquals(expectedFailure.getReason().getMessage(), actualFailure.getReason().getMessage());
        }

    }

    private void assertTaskStatusEquals(BulkByScrollTask.Status expected, BulkByScrollTask.Status actual) {
        assertEquals(expected.getUpdated(), actual.getUpdated());
        assertEquals(expected.getCreated(), actual.getCreated());
        assertEquals(expected.getDeleted(), actual.getDeleted());
        assertEquals(expected.getBatches(), actual.getBatches());
        assertEquals(expected.getVersionConflicts(), actual.getVersionConflicts());
        assertEquals(expected.getNoops(), actual.getNoops());
        assertEquals(expected.getBulkRetries(), actual.getBulkRetries());
        assertEquals(expected.getSearchRetries(), actual.getSearchRetries());
        assertEquals(expected.getThrottled(), actual.getThrottled());
        assertEquals(expected.getRequestsPerSecond(), actual.getRequestsPerSecond(), 0f);
        assertEquals(expected.getReasonCancelled(), actual.getReasonCancelled());
        assertEquals(expected.getThrottledUntil(), actual.getThrottledUntil());
    }
}
