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
import org.elasticsearch.Version;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.index.reindex.remote.RemoteInfo;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.lang.Math.abs;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.hasSize;

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
            TimeValue socketTimeout = parseTimeValue(randomPositiveTimeValue(), "socketTimeout");
            TimeValue connectTimeout = parseTimeValue(randomPositiveTimeValue(), "connectTimeout");
            reindex.setRemoteInfo(new RemoteInfo(randomAsciiOfLength(5), randomAsciiOfLength(5), port, query, username, password, headers,
                    socketTimeout, connectTimeout));
        }
        ReindexRequest tripped = new ReindexRequest();
        roundTrip(reindex, tripped);
        assertRequestEquals(reindex, tripped);

        // Try slices with a version that doesn't support slices. That should fail.
        reindex.setSlices(between(2, 1000));
        Exception e = expectThrows(IllegalArgumentException.class, () -> roundTrip(Version.V_5_0_0_rc1, reindex, null));
        assertEquals("Attempting to send sliced reindex-style request to a node that doesn't support it. "
                + "Version is [5.0.0-rc1] but must be [5.1.1]", e.getMessage());

        // Try without slices with a version that doesn't support slices. That should work.
        tripped = new ReindexRequest();
        reindex.setSlices(1);
        roundTrip(Version.V_5_0_0_rc1, reindex, tripped);
        assertRequestEquals(Version.V_5_0_0_rc1, reindex, tripped);
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

        // Try slices with a version that doesn't support slices. That should fail.
        update.setSlices(between(2, 1000));
        Exception e = expectThrows(IllegalArgumentException.class, () -> roundTrip(Version.V_5_0_0_rc1, update, null));
        assertEquals("Attempting to send sliced reindex-style request to a node that doesn't support it. "
                + "Version is [5.0.0-rc1] but must be [5.1.1]", e.getMessage());

        // Try without slices with a version that doesn't support slices. That should work.
        tripped = new UpdateByQueryRequest();
        update.setSlices(1);
        roundTrip(Version.V_5_0_0_rc1, update, tripped);
        assertRequestEquals(update, tripped);
        assertEquals(update.getPipeline(), tripped.getPipeline());
    }

    public void testDeleteByQueryRequest() throws IOException {
        DeleteByQueryRequest delete = new DeleteByQueryRequest(new SearchRequest());
        randomRequest(delete);
        DeleteByQueryRequest tripped = new DeleteByQueryRequest();
        roundTrip(delete, tripped);
        assertRequestEquals(delete, tripped);

        // Try slices with a version that doesn't support slices. That should fail.
        delete.setSlices(between(2, 1000));
        Exception e = expectThrows(IllegalArgumentException.class, () -> roundTrip(Version.V_5_0_0_rc1, delete, null));
        assertEquals("Attempting to send sliced reindex-style request to a node that doesn't support it. "
                + "Version is [5.0.0-rc1] but must be [5.1.1]", e.getMessage());

        // Try without slices with a version that doesn't support slices. That should work.
        tripped = new DeleteByQueryRequest();
        delete.setSlices(1);
        roundTrip(Version.V_5_0_0_rc1, delete, tripped);
        assertRequestEquals(delete, tripped);
    }

    private void randomRequest(AbstractBulkByScrollRequest<?> request) {
        request.getSearchRequest().indices("test");
        request.getSearchRequest().source().size(between(1, 1000));
        request.setSize(random().nextBoolean() ? between(1, Integer.MAX_VALUE) : -1);
        request.setAbortOnVersionConflict(random().nextBoolean());
        request.setRefresh(rarely());
        request.setTimeout(TimeValue.parseTimeValue(randomTimeValue(), null, "test"));
        request.setWaitForActiveShards(randomIntBetween(0, 10));
        request.setRequestsPerSecond(between(0, Integer.MAX_VALUE));
        request.setSlices(between(1, Integer.MAX_VALUE));
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
            if (version.onOrAfter(Version.V_5_2_0_UNRELEASED)) {
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

    public void testBulkByTaskStatus() throws IOException {
        BulkByScrollTask.Status status = randomStatus();
        BytesStreamOutput out = new BytesStreamOutput();
        status.writeTo(out);
        BulkByScrollTask.Status tripped = new BulkByScrollTask.Status(out.bytes().streamInput());
        assertTaskStatusEquals(out.getVersion(), status, tripped);

        // Also check round tripping pre-5.1 which is the first version to support parallelized scroll
        out = new BytesStreamOutput();
        out.setVersion(Version.V_5_0_0_rc1); // This can be V_5_0_0
        status.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_5_0_0_rc1);
        tripped = new BulkByScrollTask.Status(in);
        assertTaskStatusEquals(Version.V_5_0_0_rc1, status, tripped);
    }

    public void testReindexResponse() throws IOException {
        BulkIndexByScrollResponse response = new BulkIndexByScrollResponse(timeValueMillis(randomNonNegativeLong()), randomStatus(),
                randomIndexingFailures(), randomSearchFailures(), randomBoolean());
        BulkIndexByScrollResponse tripped = new BulkIndexByScrollResponse();
        roundTrip(response, tripped);
        assertResponseEquals(response, tripped);
    }

    public void testBulkIndexByScrollResponse() throws IOException {
        BulkIndexByScrollResponse response = new BulkIndexByScrollResponse(timeValueMillis(randomNonNegativeLong()), randomStatus(),
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
        if (randomBoolean()) {
            return randomWorkingStatus(null);
        }
        boolean canHaveNullStatues = randomBoolean();
        List<BulkByScrollTask.StatusOrException> statuses = IntStream.range(0, between(0, 10))
                .mapToObj(i -> {
                    if (canHaveNullStatues && rarely()) {
                        return null;
                    }
                    if (randomBoolean()) {
                        return new BulkByScrollTask.StatusOrException(new ElasticsearchException(randomAsciiOfLength(5)));
                    }
                    return new BulkByScrollTask.StatusOrException(randomWorkingStatus(i));
                })
                .collect(toList());
        return new BulkByScrollTask.Status(statuses, randomBoolean() ? "test" : null);
    }

    private BulkByScrollTask.Status randomWorkingStatus(Integer sliceId) {
        // These all should be believably small because we sum them if we have multiple workers
        int total = between(0, 10000000);
        int updated = between(0, total);
        int created = between(0, total - updated);
        int deleted = between(0, total - updated - created);
        int noops = total - updated - created - deleted;
        int batches = between(0, 10000);
        long versionConflicts = between(0, total);
        long bulkRetries = between(0, 10000000);
        long searchRetries = between(0, 100000);
        return new BulkByScrollTask.Status(sliceId, total, updated, created, deleted, batches, versionConflicts, noops, bulkRetries,
                searchRetries, parseTimeValue(randomPositiveTimeValue(), "test"), abs(random().nextFloat()),
                randomBoolean() ? null : randomSimpleString(random()), parseTimeValue(randomPositiveTimeValue(), "test"));
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

        return new Script(type, lang, idOrCode, params);
    }

    private void assertResponseEquals(BulkIndexByScrollResponse expected, BulkIndexByScrollResponse actual) {
        assertEquals(expected.getTook(), actual.getTook());
        assertTaskStatusEquals(Version.CURRENT, expected.getStatus(), actual.getStatus());
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

    private void assertTaskStatusEquals(Version version, BulkByScrollTask.Status expected, BulkByScrollTask.Status actual) {
        assertEquals(expected.getTotal(), actual.getTotal());
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
        if (version.onOrAfter(Version.V_5_1_1_UNRELEASED)) {
            assertThat(actual.getSliceStatuses(), hasSize(expected.getSliceStatuses().size()));
            for (int i = 0; i < expected.getSliceStatuses().size(); i++) {
                BulkByScrollTask.StatusOrException sliceStatus = expected.getSliceStatuses().get(i);
                if (sliceStatus == null) {
                    assertNull(actual.getSliceStatuses().get(i));
                } else if (sliceStatus.getException() == null) {
                    assertNull(actual.getSliceStatuses().get(i).getException());
                    assertTaskStatusEquals(version, sliceStatus.getStatus(), actual.getSliceStatuses().get(i).getStatus());
                } else {
                    assertNull(actual.getSliceStatuses().get(i).getStatus());
                    // Just check the message because we're not testing exception serialization in general here.
                    assertEquals(sliceStatus.getException().getMessage(), actual.getSliceStatuses().get(i).getException().getMessage());
                }
            }
        } else {
            assertEquals(emptyList(), actual.getSliceStatuses());
        }
    }
}
