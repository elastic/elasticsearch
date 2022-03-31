/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.AbstractBulkIndexByScrollRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.elasticsearch.core.TimeValue.parseTimeValue;

/**
 * Round trip tests for all {@link Writeable} things declared in this plugin.
 */
public class RoundTripTests extends ESTestCase {
    public void testReindexRequest() throws IOException {
        ReindexRequest reindex = new ReindexRequest();
        randomRequest(reindex);
        reindex.getDestination().version(randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED, 12L, 1L, 123124L, 12L));
        reindex.getDestination().index("test");
        if (randomBoolean()) {
            int port = between(1, Integer.MAX_VALUE);
            BytesReference query = new BytesArray("{\"match_all\":{}}");
            String username = randomBoolean() ? randomAlphaOfLength(5) : null;
            SecureString password = username != null && randomBoolean() ? new SecureString(randomAlphaOfLength(5).toCharArray()) : null;
            int headersCount = randomBoolean() ? 0 : between(1, 10);
            Map<String, String> headers = Maps.newMapWithExpectedSize(headersCount);
            while (headers.size() < headersCount) {
                headers.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
            TimeValue socketTimeout = parseTimeValue(randomPositiveTimeValue(), "socketTimeout");
            TimeValue connectTimeout = parseTimeValue(randomPositiveTimeValue(), "connectTimeout");
            reindex.setRemoteInfo(
                new RemoteInfo(
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    port,
                    null,
                    query,
                    username,
                    password,
                    headers,
                    socketTimeout,
                    connectTimeout
                )
            );
        }
        ReindexRequest tripped = new ReindexRequest(toInputByteStream(reindex));
        assertRequestEquals(reindex, tripped);

        // Try regular slices with a version that doesn't support slices=auto, which should succeed
        reindex.setSlices(between(1, Integer.MAX_VALUE));
        tripped = new ReindexRequest(toInputByteStream(reindex));
        assertRequestEquals(reindex, tripped);
    }

    public void testUpdateByQueryRequest() throws IOException {
        UpdateByQueryRequest update = new UpdateByQueryRequest();
        randomRequest(update);
        if (randomBoolean()) {
            update.setPipeline(randomAlphaOfLength(5));
        }
        UpdateByQueryRequest tripped = new UpdateByQueryRequest(toInputByteStream(update));
        assertRequestEquals(update, tripped);
        assertEquals(update.getPipeline(), tripped.getPipeline());

        // Try regular slices with a version that doesn't support slices=auto, which should succeed
        update.setSlices(between(1, Integer.MAX_VALUE));
        tripped = new UpdateByQueryRequest(toInputByteStream(update));
        assertRequestEquals(update, tripped);
        assertEquals(update.getPipeline(), tripped.getPipeline());
    }

    public void testDeleteByQueryRequest() throws IOException {
        DeleteByQueryRequest delete = new DeleteByQueryRequest();
        randomRequest(delete);
        DeleteByQueryRequest tripped = new DeleteByQueryRequest(toInputByteStream(delete));
        assertRequestEquals(delete, tripped);

        // Try regular slices with a version that doesn't support slices=auto, which should succeed
        delete.setSlices(between(1, Integer.MAX_VALUE));
        tripped = new DeleteByQueryRequest(toInputByteStream(delete));
        assertRequestEquals(delete, tripped);
    }

    private void randomRequest(AbstractBulkByScrollRequest<?> request) {
        request.getSearchRequest().indices("test");
        request.getSearchRequest().source().size(between(1, 1000));
        if (randomBoolean()) {
            request.setMaxDocs(between(1, Integer.MAX_VALUE));
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

    private void assertRequestEquals(ReindexRequest request, ReindexRequest tripped) {
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
            assertEquals(request.getRemoteInfo().getSocketTimeout(), tripped.getRemoteInfo().getSocketTimeout());
            assertEquals(request.getRemoteInfo().getConnectTimeout(), tripped.getRemoteInfo().getConnectTimeout());
        }
    }

    private void assertRequestEquals(AbstractBulkIndexByScrollRequest<?> request, AbstractBulkIndexByScrollRequest<?> tripped) {
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
            request.setTargetTaskId(new TaskId(randomAlphaOfLength(5), randomLong()));
        }
        RethrottleRequest tripped = new RethrottleRequest(toInputByteStream(request));
        assertEquals(request.getRequestsPerSecond(), tripped.getRequestsPerSecond(), 0.00001);
        assertArrayEquals(request.getActions(), tripped.getActions());
        assertEquals(request.getTargetTaskId(), tripped.getTargetTaskId());
    }

    private StreamInput toInputByteStream(Writeable example) throws IOException {
        return toInputByteStream(Version.CURRENT, example);
    }

    private StreamInput toInputByteStream(Version version, Writeable example) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        example.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        return in;
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
