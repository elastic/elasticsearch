/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * The fetch contexts that a remote cluster keeps open for the querying cluster, as its response lists them.
 */
public class ComputeResponseFetchContextsTests extends ESTestCase {
    private static final TransportVersion BEFORE_FETCH_CONTEXTS = TransportVersionUtils.getPreviousVersion(
        DataNodeRequest.ESQL_FETCH_CONTEXTS
    );
    private static final Map<String, List<ShardSearchContextId>> CONTEXTS = Map.of(
        "r1",
        List.of(new ShardSearchContextId("session", 1), new ShardSearchContextId("session", 2)),
        "r2",
        List.of(new ShardSearchContextId("session", 3))
    );
    private static final TimeValue KEEP_ALIVE = TimeValue.timeValueMinutes(4);

    public void testTheContextsAndTheirKeepAliveRoundTrip() throws IOException {
        ComputeResponse response = response().withFetchContexts(CONTEXTS, KEEP_ALIVE);

        ComputeResponse read = roundTrip(response, TransportVersion.current());

        assertThat(read.fetchContexts(), equalTo(CONTEXTS));
        assertThat(read.fetchContextsKeepAlive(), equalTo(KEEP_ALIVE));
        assertThat(read.getTotalShards(), equalTo(5));
    }

    public void testAResponseWithoutContextsHasNoKeepAlive() throws IOException {
        ComputeResponse response = response().withFetchContexts(Map.of(), KEEP_ALIVE);

        ComputeResponse read = roundTrip(response, TransportVersion.current());

        assertThat(read.fetchContexts(), anEmptyMap());
        assertThat(read.fetchContextsKeepAlive(), nullValue());
    }

    /**
     * Only a querying cluster that asked for fetch contexts gets them, and it can read them. Leaving them out would leave
     * the contexts open until their keep-alive passed.
     */
    public void testAQueryingClusterWithoutTheFetchPhaseIsNotSentContexts() {
        ComputeResponse response = response().withFetchContexts(CONTEXTS, KEEP_ALIVE);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> roundTrip(response, BEFORE_FETCH_CONTEXTS));

        assertThat(e.getMessage(), containsString("can't send fetch contexts"));
    }

    public void testAResponseWithoutContextsReachesAnOlderQueryingCluster() throws IOException {
        ComputeResponse read = roundTrip(response(), BEFORE_FETCH_CONTEXTS);

        assertThat(read.fetchContexts(), anEmptyMap());
        assertThat(read.getTotalShards(), equalTo(5));
    }

    private static ComputeResponse response() {
        DriverCompletionInfo completionInfo = new DriverCompletionInfo(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            List.of(),
            List.of(),
            Map.of(),
            false,
            false,
            Set.of()
        );
        return new ComputeResponse(completionInfo, TimeValue.timeValueMillis(7), 5, 5, 0, 0, List.of());
    }

    private static ComputeResponse roundTrip(ComputeResponse response, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            response.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setTransportVersion(version);
            return new ComputeResponse(in, new ThreadContext(Settings.EMPTY));
        }
    }
}
