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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.fetch.lifetime.OpenContextInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class DataNodeComputeResponseTests extends ESTestCase {
    public void testOpenContextsRoundTrip() throws IOException {
        List<OpenContextInfo> open = randomList(0, 5, DataNodeComputeResponseTests::randomOpenContext);
        DataNodeComputeResponse response = new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of()).withOpenContexts(open);

        DataNodeComputeResponse copy = roundTrip(response, TransportVersion.current());

        assertThat(copy.openContexts(), equalTo(open));
    }

    /**
     * Only a coordinator that asked for fetch contexts gets them back, and asking needs the same version as reading them.
     */
    public void testOpenContextsNeverReachAnOlderNode() throws IOException {
        TransportVersion older = TransportVersionUtils.getPreviousVersion(DataNodeRequest.ESQL_FETCH_CONTEXTS);
        DataNodeComputeResponse without = new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of());
        assertThat(roundTrip(without, older).openContexts(), empty());

        DataNodeComputeResponse with = without.withOpenContexts(List.of(randomOpenContext()));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> roundTrip(with, older));
        assertThat(e.getMessage(), containsString("can't send fetch contexts to a node on"));
    }

    private static OpenContextInfo randomOpenContext() {
        return new OpenContextInfo(
            new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), between(0, 10)),
            new ShardSearchContextId(randomAlphaOfLength(10), randomNonNegativeLong(), randomBoolean() ? null : randomAlphaOfLength(8))
        );
    }

    private static DataNodeComputeResponse roundTrip(DataNodeComputeResponse response, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                return new DataNodeComputeResponse(in, new ThreadContext(Settings.EMPTY));
            }
        }
    }
}
