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

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class NodesStatsRequestTests extends ESTestCase {

    /**
     * Make sure that we can set, serialize, and deserialize arbitrary sets
     * of metrics.
     *
     * TODO: Use a looping construct rather than direct API calls
     */
    public void testMetricsSetters() throws Exception {
        NodesStatsRequest request = new NodesStatsRequest(randomAlphaOfLength(8));
        request.indices(randomFrom(CommonStatsFlags.ALL));
        request.os(randomBoolean());
        request.process(randomBoolean());
        request.jvm(randomBoolean());
        request.threadPool(randomBoolean());
        request.fs(randomBoolean());
        request.transport(randomBoolean());
        request.http(randomBoolean());
        request.breaker(randomBoolean());
        request.script(randomBoolean());
        request.discovery(randomBoolean());
        request.ingest(randomBoolean());
        request.adaptiveSelection(randomBoolean());
        NodesStatsRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    /**
     * Test that a newly constructed NodesStatsRequestObject requests all of the
     * possible metrics defined in {@link NodesStatsRequest}.
     */
    public void testNodesStatsRequestDefaults() {
        NodesStatsRequest defaultNodesStatsRequest = new NodesStatsRequest(randomAlphaOfLength(8));
        NodesStatsRequest constructedNodesStatsRequest = new NodesStatsRequest(randomAlphaOfLength(8));
        constructedNodesStatsRequest.clear();
        constructedNodesStatsRequest.indices(CommonStatsFlags.ALL);

        assertRequestsEqual(defaultNodesStatsRequest, constructedNodesStatsRequest);
    }

    /**
     * Test that the {@link NodesStatsRequest#all()} method sets all of the
     * metrics to {@code true}.
     *
     * TODO: Use a looping construct rather than direct API calls
     */
    public void testNodesInfoRequestAll() throws Exception {
        NodesStatsRequest request = new NodesStatsRequest("node");
        request.all();

        assertThat(request.indices().getFlags(), equalTo(CommonStatsFlags.ALL.getFlags()));
        assertTrue(request.os());
        assertTrue(request.process());
        assertTrue(request.jvm());
        assertTrue(request.threadPool());
        assertTrue(request.fs());
        assertTrue(request.transport());
        assertTrue(request.http());
        assertTrue(request.breaker());
        assertTrue(request.script());
        assertTrue(request.discovery());
        assertTrue(request.ingest());
        assertTrue(request.adaptiveSelection());
    }

    /**
     * Test that the {@link NodesStatsRequest#clear()} method sets all of the
     * metrics to {@code false}.
     *
     * TODO: Use a looping construct rather than direct API calls
     */
    public void testNodesInfoRequestClear() throws Exception {
        NodesStatsRequest request = new NodesStatsRequest("node");
        request.clear();

        assertThat(request.indices().getFlags(), equalTo(CommonStatsFlags.NONE.getFlags()));
        assertFalse(request.os());
        assertFalse(request.process());
        assertFalse(request.jvm());
        assertFalse(request.threadPool());
        assertFalse(request.fs());
        assertFalse(request.transport());
        assertFalse(request.http());
        assertFalse(request.breaker());
        assertFalse(request.script());
        assertFalse(request.discovery());
        assertFalse(request.ingest());
        assertFalse(request.adaptiveSelection());
    }

    /**
     * Serialize and deserialize a request.
     * @param request A request to serialize.
     * @return The deserialized, "round-tripped" request.
     */
    private static NodesStatsRequest roundTripRequest(NodesStatsRequest request) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new NodesStatsRequest(in);
            }
        }
    }

    private static void assertRequestsEqual(NodesStatsRequest request1, NodesStatsRequest request2) {
        // TODO: Use a looping construct rather than direct API calls
        assertThat(request1.indices().getFlags(), equalTo(request2.indices().getFlags()));
        assertThat(request1.os(), equalTo(request2.os()));
        assertThat(request1.process(), equalTo(request2.process()));
        assertThat(request1.jvm(), equalTo(request2.jvm()));
        assertThat(request1.threadPool(), equalTo(request2.threadPool()));
        assertThat(request1.fs(), equalTo(request2.fs()));
        assertThat(request1.transport(), equalTo(request2.transport()));
        assertThat(request1.http(), equalTo(request2.http()));
        assertThat(request1.breaker(), equalTo(request2.breaker()));
        assertThat(request1.script(), equalTo(request2.script()));
        assertThat(request1.discovery(), equalTo(request2.discovery()));
        assertThat(request1.ingest(), equalTo(request2.ingest()));
        assertThat(request1.adaptiveSelection(), equalTo(request2.adaptiveSelection()));
    }
}
